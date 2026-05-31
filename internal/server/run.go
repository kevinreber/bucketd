package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/kevinreber/bucketd/internal/backend"
	ratelimitpb "github.com/kevinreber/bucketd/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Config is the runtime configuration for the bucketd server.
//
// Fields are populated from environment variables in LoadConfigFromEnv; tests
// construct a Config struct directly and skip the env layer.
type Config struct {
	// Addr is the gRPC listen address. Defaults to ":50051".
	Addr string

	// RedisURL, if set, switches the backend to Redis. Empty means in-process
	// Memory backend (Phase 1 mode — fine for single-node dev and tests).
	RedisURL string

	// ShutdownTimeout is how long GracefulStop has to drain in-flight RPCs
	// before we fall back to forceful Stop. Defaults to 10s.
	ShutdownTimeout time.Duration

	// Logger receives structured operational events. Defaults to a slog
	// JSON handler writing to stderr.
	Logger *slog.Logger
}

// LoadConfigFromEnv reads runtime config from environment variables. Missing
// values fall back to sane defaults.
//
//   - ADDR             — listen address (default ":50051")
//   - REDIS_URL        — if set, use Redis backend
//   - SHUTDOWN_TIMEOUT — graceful shutdown budget (default "10s", Go duration syntax)
func LoadConfigFromEnv() (Config, error) {
	c := Config{
		Addr:            envOr("ADDR", ":50051"),
		RedisURL:        os.Getenv("REDIS_URL"),
		ShutdownTimeout: 10 * time.Second,
		Logger:          slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	}
	if raw := os.Getenv("SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse SHUTDOWN_TIMEOUT %q: %w", raw, err)
		}
		c.ShutdownTimeout = d
	}
	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Run starts the bucketd gRPC server and blocks until ctx is cancelled or
// the listener stops accepting. On ctx cancel, it triggers a graceful
// shutdown bounded by Config.ShutdownTimeout, then falls back to Stop().
//
// Returns nil on a clean shutdown, error otherwise.
func Run(ctx context.Context, cfg Config) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	be, err := chooseBackend(ctx, cfg, logger)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}
	logger.Info("listening", "addr", lis.Addr().String())

	grpcServer := grpc.NewServer()
	ratelimitpb.RegisterRateLimiterServer(grpcServer, NewServer(be))

	// Register the standard gRPC health service so Fly.io / load balancers can
	// distinguish a cold-starting instance from a dead one.
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("bucketd.v1.RateLimiter", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)

	// Serve in its own goroutine so we can react to ctx cancellation here.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining", "timeout", cfg.ShutdownTimeout)
		// Flip the health service to NOT_SERVING immediately so load balancers
		// stop sending new traffic while we drain in-flight RPCs.
		hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		hs.SetServingStatus("bucketd.v1.RateLimiter", healthpb.HealthCheckResponse_NOT_SERVING)

		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			logger.Info("graceful shutdown complete")
			return nil
		case <-time.After(cfg.ShutdownTimeout):
			logger.Warn("graceful shutdown timed out, forcing", "timeout", cfg.ShutdownTimeout)
			grpcServer.Stop()
			return nil
		}

	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}

// chooseBackend wires the Redis backend if REDIS_URL is set, else falls back
// to the in-process Memory backend. The Redis backend uses NewRedisWithPreload
// to fail fast if Redis is unreachable or rejects the Lua scripts.
func chooseBackend(ctx context.Context, cfg Config, logger *slog.Logger) (Backend, error) {
	if cfg.RedisURL == "" {
		logger.Info("backend: memory (REDIS_URL unset)")
		return backend.NewMemory(nil), nil
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", opts.Addr, err)
	}

	be, err := backend.NewRedisWithPreload(ctx, client)
	if err != nil {
		return nil, err
	}
	logger.Info("backend: redis", "addr", opts.Addr)
	return be, nil
}
