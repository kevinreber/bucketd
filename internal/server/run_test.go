package server_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kevinreber/bucketd/client"
	"github.com/kevinreber/bucketd/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"log/slog"
	"io"
)

// pickFreePort asks the OS for an available TCP port. There's a small race
// window between releasing and rebinding but it's fine for tests.
func pickFreePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// quietLogger discards all log output so test runs are tidy.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitForServing polls the gRPC Health endpoint until it reports SERVING
// or the deadline expires. Used to gate test bodies on a fully-started server.
func waitForServing(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			cancel()
			_ = conn.Close()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server at %s did not reach SERVING within %s", addr, timeout)
}

func TestRun_ServesAllowOverRealNetwork(t *testing.T) {
	addr := pickFreePort(t)
	cfg := server.Config{
		Addr:            addr,
		ShutdownTimeout: 2 * time.Second,
		Logger:          quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Run(ctx, cfg); err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitForServing(t, addr, 3*time.Second)

	cli, err := client.New([]string{addr})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	v, err := cli.Allow(context.Background(), "user-1", 1, client.Limit{
		Capacity:   5,
		RefillRate: 1.0,
	})
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !v.Allowed {
		t.Errorf("expected allowed=true on first call, got false")
	}
	if v.Remaining != 4 {
		t.Errorf("expected remaining=4, got %d", v.Remaining)
	}
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	addr := pickFreePort(t)
	cfg := server.Config{
		Addr:            addr,
		ShutdownTimeout: 2 * time.Second,
		Logger:          quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Run(ctx, cfg)
	}()

	waitForServing(t, addr, 3*time.Second)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not exit within graceful-shutdown deadline")
	}
}

func TestRun_HealthFlipsToNotServingDuringShutdown(t *testing.T) {
	addr := pickFreePort(t)
	cfg := server.Config{
		Addr: addr,
		// Generous shutdown so we have time to observe the NOT_SERVING flip
		// before the server actually exits.
		ShutdownTimeout: 5 * time.Second,
		Logger:          quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = server.Run(ctx, cfg)
		close(done)
	}()

	waitForServing(t, addr, 3*time.Second)

	// Cancel and check health within a few hundred ms.
	cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	hc := healthpb.NewHealthClient(conn)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx2, c2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		resp, herr := hc.Check(ctx2, &healthpb.HealthCheckRequest{})
		c2()
		if herr == nil && resp.GetStatus() == healthpb.HealthCheckResponse_NOT_SERVING {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	<-done
}

func TestLoadConfigFromEnv_ParsesDuration(t *testing.T) {
	t.Setenv("ADDR", ":1234")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("REDIS_URL", "")

	cfg, err := server.LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	if cfg.Addr != ":1234" {
		t.Errorf("Addr = %q, want :1234", cfg.Addr)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 5s", cfg.ShutdownTimeout)
	}
}

func TestLoadConfigFromEnv_RejectsBadDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "not-a-duration")
	if _, err := server.LoadConfigFromEnv(); err == nil {
		t.Errorf("expected error for malformed SHUTDOWN_TIMEOUT")
	}
}

func TestRun_RejectsUnreachableRedis(t *testing.T) {
	cfg := server.Config{
		Addr:            pickFreePort(t),
		RedisURL:        "redis://127.0.0.1:1", // port 1 is unbindable; ping fails fast
		ShutdownTimeout: 1 * time.Second,
		Logger:          quietLogger(),
	}
	err := server.Run(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error from unreachable Redis, got nil")
	}
}
