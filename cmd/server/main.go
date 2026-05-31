// Command bucketd is the rate-limiter daemon. It serves the
// bucketd.v1.RateLimiter gRPC service plus the standard gRPC Health service.
//
// Configuration is via environment variables (see internal/server/run.go
// for the full list). Shutdown is graceful: SIGTERM/SIGINT triggers a
// drain bounded by SHUTDOWN_TIMEOUT (default 10s), then a forceful stop.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/kevinreber/bucketd/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := server.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if err := server.Run(ctx, cfg); err != nil {
		log.Fatalf("run: %v", err)
	}
}
