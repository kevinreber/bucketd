package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kevinreber/bucketd/internal/algorithms"
	"github.com/kevinreber/bucketd/internal/backend"
	"github.com/kevinreber/bucketd/internal/server"
	ratelimitpb "github.com/kevinreber/bucketd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// newTestServer wires a gRPC server to an in-memory backend, exposed over a
// bufconn (no real network). Returns a connected client and a cleanup func.
func newTestServer(t *testing.T, clock algorithms.Clock) (ratelimitpb.RateLimiterClient, func()) {
	t.Helper()

	lis := bufconn.Listen(1 << 16) // 64 KiB buffer
	be := backend.NewMemory(clock)
	srv := grpc.NewServer()
	ratelimitpb.RegisterRateLimiterServer(srv, server.NewServer(be))

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	client := ratelimitpb.NewRateLimiterClient(conn)
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		// drain server goroutine so the test doesn't leak it
		<-serverErr
	}
	return client, cleanup
}

func TestAllow_FullBucketGrants(t *testing.T) {
	client, cleanup := newTestServer(t, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Allow(ctx, &ratelimitpb.AllowRequest{
		Key:        "user-1",
		Tokens:     1,
		Capacity:   5,
		RefillRate: 1.0,
	})
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !resp.Allowed {
		t.Errorf("expected allowed=true on first call, got false")
	}
	if resp.Remaining != 4 {
		t.Errorf("expected remaining=4, got %d", resp.Remaining)
	}
	if resp.RetryAfterMs != 0 {
		t.Errorf("expected retry_after_ms=0 on allow, got %d", resp.RetryAfterMs)
	}
}

func TestAllow_DepletesAndDenies(t *testing.T) {
	// Frozen clock so refill can't sneak in between drains.
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }

	client, cleanup := newTestServer(t, clock)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &ratelimitpb.AllowRequest{
		Key: "user-2", Tokens: 1, Capacity: 3, RefillRate: 10.0,
	}

	// Drain the bucket: 3 successes.
	for i := 0; i < 3; i++ {
		resp, err := client.Allow(ctx, req)
		if err != nil {
			t.Fatalf("drain[%d]: %v", i, err)
		}
		if !resp.Allowed {
			t.Fatalf("drain[%d]: expected allowed=true, got false", i)
		}
	}

	// 4th call denied; retry hint should be ~100ms (1 token / 10 per sec).
	resp, err := client.Allow(ctx, req)
	if err != nil {
		t.Fatalf("4th call: %v", err)
	}
	if resp.Allowed {
		t.Errorf("expected denied after drain, got allowed=true")
	}
	if resp.RetryAfterMs < 90 || resp.RetryAfterMs > 110 {
		t.Errorf("expected retry_after_ms ~100, got %d", resp.RetryAfterMs)
	}
}

func TestAllow_RefillRestoresGrants(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }

	client, cleanup := newTestServer(t, clock)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &ratelimitpb.AllowRequest{
		Key: "user-3", Tokens: 1, Capacity: 2, RefillRate: 10.0,
	}

	// Drain.
	for i := 0; i < 2; i++ {
		if _, err := client.Allow(ctx, req); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}

	// Advance clock by 200ms — should accrue exactly 2 tokens.
	now = now.Add(200 * time.Millisecond)

	for i := 0; i < 2; i++ {
		resp, err := client.Allow(ctx, req)
		if err != nil {
			t.Fatalf("post-refill[%d]: %v", i, err)
		}
		if !resp.Allowed {
			t.Errorf("post-refill[%d]: expected allowed=true, got false", i)
		}
	}
}

func TestAllow_RejectsEmptyKey(t *testing.T) {
	client, cleanup := newTestServer(t, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.Allow(ctx, &ratelimitpb.AllowRequest{
		Tokens: 1, Capacity: 1, RefillRate: 1.0,
	})
	if err == nil {
		t.Fatalf("expected error for empty key, got nil")
	}
}
