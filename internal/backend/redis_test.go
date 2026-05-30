package backend_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kevinreber/bucketd/internal/backend"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newRedisBackend returns a connected Redis-backed bucketd backend.
//
// Strategy:
//   - If REDIS_URL is set (CI service container, local dev override),
//     connect to it directly. Fast, no Docker pull overhead.
//   - Otherwise fall back to spinning up a testcontainers Redis instance.
//     Tests skip automatically when Docker isn't available.
//
// The backend's keys are flushed at setup so test order doesn't matter.
func newRedisBackend(t *testing.T) *backend.Redis {
	t.Helper()
	ctx := context.Background()

	var addr string
	if url := os.Getenv("REDIS_URL"); url != "" {
		opts, err := redis.ParseURL(url)
		if err != nil {
			t.Fatalf("parse REDIS_URL: %v", err)
		}
		addr = opts.Addr
	} else {
		req := testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		}
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Skipf("redis container unavailable (likely no Docker): %v", err)
		}
		t.Cleanup(func() { _ = container.Terminate(ctx) })

		host, err := container.Host(ctx)
		if err != nil {
			t.Fatalf("container host: %v", err)
		}
		port, err := container.MappedPort(ctx, "6379")
		if err != nil {
			t.Fatalf("container port: %v", err)
		}
		addr = fmt.Sprintf("%s:%s", host, port.Port())
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	// Best-effort flush so tests don't see leftover state from prior runs.
	_ = client.FlushAll(ctx).Err()

	return backend.NewRedis(client)
}

// ============================================================
// Token bucket against real Redis
// ============================================================

func TestRedis_TokenBucket_FullBucketGrants(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	v, err := be.Allow(ctx, "user-1", 1, 5, 1.0)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !v.Allowed {
		t.Errorf("expected allowed on first call, got denied")
	}
	if v.Remaining != 4 {
		t.Errorf("expected remaining=4, got %d", v.Remaining)
	}
}

func TestRedis_TokenBucket_DrainAndDeny(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		v, err := be.Allow(ctx, "user-2", 1, 3, 10.0)
		if err != nil {
			t.Fatalf("drain[%d]: %v", i, err)
		}
		if !v.Allowed {
			t.Fatalf("drain[%d]: expected allowed", i)
		}
	}

	v, err := be.Allow(ctx, "user-2", 1, 3, 10.0)
	if err != nil {
		t.Fatalf("post-drain: %v", err)
	}
	if v.Allowed {
		t.Errorf("expected denied after drain")
	}
	if v.RetryAfterMs <= 0 || v.RetryAfterMs > 200 {
		t.Errorf("retry_after_ms should be ~100ms-ish, got %d", v.RetryAfterMs)
	}
}

func TestRedis_TokenBucket_RefillsOverTime(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, _ = be.Allow(ctx, "user-3", 1, 5, 100.0) // 100 tokens/sec
	}
	// 5 tokens drained. At 100/sec, we get 1 token every 10ms.
	// Wait 60ms => at least 5 tokens should be back.
	time.Sleep(60 * time.Millisecond)

	v, err := be.Allow(ctx, "user-3", 5, 5, 100.0)
	if err != nil {
		t.Fatalf("refill: %v", err)
	}
	if !v.Allowed {
		t.Errorf("expected allowed after refill")
	}
}

// The marquee test: under heavy concurrency, the bucket math stays exact.
// If Lua atomicity were broken (e.g., we did Get/Set in Go without WATCH),
// concurrent goroutines would race and either over- or under-grant.
func TestRedis_TokenBucket_AtomicityUnderConcurrency(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	const (
		key      = "atomicity-test"
		capacity = 100
		workers  = 100
		// Use a low refill rate so the bucket effectively can only grant `capacity` tokens.
		refillRate = 0.0001
	)

	var allowed atomic.Int64
	var denied atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := be.Allow(ctx, key, 1, capacity, refillRate)
			if err != nil {
				t.Errorf("worker Allow: %v", err)
				return
			}
			if v.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	gotAllowed := allowed.Load()
	gotDenied := denied.Load()

	// With 100 workers and capacity 100, exactly 100 should succeed.
	// Refill is negligible at 0.0001/sec over the test's duration.
	if gotAllowed > int64(capacity) {
		t.Errorf("over-granted: allowed=%d, capacity=%d (atomicity broken)", gotAllowed, capacity)
	}
	if gotAllowed+gotDenied != int64(workers) {
		t.Errorf("counts don't add up: allowed=%d + denied=%d != workers=%d",
			gotAllowed, gotDenied, workers)
	}
	// We should be very close to capacity. Allow some slack for refill.
	if gotAllowed < int64(capacity-1) {
		t.Errorf("under-granted: allowed=%d, capacity=%d", gotAllowed, capacity)
	}
}

// ============================================================
// Sliding window against real Redis
// ============================================================

func TestRedis_SlidingWindow_AllowsWithinLimit(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		v, err := be.AllowSliding(ctx, "sw-1", 1, 3, 1000)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if !v.Allowed {
			t.Errorf("event %d: expected allowed", i)
		}
	}
}

func TestRedis_SlidingWindow_DeniesOverLimit(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, _ = be.AllowSliding(ctx, "sw-2", 1, 3, 1000)
	}
	v, err := be.AllowSliding(ctx, "sw-2", 1, 3, 1000)
	if err != nil {
		t.Fatalf("over: %v", err)
	}
	if v.Allowed {
		t.Errorf("expected denied on 4th event")
	}
}

func TestRedis_SlidingWindow_DropsOldEvents(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, _ = be.AllowSliding(ctx, "sw-3", 1, 3, 200) // 200ms window
	}
	time.Sleep(250 * time.Millisecond)
	v, err := be.AllowSliding(ctx, "sw-3", 1, 3, 200)
	if err != nil {
		t.Fatalf("post-window: %v", err)
	}
	if !v.Allowed {
		t.Errorf("expected allowed after window expired")
	}
}

// ============================================================
// Validation
// ============================================================

func TestRedis_RejectsBadInput(t *testing.T) {
	be := newRedisBackend(t)
	ctx := context.Background()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"empty key (tokenbucket)", func() error {
			_, err := be.Allow(ctx, "", 1, 5, 1.0)
			return err
		}},
		{"zero tokens (tokenbucket)", func() error {
			_, err := be.Allow(ctx, "k", 0, 5, 1.0)
			return err
		}},
		{"zero capacity (tokenbucket)", func() error {
			_, err := be.Allow(ctx, "k", 1, 0, 1.0)
			return err
		}},
		{"empty key (slidingwindow)", func() error {
			_, err := be.AllowSliding(ctx, "", 1, 5, 1000)
			return err
		}},
		{"zero limit (slidingwindow)", func() error {
			_, err := be.AllowSliding(ctx, "k", 1, 0, 1000)
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.fn(); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}
