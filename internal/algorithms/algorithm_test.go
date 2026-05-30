package algorithms_test

import (
	"testing"
	"time"

	"github.com/kevinreber/bucketd/internal/algorithms"
)

// ============================================================
// TokenBucket
// ============================================================

func TestTokenBucket_StartsFull(t *testing.T) {
	tb, err := algorithms.NewTokenBucket(5, 1.0, fixedClock(time.Unix(0, 0)))
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	v, _ := tb.Allow(5)
	if !v.Allowed {
		t.Fatalf("expected allowed on first drain to capacity")
	}
	if v.Remaining != 0 {
		t.Errorf("expected remaining=0 after draining capacity, got %d", v.Remaining)
	}
}

func TestTokenBucket_RefillsLazily(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	tb, _ := algorithms.NewTokenBucket(5, 10.0, clock)

	// drain
	if v, _ := tb.Allow(5); !v.Allowed {
		t.Fatalf("drain should succeed")
	}
	// 6th immediate request denied
	if v, _ := tb.Allow(1); v.Allowed {
		t.Errorf("expected immediate retry to be denied")
	}
	// advance 500ms -> 5 tokens accrued
	now = now.Add(500 * time.Millisecond)
	if v, _ := tb.Allow(5); !v.Allowed {
		t.Errorf("expected allow after refill")
	}
}

func TestTokenBucket_CapsAtCapacity(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	tb, _ := algorithms.NewTokenBucket(5, 10.0, clock)

	// advance a huge amount; bucket should still cap at 5
	now = now.Add(1 * time.Hour)
	if v, _ := tb.Allow(5); !v.Allowed {
		t.Errorf("cap test: expected allow at capacity")
	}
	if v, _ := tb.Allow(1); v.Allowed {
		t.Errorf("cap test: should be empty immediately after draining cap")
	}
}

func TestTokenBucket_DenyReturnsRetryAfter(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	tb, _ := algorithms.NewTokenBucket(2, 10.0, clock) // 10/sec -> 100ms per token

	_, _ = tb.Allow(2) // drain
	v, _ := tb.Allow(1)
	if v.Allowed {
		t.Fatalf("expected deny after drain")
	}
	if v.RetryAfterMs < 90 || v.RetryAfterMs > 110 {
		t.Errorf("expected retry_after ~100ms, got %d", v.RetryAfterMs)
	}
}

func TestTokenBucket_RejectsBadConfig(t *testing.T) {
	for _, c := range []struct {
		name string
		cap  int
		rate float64
	}{
		{"zero capacity", 0, 1.0},
		{"negative capacity", -1, 1.0},
		{"zero rate", 5, 0},
		{"negative rate", 5, -1.0},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := algorithms.NewTokenBucket(c.cap, c.rate, nil); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

func TestTokenBucket_RejectsBadTokens(t *testing.T) {
	tb, _ := algorithms.NewTokenBucket(5, 1.0, nil)
	for _, n := range []int{0, -1, -100} {
		if _, err := tb.Allow(n); err == nil {
			t.Errorf("Allow(%d) should error", n)
		}
	}
}

// ============================================================
// SlidingWindow
// ============================================================

func TestSlidingWindow_AllowsWithinLimit(t *testing.T) {
	clock := fixedClock(time.Unix(0, 0))
	sw, err := algorithms.NewSlidingWindow(3, time.Second, clock)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	for i := 0; i < 3; i++ {
		if v, _ := sw.Allow(1); !v.Allowed {
			t.Fatalf("event %d: expected allowed", i)
		}
	}
}

func TestSlidingWindow_DeniesOverLimit(t *testing.T) {
	clock := fixedClock(time.Unix(0, 0))
	sw, _ := algorithms.NewSlidingWindow(3, time.Second, clock)
	for i := 0; i < 3; i++ {
		_, _ = sw.Allow(1)
	}
	if v, _ := sw.Allow(1); v.Allowed {
		t.Errorf("expected deny on 4th event in same window")
	}
}

func TestSlidingWindow_DropsOldEvents(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	sw, _ := algorithms.NewSlidingWindow(3, time.Second, clock)

	for i := 0; i < 3; i++ {
		_, _ = sw.Allow(1)
	}
	now = now.Add(1500 * time.Millisecond) // past the window
	if v, _ := sw.Allow(1); !v.Allowed {
		t.Errorf("expected allow after window expired")
	}
}

func TestSlidingWindow_RetryAfterPointsToOldestExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	sw, _ := algorithms.NewSlidingWindow(2, time.Second, clock)

	_, _ = sw.Allow(1)                  // at t=0
	now = now.Add(200 * time.Millisecond)
	_, _ = sw.Allow(1)                  // at t=200ms
	v, _ := sw.Allow(1)                 // denied; oldest expires at t=1000ms, so retry in 800ms
	if v.Allowed {
		t.Fatalf("expected deny")
	}
	if v.RetryAfterMs < 790 || v.RetryAfterMs > 810 {
		t.Errorf("expected retry_after ~800ms, got %d", v.RetryAfterMs)
	}
}

func TestSlidingWindow_RejectsBadConfig(t *testing.T) {
	for _, c := range []struct {
		name   string
		limit  int
		window time.Duration
	}{
		{"zero limit", 0, time.Second},
		{"negative limit", -1, time.Second},
		{"zero window", 5, 0},
		{"negative window", 5, -time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := algorithms.NewSlidingWindow(c.limit, c.window, nil); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

// helpers

func fixedClock(t time.Time) algorithms.Clock {
	return func() time.Time { return t }
}
