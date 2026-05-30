package algorithms

import (
	"errors"
	"sync"
	"time"
)

// ErrInvalidTokens is returned when a non-positive token count is requested.
var ErrInvalidTokens = errors.New("tokens must be > 0")

// ErrInvalidConfig is returned when bucket capacity or refill rate is non-positive.
var ErrInvalidConfig = errors.New("capacity and refill_rate must be > 0")

// Clock is the source of monotonic time. Tests inject a fake clock so refill
// behavior is deterministic without sleeping.
type Clock func() time.Time

// TokenBucket is a single-key token bucket. Concurrent calls to Allow are safe.
//
// Bursty by design: the bucket starts full at Capacity and refills at RefillRate
// tokens per second, capped at Capacity. A caller can consume up to Capacity
// tokens in a single burst from a cold-start bucket.
type TokenBucket struct {
	capacity   float64
	refillRate float64 // tokens per second
	clock      Clock

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

// NewTokenBucket constructs a full bucket.
//
// capacity must be > 0; refillRate must be > 0. clock may be nil (defaults to
// time.Now).
func NewTokenBucket(capacity int, refillRate float64, clock Clock) (*TokenBucket, error) {
	if capacity <= 0 || refillRate <= 0 {
		return nil, ErrInvalidConfig
	}
	if clock == nil {
		clock = time.Now
	}
	return &TokenBucket{
		capacity:   float64(capacity),
		refillRate: refillRate,
		clock:      clock,
		tokens:     float64(capacity),
		lastRefill: clock(),
	}, nil
}

// Verdict is the outcome of a single Allow call.
type Verdict struct {
	Allowed      bool
	Remaining    int           // floor of fractional tokens for client-facing reporting
	RetryAfterMs int           // milliseconds until enough tokens accrue (zero when allowed)
	WaitFor      time.Duration // unrounded wait (handy for tests + internal use)
}

// Allow consumes n tokens if the bucket has them. Returns whether the request
// was allowed plus the post-call (or current) token count and an estimated
// wait until the request would be allowed.
//
// Returns ErrInvalidTokens if n <= 0.
func (b *TokenBucket) Allow(n int) (Verdict, error) {
	if n <= 0 {
		return Verdict{}, ErrInvalidTokens
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill()

	want := float64(n)
	if b.tokens >= want {
		b.tokens -= want
		return Verdict{
			Allowed:   true,
			Remaining: int(b.tokens),
		}, nil
	}

	// Deny. Compute when the bucket will have enough.
	deficit := want - b.tokens
	wait := time.Duration(deficit / b.refillRate * float64(time.Second))
	return Verdict{
		Allowed:      false,
		Remaining:    int(b.tokens),
		RetryAfterMs: int(wait / time.Millisecond),
		WaitFor:      wait,
	}, nil
}

// refill brings the bucket up to its current accrued state.
//
// Caller MUST hold b.mu. Refill is lazy: tokens are computed from elapsed
// clock time on demand, not maintained by a background goroutine.
func (b *TokenBucket) refill() {
	now := b.clock()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens = min(b.capacity, b.tokens+elapsed*b.refillRate)
	b.lastRefill = now
}
