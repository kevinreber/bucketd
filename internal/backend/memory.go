package backend

import (
	"context"
	"sync"

	"github.com/kevinreber/bucketd/internal/algorithms"
)

// Memory is an in-process backend. One token bucket per (key, capacity, refillRate)
// triple. No persistence, no cross-process coordination; intended for tests and
// for single-node deployments where Redis is overkill.
//
// The outer map is protected by an RWMutex; bucket creation is rare (one per
// new key) while reads (lookup) dominate, so RWLock is the right shape.
// Per-bucket safety is the bucket's own mu.
type Memory struct {
	clock algorithms.Clock

	mu      sync.RWMutex
	buckets map[bucketKey]*algorithms.TokenBucket
}

// bucketKey distinguishes buckets with the same client key but different
// rate-limit configs. Callers passing different (capacity, refillRate) for
// the same key get different buckets — by design, since bucketd is config-
// less and trusts the caller's parameters.
type bucketKey struct {
	key        string
	capacity   int32
	refillRate float64
}

// NewMemory constructs an empty Memory backend. clock may be nil (defaults
// to time.Now via algorithms.NewTokenBucket).
func NewMemory(clock algorithms.Clock) *Memory {
	return &Memory{
		clock:   clock,
		buckets: make(map[bucketKey]*algorithms.TokenBucket),
	}
}

// Allow asks the backend's bucket for `key` to grant `tokens` tokens. If a
// bucket with the given (key, capacity, refillRate) shape doesn't exist yet,
// it is lazily created at full capacity.
func (m *Memory) Allow(
	_ context.Context,
	key string,
	tokens int,
	capacity int32,
	refillRate float64,
) (algorithms.Verdict, error) {
	bk := bucketKey{key: key, capacity: capacity, refillRate: refillRate}

	// Fast path: read-lock to find an existing bucket.
	m.mu.RLock()
	b, ok := m.buckets[bk]
	m.mu.RUnlock()

	if !ok {
		// Slow path: write-lock and double-check (another goroutine may have
		// created the same bucket while we were waiting).
		m.mu.Lock()
		b, ok = m.buckets[bk]
		if !ok {
			nb, err := algorithms.NewTokenBucket(int(capacity), refillRate, m.clock)
			if err != nil {
				m.mu.Unlock()
				return algorithms.Verdict{}, err
			}
			m.buckets[bk] = nb
			b = nb
		}
		m.mu.Unlock()
	}

	return b.Allow(tokens)
}
