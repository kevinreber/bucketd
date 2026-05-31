package backend

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/kevinreber/bucketd/internal/algorithms"
)

// DefaultMemoryMaxBuckets is the default cap on how many distinct buckets
// the in-memory backend will hold simultaneously. New buckets beyond this
// limit cause LRU eviction of the least-recently-used bucket.
//
// Sized to comfortably fit in a 256 MB Fly.io VM without dominating memory:
// each bucket is ~200 bytes (TokenBucket struct + map overhead). 100k
// buckets is ~20 MB and still leaves plenty of headroom for the rest of
// the process.
const DefaultMemoryMaxBuckets = 100_000

// ErrMemoryFull is returned when the backend cannot create a new bucket
// because the cap has been reached and no eviction candidate exists. This
// should not happen in normal operation (LRU always finds a candidate),
// but is here as a defensive sentinel.
var ErrMemoryFull = errors.New("in-memory backend is full")

// Memory is an in-process backend. One token bucket per (key, capacity, refillRate)
// triple. No persistence, no cross-process coordination; intended for tests and
// for single-node deployments where Redis is overkill.
//
// Bounded by MaxBuckets: when full, the least-recently-used bucket is
// evicted to make room for a new one. This prevents key churn (e.g., per-
// request IDs, UUIDs, or expired API keys flowing through the rate limiter)
// from growing the map without limit.
//
// The outer map is protected by a Mutex (not RWMutex) because every Allow
// touches the LRU list, which is a write. Per-bucket Allow is the bucket's
// own concern; this layer only serializes the bucket lookup + LRU update.
type Memory struct {
	clock      algorithms.Clock
	maxBuckets int

	mu      sync.Mutex
	buckets map[bucketKey]*memoryEntry
	lru     *list.List // front = most recent; back = oldest
}

type memoryEntry struct {
	bucket *algorithms.TokenBucket
	node   *list.Element // pointer back into the LRU list for O(1) moves
	key    bucketKey
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

// NewMemory constructs an empty Memory backend with the default max-buckets
// cap. Pass NewMemoryWithCap for a custom cap.
func NewMemory(clock algorithms.Clock) *Memory {
	return NewMemoryWithCap(clock, DefaultMemoryMaxBuckets)
}

// NewMemoryWithCap is NewMemory with a configurable max-buckets cap. Useful
// for tests that want to exercise LRU eviction without 100k allocations.
func NewMemoryWithCap(clock algorithms.Clock, maxBuckets int) *Memory {
	if maxBuckets <= 0 {
		maxBuckets = DefaultMemoryMaxBuckets
	}
	return &Memory{
		clock:      clock,
		maxBuckets: maxBuckets,
		buckets:    make(map[bucketKey]*memoryEntry),
		lru:        list.New(),
	}
}

// Allow asks the backend's bucket for `key` to grant `tokens` tokens. If a
// bucket with the given (key, capacity, refillRate) shape doesn't exist yet,
// it is lazily created at full capacity, possibly evicting the LRU bucket
// to stay under MaxBuckets.
func (m *Memory) Allow(
	_ context.Context,
	key string,
	tokens int,
	capacity int32,
	refillRate float64,
) (algorithms.Verdict, error) {
	bk := bucketKey{key: key, capacity: capacity, refillRate: refillRate}

	m.mu.Lock()
	entry, ok := m.buckets[bk]
	if ok {
		// Touch: move to front of LRU.
		m.lru.MoveToFront(entry.node)
		m.mu.Unlock()
		return entry.bucket.Allow(tokens)
	}

	// Cache miss: create. Evict if at cap.
	if len(m.buckets) >= m.maxBuckets {
		oldest := m.lru.Back()
		if oldest == nil {
			m.mu.Unlock()
			return algorithms.Verdict{}, ErrMemoryFull
		}
		oldEntry := oldest.Value.(*memoryEntry)
		delete(m.buckets, oldEntry.key)
		m.lru.Remove(oldest)
	}

	tb, err := algorithms.NewTokenBucket(int(capacity), refillRate, m.clock)
	if err != nil {
		m.mu.Unlock()
		return algorithms.Verdict{}, err
	}
	newEntry := &memoryEntry{bucket: tb, key: bk}
	newEntry.node = m.lru.PushFront(newEntry)
	m.buckets[bk] = newEntry
	m.mu.Unlock()

	return tb.Allow(tokens)
}

// Len returns the current number of buckets held by the backend. Useful
// for observability and tests.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buckets)
}
