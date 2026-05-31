package backend_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/kevinreber/bucketd/internal/backend"
)

func TestMemory_BucketCacheReusesEntries(t *testing.T) {
	be := backend.NewMemoryWithCap(nil, 10)

	for i := 0; i < 5; i++ {
		if _, err := be.Allow(context.Background(), "key-1", 1, 5, 1.0); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}
	if got := be.Len(); got != 1 {
		t.Errorf("expected 1 bucket (same key reused), got %d", got)
	}
}

func TestMemory_LRUEvictsOldestWhenFull(t *testing.T) {
	be := backend.NewMemoryWithCap(nil, 3)
	ctx := context.Background()

	// Fill to cap: 3 distinct keys.
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("k-%d", i)
		if _, err := be.Allow(ctx, key, 1, 5, 1.0); err != nil {
			t.Fatalf("Allow %q: %v", key, err)
		}
	}
	if got := be.Len(); got != 3 {
		t.Fatalf("expected len=3, got %d", got)
	}

	// Touch k-0 so it's MRU.
	_, _ = be.Allow(ctx, "k-0", 1, 5, 1.0)

	// Add a 4th key — should evict k-1 (now LRU after the touch).
	_, _ = be.Allow(ctx, "k-3", 1, 5, 1.0)

	if got := be.Len(); got != 3 {
		t.Errorf("expected len=3 after eviction, got %d", got)
	}

	// k-1's bucket should be gone — a new Allow on it must come from a
	// fresh full bucket. We can't observe the eviction directly, but a
	// fresh bucket allows the next 5 requests in a row.
	for i := 0; i < 5; i++ {
		v, err := be.Allow(ctx, "k-1", 1, 5, 1.0)
		if err != nil {
			t.Fatalf("k-1 readd[%d]: %v", i, err)
		}
		if !v.Allowed {
			t.Errorf("k-1 readd[%d]: expected allow (fresh bucket), got deny", i)
		}
	}
}

func TestMemory_DistinctConfigsAreDistinctBuckets(t *testing.T) {
	be := backend.NewMemoryWithCap(nil, 10)
	ctx := context.Background()

	// Same key, different config = different bucket.
	_, _ = be.Allow(ctx, "k", 1, 5, 1.0)
	_, _ = be.Allow(ctx, "k", 1, 10, 1.0) // different capacity

	if got := be.Len(); got != 2 {
		t.Errorf("expected 2 buckets for distinct configs, got %d", got)
	}
}
