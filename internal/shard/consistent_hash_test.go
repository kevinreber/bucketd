package shard_test

import (
	"fmt"
	"testing"

	"github.com/kevinreber/bucketd/internal/shard"
)

func TestRing_EmptyRingReturnsEmpty(t *testing.T) {
	r := shard.NewRing(0)
	if got := r.Get("anything"); got != "" {
		t.Errorf("empty ring should return empty string, got %q", got)
	}
}

func TestRing_SingleNodeOwnsAll(t *testing.T) {
	r := shard.NewRing(0)
	r.Add("node-a")
	for _, k := range []string{"a", "b", "c", "user-42", "🦔"} {
		if got := r.Get(k); got != "node-a" {
			t.Errorf("Get(%q) = %q, want node-a", k, got)
		}
	}
}

func TestRing_DistributionIsRoughlyEven(t *testing.T) {
	r := shard.NewRing(0)
	nodes := []string{"a", "b", "c"}
	for _, n := range nodes {
		r.Add(n)
	}

	counts := map[string]int{}
	const numKeys = 30_000
	for i := 0; i < numKeys; i++ {
		counts[r.Get(fmt.Sprintf("key-%d", i))]++
	}

	expected := numKeys / len(nodes)        // 10,000 per node
	tolerance := (expected * 15) / 100      // ±15% of perfect uniformity
	// With 150 virtual nodes per real node and only 3 real nodes, the
	// theoretical variance is small but real-world distribution noise can
	// reach ~10-15%. We're asserting "roughly even", not "exact uniform."
	for n, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("node %s got %d keys, expected ~%d±%d", n, c, expected, tolerance)
		}
	}
}

// The headline correctness property: adding a node should redistribute only
// ~1/(N+1) of keys, not the whole space. If the redistribution percentage
// is close to 100% the hash function or virtual-node setup is wrong.
func TestRing_AddingNodeRedistributesOnlySmallFraction(t *testing.T) {
	r := shard.NewRing(0)
	for _, n := range []string{"a", "b", "c"} {
		r.Add(n)
	}

	// Take a snapshot of key -> node assignments with 3 nodes.
	const numKeys = 10_000
	keys := make([]string, numKeys)
	before := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		before[i] = r.Get(keys[i])
	}

	// Add a fourth node.
	r.Add("d")

	moved := 0
	for i, k := range keys {
		if r.Get(k) != before[i] {
			moved++
		}
	}

	// Expected: ~1/4 of keys move when adding the 4th node (25%).
	// Bounded check at 35% gives slack for virtual-node distribution noise.
	pct := float64(moved) / float64(numKeys) * 100
	if pct > 35 {
		t.Errorf("redistribution too high: %.1f%% moved (expected ~25%%)", pct)
	}
	if pct < 15 {
		t.Errorf("redistribution suspiciously low: %.1f%% moved", pct)
	}
}

func TestRing_RemoveRedistributesOnlyAffected(t *testing.T) {
	r := shard.NewRing(0)
	for _, n := range []string{"a", "b", "c"} {
		r.Add(n)
	}

	const numKeys = 10_000
	keys := make([]string, numKeys)
	before := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		before[i] = r.Get(keys[i])
	}

	r.Remove("a")

	// Every key that was on "a" must move. No key that wasn't on "a" should move.
	for i, k := range keys {
		after := r.Get(k)
		if before[i] == "a" {
			if after == "a" {
				t.Errorf("key %q still on a after Remove(a)", k)
			}
		} else {
			if after != before[i] {
				t.Errorf("key %q moved from %q to %q after Remove(a) (should be stable)",
					k, before[i], after)
			}
		}
	}
}

func TestRing_AddRemoveAddIsIdempotent(t *testing.T) {
	r := shard.NewRing(0)
	r.Add("a")
	r.Add("a") // dup
	r.Remove("a")
	if got := r.Get("anything"); got != "" {
		t.Errorf("expected empty ring after remove, got %q", got)
	}
	r.Add("a")
	if got := r.Get("anything"); got != "a" {
		t.Errorf("expected re-add to work, got %q", got)
	}
}

func TestRing_Nodes(t *testing.T) {
	r := shard.NewRing(0)
	for _, n := range []string{"c", "a", "b"} {
		r.Add(n)
	}
	got := r.Nodes()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Nodes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Nodes()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
