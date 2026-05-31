// Package shard implements client-side consistent hashing for routing
// rate-limit keys to bucketd nodes.
//
// Each real node is represented by VirtualNodesPerNode points on a uint64
// hash ring; keys are hashed and assigned to the next virtual node clockwise.
// Virtual nodes smooth the distribution: with one virtual node per real node,
// a 3-node ring can have keys land 70/20/10% on bad hash luck. With ~150
// virtual nodes per real node, the distribution is within a few percent of
// uniform.
//
// Why hand-rolled instead of a library: this is the interview-relevant part.
// Libraries exist (e.g., github.com/serialx/hashring) but knowing the
// implementation cold matters more than the LoC saved.
package shard

import (
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// VirtualNodesPerNode is the number of points each real node gets on the
// ring. 150 is the conventional default — large enough to smooth the
// distribution to within a few percent of uniform, small enough that
// memory and rebuild cost stay negligible.
const VirtualNodesPerNode = 150

// Ring is a consistent-hash ring. Safe for concurrent use; reads (Get)
// dominate writes (Add/Remove), so guarded by an RWMutex.
type Ring struct {
	virtualNodes int

	mu        sync.RWMutex
	positions []uint64          // sorted ring positions
	owners    map[uint64]string // position -> real node address
}

// NewRing constructs an empty ring. Pass VirtualNodesPerNode in normal use;
// other values are intended for tests.
func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = VirtualNodesPerNode
	}
	return &Ring{
		virtualNodes: virtualNodes,
		owners:       make(map[uint64]string),
	}
}

// Add inserts a node into the ring. Adding an already-present node is a
// no-op (existing positions are kept). Returns the count of positions added.
//
// Hash collisions in uint64-space are vanishingly rare (~450 points among
// 18 quintillion slots), but if hashVirtualNode(node, i) ever lands on an
// existing position, we increment the index and try again instead of
// skipping. This guarantees every node gets exactly r.virtualNodes positions,
// preventing permanent load imbalance after a remove + re-add of a node that
// previously collided.
func (r *Ring) Add(node string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Don't re-seed if the node is already in the ring.
	for _, owner := range r.owners {
		if owner == node {
			return 0
		}
	}

	added := 0
	probe := 0
	const maxProbes = 10_000 // defensive cap; in practice collisions are exceedingly rare
	for added < r.virtualNodes && probe < r.virtualNodes+maxProbes {
		pos := hashVirtualNode(node, probe)
		probe++
		if _, exists := r.owners[pos]; exists {
			continue // collision; keep probing
		}
		r.owners[pos] = node
		r.positions = append(r.positions, pos)
		added++
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
	return added
}

// Remove drops all virtual-node positions for the given node. Returns the
// count of positions removed. Removing a missing node is a no-op.
func (r *Ring) Remove(node string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	kept := r.positions[:0]
	for _, pos := range r.positions {
		if r.owners[pos] == node {
			delete(r.owners, pos)
			removed++
			continue
		}
		kept = append(kept, pos)
	}
	r.positions = kept
	return removed
}

// Get returns the node that owns the given key. Returns empty string when
// the ring is empty.
//
// Algorithm: hash the key into uint64-space, binary-search for the smallest
// virtual-node position >= the key's hash. Wraps around to the first
// position if the key hashes past the largest position.
func (r *Ring) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.positions) == 0 {
		return ""
	}
	h := xxhash.Sum64String(key)
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})
	if idx == len(r.positions) {
		idx = 0 // wrap
	}
	return r.owners[r.positions[idx]]
}

// Nodes returns the set of real nodes currently in the ring, sorted
// alphabetically. Useful for tests and debug endpoints.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, node := range r.owners {
		seen[node] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// hashVirtualNode places one of a node's virtual nodes onto the ring.
//
// The virtual-node index gets mixed into the hash so a single real node
// produces N distinct positions. Using node + ":" + i as the input is the
// conventional trick.
func hashVirtualNode(node string, i int) uint64 {
	return xxhash.Sum64String(node + ":" + strconv.Itoa(i))
}
