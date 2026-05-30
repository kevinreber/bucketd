package client_test

import (
	"fmt"
	"testing"

	"github.com/kevinreber/bucketd/client"
)

func TestNew_RejectsEmptyAddrs(t *testing.T) {
	if _, err := client.New(nil); err == nil {
		t.Errorf("expected error for nil addrs")
	}
	if _, err := client.New([]string{}); err == nil {
		t.Errorf("expected error for empty addrs")
	}
}

func TestRoutedAddr_SameKeyAlwaysSameAddr(t *testing.T) {
	c, err := client.New([]string{"node-a:50051", "node-b:50051", "node-c:50051"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	for _, key := range []string{"user-1", "user-42", "🦔", "very long key here"} {
		first := c.RoutedAddr(key)
		for i := 0; i < 100; i++ {
			if got := c.RoutedAddr(key); got != first {
				t.Errorf("key %q: routing flapped (%q vs %q)", key, first, got)
			}
		}
	}
}

func TestRoutedAddr_KeysSpreadAcrossNodes(t *testing.T) {
	addrs := []string{"node-a:50051", "node-b:50051", "node-c:50051"}
	c, err := client.New(addrs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	counts := map[string]int{}
	for i := 0; i < 3000; i++ {
		counts[c.RoutedAddr(fmt.Sprintf("key-%d", i))]++
	}
	if len(counts) != len(addrs) {
		t.Errorf("expected all 3 nodes to receive keys, got %d distinct: %v", len(counts), counts)
	}
	expected := 3000 / 3
	for n, c := range counts {
		if c < expected*70/100 || c > expected*130/100 {
			t.Errorf("node %s got %d keys, expected ~%d (±30%%)", n, c, expected)
		}
	}
}

func TestRoutedAddr_AddNodeMovesSomeKeys(t *testing.T) {
	c, err := client.New([]string{"a:1", "b:1", "c:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	const numKeys = 3000
	before := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		before[i] = c.RoutedAddr(fmt.Sprintf("key-%d", i))
	}

	c.AddNode("d:1")

	moved := 0
	for i := 0; i < numKeys; i++ {
		if c.RoutedAddr(fmt.Sprintf("key-%d", i)) != before[i] {
			moved++
		}
	}
	pct := float64(moved) / float64(numKeys) * 100
	// Expected ~25%; allow generous slack.
	if pct < 15 || pct > 35 {
		t.Errorf("expected ~25%% keys to move after AddNode, got %.1f%%", pct)
	}
}

func TestRoutedAddr_RemoveNodeMovesOnlyAffected(t *testing.T) {
	c, err := client.New([]string{"a:1", "b:1", "c:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	const numKeys = 3000
	before := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		before[i] = c.RoutedAddr(fmt.Sprintf("key-%d", i))
	}

	c.RemoveNode("a:1")

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		after := c.RoutedAddr(key)
		if before[i] == "a:1" {
			if after == "a:1" {
				t.Errorf("key %q still on a:1 after RemoveNode", key)
			}
		} else {
			if after != before[i] {
				t.Errorf("key %q moved unnecessarily (%q -> %q)", key, before[i], after)
			}
		}
	}
}
