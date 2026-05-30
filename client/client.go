// Package client is the public Go library for talking to a bucketd cluster.
//
// In single-node deployments, callers can construct a Client with one
// address and call Allow normally. In multi-node deployments, pass multiple
// addresses; the client uses consistent hashing to route each rate-limit
// key to the same bucketd node, ensuring bucket state stays coherent across
// a fleet of stateless bucketd instances sharing a Redis backend.
//
//	cli, err := client.New([]string{"bucketd-1:50051", "bucketd-2:50051"})
//	if err != nil { ... }
//	defer cli.Close()
//
//	v, err := cli.Allow(ctx, "user-42", 1, client.Limit{
//	    Capacity:   100,
//	    RefillRate: 10,
//	})
//
// The LLM Gateway (sprint 3) imports this package directly. The package
// has no transitive dependency on any other bucketd internal package so
// consumers don't pay for unrelated code.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kevinreber/bucketd/internal/shard"
	ratelimitpb "github.com/kevinreber/bucketd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// defaultServiceConfig retries Allow on UNAVAILABLE (transient network /
// instance restart). Allow is naturally idempotent — granting the same
// bucket twice is exactly what concurrent retries would do anyway, so
// retry-on-failure is safe.
const defaultServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "bucketd.v1.RateLimiter"}],
    "retryPolicy": {
      "MaxAttempts": 3,
      "InitialBackoff": "0.05s",
      "MaxBackoff": "0.5s",
      "BackoffMultiplier": 2.0,
      "RetryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

// defaultKeepalive sends an HTTP/2 ping every 30s on idle connections; if
// the server doesn't respond within 10s, gRPC closes the conn so the next
// RPC re-dials. Protects against silent NAT-table drops common in cloud
// deployments (Fly.io, etc.).
var defaultKeepalive = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: false,
}

// Limit describes a token-bucket rate-limit policy. Bucketd is stateless on
// policy — callers pass these on every Allow call.
type Limit struct {
	Capacity   int32   // Max tokens the bucket can hold.
	RefillRate float64 // Tokens added per second.
}

// Verdict is the outcome of an Allow call, mirroring the proto AllowResponse.
type Verdict struct {
	// Allowed reports whether the requested tokens were granted.
	Allowed bool

	// Remaining is an advisory hint about bucket state after this call.
	//
	// Important: this value is NOT reliable for arithmetic decisions across
	// multiple concurrent callers. On the allow path it reflects the bucket
	// state immediately after this call (other concurrent callers may
	// already have consumed more). On the deny path it reflects the bucket
	// state at the moment the deny decision was made (i.e., the floor of
	// what's left before the request would have succeeded).
	//
	// Use it for UX hints ("X requests remaining") or coarse retry-budget
	// decisions. Don't use it for fairness or scheduling guarantees.
	Remaining int32

	// RetryAfterMs is the estimated time until the bucket would grant a
	// request of the same size, in milliseconds. Zero when Allowed.
	//
	// Computed by the algorithm, not measured — actual refill may vary
	// slightly with clock skew between bucketd nodes when using Redis.
	RetryAfterMs int32
}

// Client routes Allow calls to the right bucketd node for each key.
type Client struct {
	ring *shard.Ring

	mu    sync.RWMutex
	conns map[string]*grpcConn
}

type grpcConn struct {
	conn   *grpc.ClientConn
	client ratelimitpb.RateLimiterClient
}

// New constructs a Client that consistently hashes keys across the given
// bucketd addresses. Returns an error if no addresses are supplied.
//
// The Client lazily dials each address on first use. Callers should call
// Close when done.
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client.New: at least one bucketd address required")
	}
	c := &Client{
		ring:  shard.NewRing(0),
		conns: make(map[string]*grpcConn),
	}
	for _, addr := range addrs {
		c.ring.Add(addr)
	}
	return c, nil
}

// Allow consults the bucketd node that owns `key` and asks for `tokens`
// tokens against the given Limit.
func (c *Client) Allow(
	ctx context.Context,
	key string,
	tokens int32,
	limit Limit,
) (Verdict, error) {
	addr := c.ring.Get(key)
	if addr == "" {
		return Verdict{}, errors.New("client.Allow: ring is empty (no nodes)")
	}

	conn, err := c.connFor(addr)
	if err != nil {
		return Verdict{}, fmt.Errorf("client.Allow: connect to %s: %w", addr, err)
	}

	resp, err := conn.client.Allow(ctx, &ratelimitpb.AllowRequest{
		Key:        key,
		Tokens:     tokens,
		Capacity:   limit.Capacity,
		RefillRate: limit.RefillRate,
	})
	if err != nil {
		return Verdict{}, fmt.Errorf("client.Allow: rpc to %s: %w", addr, err)
	}

	return Verdict{
		Allowed:      resp.GetAllowed(),
		Remaining:    resp.GetRemaining(),
		RetryAfterMs: resp.GetRetryAfterMs(),
	}, nil
}

// RoutedAddr returns the bucketd address that owns the given key. Useful
// for introspection (debug endpoints, logging) and for callers that want
// to know the routing decision without actually issuing an Allow RPC.
// Returns empty string when the ring is empty.
func (c *Client) RoutedAddr(key string) string {
	return c.ring.Get(key)
}

// AddNode adds a bucketd address to the ring at runtime. Lookups after this
// call may move ~1/(N+1) of keys to the new node (consistent hashing's
// minimal-rebalance property).
func (c *Client) AddNode(addr string) {
	c.ring.Add(addr)
}

// RemoveNode drops a bucketd address from the ring and closes any open
// connection to it. Lookups after this call move all of that node's keys
// to other nodes.
//
// Order of operations matters: we close+delete the cached connection FIRST
// (under c.mu), then remove from the ring. Doing it in this order ensures
// that any concurrent Allow that grabbed the soon-to-be-removed address
// from the ring will either (a) find no cached conn and re-dial (which
// will fail or succeed depending on the node's actual liveness — caller's
// problem either way), or (b) use the closed conn and get a clean RPC
// error. Either is recoverable; the inverse order would let a concurrent
// connFor cache a brand-new conn AFTER the ring removal, which RemoveNode
// would never see and which would leak until Close.
func (c *Client) RemoveNode(addr string) {
	c.mu.Lock()
	if gc, ok := c.conns[addr]; ok {
		_ = gc.conn.Close()
		delete(c.conns, addr)
	}
	c.mu.Unlock()
	c.ring.Remove(addr)
}

// Close releases all gRPC connections held by the client.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for addr, gc := range c.conns {
		if err := gc.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, addr)
	}
	return firstErr
}

// connFor returns a cached connection for an address, dialing on first use.
func (c *Client) connFor(addr string) (*grpcConn, error) {
	c.mu.RLock()
	gc, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return gc, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check after upgrading the lock.
	if gc, ok := c.conns[addr]; ok {
		return gc, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(defaultKeepalive),
		grpc.WithDefaultServiceConfig(defaultServiceConfig),
	)
	if err != nil {
		return nil, err
	}
	gc = &grpcConn{
		conn:   conn,
		client: ratelimitpb.NewRateLimiterClient(conn),
	}
	c.conns[addr] = gc
	return gc, nil
}
