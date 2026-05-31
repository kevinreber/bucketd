package backend

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/kevinreber/bucketd/internal/algorithms"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/tokenbucket.lua
var tokenBucketScript string

//go:embed lua/slidingwindow.lua
var slidingWindowScript string

// ErrEmptyKey is returned when the caller passes a zero-length key.
var ErrEmptyKey = errors.New("key must not be empty")

// Redis is a Redis-backed rate-limit storage. Concurrency safety is provided
// by Redis's single-threaded execution of Lua scripts — bucketd state is
// modified atomically inside one Redis cycle per Allow call.
//
// Both algorithms (token bucket, sliding window) are supported via the
// AllowSliding method for explicit sliding-window use, while Allow uses the
// token bucket (matching the gRPC contract's stateless-config shape).
type Redis struct {
	client *redis.Client

	tokenBucket   *redis.Script
	slidingWindow *redis.Script
}

// NewRedis wires a Redis backend to an existing redis.Client. The caller
// owns the client lifecycle (pool size, timeouts, auth).
//
// Use NewRedisWithPreload at startup to fail fast if Redis is unreachable
// or refuses to load the scripts.
func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		client:        client,
		tokenBucket:   redis.NewScript(tokenBucketScript),
		slidingWindow: redis.NewScript(slidingWindowScript),
	}
}

// NewRedisWithPreload is NewRedis plus an upfront SCRIPT LOAD of both Lua
// scripts. If Redis is unreachable or rejects the scripts, the returned
// error surfaces immediately at startup instead of on the first user-facing
// Allow call. Recommended for production wiring.
func NewRedisWithPreload(ctx context.Context, client *redis.Client) (*Redis, error) {
	r := NewRedis(client)
	if err := r.tokenBucket.Load(ctx, client).Err(); err != nil {
		return nil, fmt.Errorf("preload tokenbucket script: %w", err)
	}
	if err := r.slidingWindow.Load(ctx, client).Err(); err != nil {
		return nil, fmt.Errorf("preload slidingwindow script: %w", err)
	}
	return r, nil
}

// Allow runs the token-bucket script against Redis for the given key.
//
// The Backend interface in the server package expects this signature so
// the gRPC service can swap between Memory and Redis without conditionals.
func (r *Redis) Allow(
	ctx context.Context,
	key string,
	tokens int,
	capacity int32,
	refillRate float64,
) (algorithms.Verdict, error) {
	if key == "" {
		return algorithms.Verdict{}, ErrEmptyKey
	}
	if tokens <= 0 {
		return algorithms.Verdict{}, algorithms.ErrInvalidTokens
	}
	if capacity <= 0 || refillRate <= 0 {
		return algorithms.Verdict{}, algorithms.ErrInvalidConfig
	}

	raw, err := r.tokenBucket.Run(
		ctx,
		r.client,
		[]string{tokenBucketKey(key)},
		capacity, refillRate, tokens,
	).Result()
	if err != nil {
		return algorithms.Verdict{}, fmt.Errorf("redis tokenbucket script: %w", err)
	}

	return parseTriple(raw)
}

// AllowSliding runs the sliding-window script. Exposed in addition to Allow
// so callers can pick the algorithm explicitly. The gRPC layer in Phase 1
// only wires Allow (token bucket); routing by algorithm choice is a Phase 4
// concern when the proto grows an `algorithm` field.
func (r *Redis) AllowSliding(
	ctx context.Context,
	key string,
	tokens int,
	limit int32,
	windowMs int32,
) (algorithms.Verdict, error) {
	if key == "" {
		return algorithms.Verdict{}, ErrEmptyKey
	}
	if tokens <= 0 {
		return algorithms.Verdict{}, algorithms.ErrInvalidTokens
	}
	if limit <= 0 || windowMs <= 0 {
		return algorithms.Verdict{}, algorithms.ErrInvalidConfig
	}

	raw, err := r.slidingWindow.Run(
		ctx,
		r.client,
		[]string{slidingWindowZsetKey(key), slidingWindowSeqKey(key)},
		limit, windowMs, tokens,
	).Result()
	if err != nil {
		return algorithms.Verdict{}, fmt.Errorf("redis slidingwindow script: %w", err)
	}

	return parseTriple(raw)
}

// parseTriple decodes a Redis array reply of the form
// {allowed (int 0|1), remaining (int), retry_after_ms (int)} into a Verdict.
func parseTriple(raw interface{}) (algorithms.Verdict, error) {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) != 3 {
		return algorithms.Verdict{}, fmt.Errorf("unexpected reply shape: %T %v", raw, raw)
	}
	allowed, ok1 := arr[0].(int64)
	remaining, ok2 := arr[1].(int64)
	retry, ok3 := arr[2].(int64)
	if !ok1 || !ok2 || !ok3 {
		return algorithms.Verdict{}, fmt.Errorf("non-integer in reply: %v", arr)
	}
	return algorithms.Verdict{
		Allowed:      allowed == 1,
		Remaining:    int(remaining),
		RetryAfterMs: int(retry),
	}, nil
}

// Key prefixes keep bucketd state from colliding with other Redis tenants.

func tokenBucketKey(key string) string {
	return "bucketd:tb:" + key
}

func slidingWindowZsetKey(key string) string {
	return "bucketd:sw:" + key
}

func slidingWindowSeqKey(key string) string {
	return "bucketd:sw:" + key + ":seq"
}
