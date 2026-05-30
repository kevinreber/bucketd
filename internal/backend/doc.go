// Package backend holds the storage backends for rate-limit state.
//
//   - memory.go — in-process backend, one bucket per (key, capacity, rate). Implemented in Phase 1.
//   - redis.go — Redis-backed via embedded Lua scripts. Lands in Phase 2.
//
// Backends are responsible for routing a request to the right per-key bucket
// and returning the algorithm's Verdict. Algorithm choice (token bucket vs
// sliding window) is the backend's concern; for now Memory always uses
// token bucket.
package backend
