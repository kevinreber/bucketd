// Package backend holds the storage backends for rate-limit state.
//
//   - memory.go — in-process, no external deps, for tests + low-traffic
//   - redis.go — production, Lua-script-based, supports multi-node
//
// Scaffolding stub — implementations land in Phase 1 (memory) and
// Phase 2 (Redis + Lua).
package backend
