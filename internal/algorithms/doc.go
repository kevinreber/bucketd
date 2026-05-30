// Package algorithms implements the rate-limit algorithms.
//
//   - tokenbucket.go — classic token bucket (bursty). Implemented in Phase 1.
//   - slidingwindow.go — log-based sliding window (smooth). Lands in Phase 2.
//
// Both algorithms expose a Verdict struct describing the allow/deny outcome
// plus retry hints. Backend code (memory, Redis) wraps them per key.
package algorithms
