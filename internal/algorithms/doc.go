// Package algorithms implements the two rate-limit algorithms:
//
//   - tokenbucket.go — classic token bucket (bursty)
//   - slidingwindow.go — log-based sliding window (smooth)
//
// Both expose the same Algorithm interface so backend code can swap them
// without conditionals.
//
// Scaffolding stub — algorithms land in Phase 1 (token bucket) and
// Phase 2 (sliding window).
package algorithms
