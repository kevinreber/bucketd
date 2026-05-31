// Package observe holds Prometheus instrumentation for bucketd.
//
// Three metrics for the v0.1.0 surface:
//
//   - bucketd_allow_total{result}        — counter, result ∈ {allowed, denied, error}
//   - bucketd_allow_duration_seconds     — histogram of Allow handler latency
//   - bucketd_memory_backend_buckets     — gauge of the in-memory backend's
//                                          current bucket count (zero/unset when
//                                          using the Redis backend)
//
// Metrics are package-level singletons registered against prometheus.DefaultRegisterer
// so net/http's promhttp.Handler() exposes them on /metrics with no extra wiring.
package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ResultAllowed = "allowed"
	ResultDenied  = "denied"
	ResultError   = "error"
)

var (
	allowTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bucketd_allow_total",
			Help: "Total Allow RPCs received, partitioned by outcome.",
		},
		[]string{"result"},
	)

	allowDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "bucketd_allow_duration_seconds",
			Help: "Latency of the Allow handler, end to end.",
			// Buckets tuned for the expected range: a fast in-memory hit
			// is sub-millisecond, a Redis Lua round-trip is single-digit
			// milliseconds, network hiccups push to tens. Anything beyond
			// 100ms is a deep tail.
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0},
		},
	)

	memoryBuckets = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "bucketd_memory_backend_buckets",
			Help: "Number of buckets currently held by the in-memory backend. " +
				"Zero / unset when the Redis backend is in use.",
		},
	)
)

// RecordAllow increments the per-result counter. Call exactly once per Allow
// RPC, in the gRPC handler.
func RecordAllow(result string) {
	allowTotal.WithLabelValues(result).Inc()
}

// ObserveAllowDuration records the duration of an Allow RPC. Use with defer:
//
//	defer observe.ObserveAllowDuration(time.Now())
func ObserveAllowDuration(start time.Time) {
	allowDuration.Observe(time.Since(start).Seconds())
}

// SetMemoryBuckets reports the current in-memory backend size. Call from a
// background goroutine on a tick when using the in-memory backend; harmless
// to leave at zero when using Redis.
func SetMemoryBuckets(n int) {
	memoryBuckets.Set(float64(n))
}
