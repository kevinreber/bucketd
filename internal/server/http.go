package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/kevinreber/bucketd/internal/algorithms"
	"github.com/kevinreber/bucketd/internal/backend"
	"github.com/kevinreber/bucketd/internal/observe"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPMux returns the http.Handler that bucketd serves on its HTTP port.
//
// Routes:
//   POST /v1/allow — JSON in/out, mirrors the gRPC Allow method for
//                    non-gRPC callers (e.g., a Python script, a webhook).
//   GET  /healthz  — 200 SERVING when serving == 1, 503 NOT_SERVING when
//                    serving == 0. Fly.io and similar load balancers can
//                    poll this to gate traffic.
//   GET  /metrics  — Prometheus exposition format (counters, histograms,
//                    gauges registered via the observe package).
//
// `serving` is a pointer to an atomic int32 owned by Run; flipping it to 0
// during shutdown causes /healthz to return 503 while the gRPC server
// drains in-flight RPCs.
func HTTPMux(be Backend, serving *atomic.Int32) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/allow", makeAllowHandler(be))
	mux.HandleFunc("GET /healthz", makeHealthHandler(serving))
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

// allowRequest is the JSON shape consumed by POST /v1/allow.
type allowRequest struct {
	Key        string  `json:"key"`
	Tokens     int32   `json:"tokens"`
	Capacity   int32   `json:"capacity"`
	RefillRate float64 `json:"refill_rate"`
}

// allowResponse is the JSON shape returned by POST /v1/allow.
type allowResponse struct {
	Allowed      bool  `json:"allowed"`
	Remaining    int32 `json:"remaining"`
	RetryAfterMs int32 `json:"retry_after_ms"`
}

func makeAllowHandler(be Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer observe.ObserveAllowDuration(start)
		w.Header().Set("Content-Type", "application/json")

		var req allowRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			observe.RecordAllow(observe.ResultError)
			writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		if req.Key == "" {
			observe.RecordAllow(observe.ResultError)
			writeJSONError(w, http.StatusBadRequest, "key must not be empty")
			return
		}

		v, err := be.Allow(r.Context(), req.Key, int(req.Tokens), req.Capacity, req.RefillRate)
		if err != nil {
			observe.RecordAllow(observe.ResultError)
			code := http.StatusInternalServerError
			if errors.Is(err, algorithms.ErrInvalidTokens) ||
				errors.Is(err, algorithms.ErrInvalidConfig) ||
				errors.Is(err, backend.ErrEmptyKey) {
				code = http.StatusBadRequest
			} else if errors.Is(err, context.DeadlineExceeded) {
				code = http.StatusGatewayTimeout
			} else if errors.Is(err, context.Canceled) {
				code = 499 // nginx convention for "Client Closed Request"
			}
			writeJSONError(w, code, err.Error())
			return
		}

		if v.Allowed {
			observe.RecordAllow(observe.ResultAllowed)
		} else {
			observe.RecordAllow(observe.ResultDenied)
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(allowResponse{
			Allowed:      v.Allowed,
			Remaining:    int32(v.Remaining),
			RetryAfterMs: int32(v.RetryAfterMs),
		})
	}
}

func makeHealthHandler(serving *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if serving.Load() == 1 {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, "SERVING")
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, "NOT_SERVING")
	}
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
