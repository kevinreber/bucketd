package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kevinreber/bucketd/internal/backend"
	"github.com/kevinreber/bucketd/internal/server"
)

// newHTTPTestServer wires the HTTP mux against an in-memory backend and
// returns the base URL plus a shutdown func. The `serving` atomic is exposed
// so tests can flip it to exercise /healthz.
func newHTTPTestServer(t *testing.T) (baseURL string, serving *atomic.Int32, cleanup func()) {
	t.Helper()
	addr := pickFreePort(t)
	be := backend.NewMemory(nil)
	var srv atomic.Int32
	srv.Store(1)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           server.HTTPMux(be, &srv),
		ReadHeaderTimeout: 2 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = httpSrv.ListenAndServe()
	}()

	// Wait for the listener to accept connections.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := (&http.Client{Timeout: 200 * time.Millisecond}).Get("http://" + addr + "/healthz")
		if err == nil {
			_ = c.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	return "http://" + addr, &srv, func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = httpSrv.Shutdown(shCtx)
		cancel()
		wg.Wait()
	}
}

func TestHTTP_AllowHappyPath(t *testing.T) {
	base, _, cleanup := newHTTPTestServer(t)
	defer cleanup()

	body := `{"key":"u-1","tokens":1,"capacity":5,"refill_rate":1.0}`
	resp, err := http.Post(base+"/v1/allow", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/allow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var out struct {
		Allowed      bool  `json:"allowed"`
		Remaining    int32 `json:"remaining"`
		RetryAfterMs int32 `json:"retry_after_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Allowed || out.Remaining != 4 || out.RetryAfterMs != 0 {
		t.Errorf("unexpected verdict: %+v", out)
	}
}

func TestHTTP_AllowRejectsBadJSON(t *testing.T) {
	base, _, cleanup := newHTTPTestServer(t)
	defer cleanup()

	resp, err := http.Post(base+"/v1/allow", "application/json", strings.NewReader(`{not json`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTP_AllowRejectsEmptyKey(t *testing.T) {
	base, _, cleanup := newHTTPTestServer(t)
	defer cleanup()

	body := `{"key":"","tokens":1,"capacity":5,"refill_rate":1.0}`
	resp, err := http.Post(base+"/v1/allow", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTP_AllowRejectsBadConfig(t *testing.T) {
	base, _, cleanup := newHTTPTestServer(t)
	defer cleanup()

	body := `{"key":"k","tokens":1,"capacity":0,"refill_rate":1.0}`
	resp, err := http.Post(base+"/v1/allow", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTP_HealthzReportsServing(t *testing.T) {
	base, serving, cleanup := newHTTPTestServer(t)
	defer cleanup()

	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "SERVING") {
		t.Errorf("body = %q, want to contain SERVING", body)
	}

	// Flip to NOT_SERVING and re-check.
	serving.Store(0)
	resp2, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz (drain): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != 503 {
		t.Errorf("drain status = %d, want 503", resp2.StatusCode)
	}
}

func TestHTTP_MetricsExposesAllowCounter(t *testing.T) {
	base, _, cleanup := newHTTPTestServer(t)
	defer cleanup()

	// Drive one Allow to bump the counter.
	body := `{"key":"metrics-test","tokens":1,"capacity":5,"refill_rate":1.0}`
	resp, err := http.Post(base+"/v1/allow", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/allow: %v", err)
	}
	resp.Body.Close()

	// Scrape /metrics and assert our metric is present.
	mresp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = mresp.Body.Close() }()
	body2, _ := io.ReadAll(mresp.Body)
	if !bytes.Contains(body2, []byte("bucketd_allow_total")) {
		t.Errorf("expected bucketd_allow_total in /metrics output, got:\n%s", body2)
	}
	if !bytes.Contains(body2, []byte("bucketd_allow_duration_seconds")) {
		t.Errorf("expected bucketd_allow_duration_seconds in /metrics output")
	}
}
