// Command bench is a load generator for bucketd. Drives a configurable
// number of concurrent workers at a target request rate, records per-call
// latency, and prints summary percentiles (p50, p90, p99, p99.9).
//
// Output is plain text suitable for pasting into the README's benchmarks
// table.
//
// Usage:
//
//	go run ./cmd/bench -addr=localhost:50051 -workers=50 -duration=10s
//	go run ./cmd/bench -addr=localhost:50051 -workers=50 -duration=10s -key-cardinality=100
//
// The -key-cardinality flag controls how many distinct keys the bench
// rotates through. Cardinality=1 means all traffic hits one bucket
// (worst-case contention); larger values spread load across more buckets.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kevinreber/bucketd/client"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "bucketd gRPC address")
	workers := flag.Int("workers", 50, "number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "total run time")
	keyCardinality := flag.Int("key-cardinality", 100, "number of distinct keys to rotate through")
	capacity := flag.Int("capacity", 1000, "token bucket capacity")
	refillRate := flag.Float64("refill-rate", 1000.0, "tokens refilled per second")
	flag.Parse()

	if err := run(*addr, *workers, *duration, *keyCardinality, *capacity, *refillRate); err != nil {
		log.Fatalf("bench: %v", err)
	}
}

func run(
	addr string,
	workers int,
	duration time.Duration,
	keyCardinality int,
	capacity int,
	refillRate float64,
) error {
	cli, err := client.New([]string{addr})
	if err != nil {
		return fmt.Errorf("client.New: %w", err)
	}
	defer func() { _ = cli.Close() }()

	// Warm up: dial the connection and prove the path works before we
	// start timing.
	warmupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = cli.Allow(warmupCtx, "warmup", 1, client.Limit{
		Capacity: int32(capacity), RefillRate: refillRate,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("warmup Allow: %w", err)
	}

	fmt.Fprintf(os.Stderr, "bench: %d workers, %s duration, %d distinct keys\n",
		workers, duration, keyCardinality)
	fmt.Fprintf(os.Stderr, "       capacity=%d, refill_rate=%.0f tokens/sec\n",
		capacity, refillRate)
	fmt.Fprintln(os.Stderr, "starting...")

	// Pre-generate keys so workers don't waste time formatting strings
	// inside the hot loop.
	keys := make([]string, keyCardinality)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
	}

	stop := make(chan struct{})
	time.AfterFunc(duration, func() { close(stop) })

	var (
		latencies []time.Duration
		mu        sync.Mutex
		allowed   atomic.Int64
		denied    atomic.Int64
		errors    atomic.Int64
	)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			var localLatencies []time.Duration
			for {
				select {
				case <-stop:
					mu.Lock()
					latencies = append(latencies, localLatencies...)
					mu.Unlock()
					return
				default:
				}

				key := keys[rng.Intn(len(keys))]
				start := time.Now()
				v, err := cli.Allow(context.Background(), key, 1, client.Limit{
					Capacity:   int32(capacity),
					RefillRate: refillRate,
				})
				elapsed := time.Since(start)

				if err != nil {
					errors.Add(1)
					continue
				}
				localLatencies = append(localLatencies, elapsed)
				if v.Allowed {
					allowed.Add(1)
				} else {
					denied.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	report(latencies, allowed.Load(), denied.Load(), errors.Load(), duration)
	return nil
}

// report prints a markdown-ready summary of the benchmark run.
func report(latencies []time.Duration, allowed, denied, errs int64, duration time.Duration) {
	total := int64(len(latencies))

	if total == 0 {
		fmt.Println("no successful requests recorded")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		idx := int(float64(total) * p)
		if idx >= int(total) {
			idx = int(total) - 1
		}
		return latencies[idx]
	}

	throughput := float64(total) / duration.Seconds()

	fmt.Println()
	fmt.Println("===== bucketd benchmark =====")
	fmt.Printf("Duration:    %s\n", duration)
	fmt.Printf("Requests:    %d total (allow=%d deny=%d err=%d)\n",
		total+errs, allowed, denied, errs)
	fmt.Printf("Throughput:  %.0f req/s\n", throughput)
	fmt.Println()
	fmt.Println("Latency:")
	fmt.Printf("  p50:    %s\n", pct(0.50))
	fmt.Printf("  p90:    %s\n", pct(0.90))
	fmt.Printf("  p99:    %s\n", pct(0.99))
	fmt.Printf("  p99.9:  %s\n", pct(0.999))
	fmt.Printf("  max:    %s\n", latencies[total-1])
	fmt.Println()
	fmt.Println("(paste into README's benchmarks section)")
}
