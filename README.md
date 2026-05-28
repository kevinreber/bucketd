# bucketd

Distributed rate limiter in Go — token bucket + sliding window algorithms, Redis Lua atomic scripts, consistent hashing for horizontal distribution, gRPC API.

## Status

🚧 **Scaffolding only — implementation in progress.** This is Sprint 2 of a multi-sprint plan. The actual algorithm, backend, and distribution code lands over the next two weeks (May 30 – Jun 13, 2026).

The repo is public now so that the LLM Gateway project (Sprint 3) has a stable module path to depend on (`github.com/kevinreber/bucketd/client`).

## What's planned

| Concern | Choice |
|---|---|
| Language | Go 1.23+ |
| RPC | gRPC + Protocol Buffers |
| Algorithms | Token bucket (bursty) + sliding window (smooth) |
| Atomicity | Redis Lua scripts, `go:embed`-ed into the binary |
| Distribution | Client-side consistent hashing with virtual nodes |
| Metrics | Prometheus |
| Deploy | Fly.io |
| License | MIT |

## Architecture (planned)

```
bucketd/
├── client/                  # Public importable Go library
├── internal/
│   ├── algorithms/
│   │   ├── tokenbucket.go
│   │   ├── slidingwindow.go
│   │   └── lua/             # go:embed Lua scripts
│   ├── backend/
│   │   ├── redis.go
│   │   └── memory.go
│   ├── server/
│   │   ├── grpc.go
│   │   └── http.go
│   ├── shard/
│   │   └── consistent_hash.go
│   └── observe/
│       └── metrics.go
├── proto/
│   └── ratelimit.proto
├── cmd/
│   ├── server/main.go
│   └── bench/main.go
└── .github/workflows/
    └── ci.yml
```

## Roadmap

| Phase | Focus | Target |
|---|---|---|
| 1 | Single-node token bucket + gRPC scaffold | Week 1 Sat |
| 2 | Redis backend with Lua atomicity, sliding window | Week 2 Sat |
| 3 | Consistent hashing for multi-node | Week 2 Tue/Thu |
| 4 | Prometheus metrics, Fly.io deploy, benchmarks | Week 2 Sat |

## Will be installable as

```bash
# Service binary (planned)
docker run kevinreber/bucketd:latest

# Go client library (planned)
go get github.com/kevinreber/bucketd/client
```

## License

MIT — see [LICENSE](LICENSE).
