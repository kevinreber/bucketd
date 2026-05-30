// Package server hosts the gRPC and HTTP entry points for bucketd.
//
//   - grpc.go — gRPC service implementation (Phase 1). Talks to a Backend
//     interface so the actual storage (memory or Redis) is pluggable.
//   - http.go — HTTP/JSON wrapper for non-gRPC clients. Lands in Phase 4.
package server
