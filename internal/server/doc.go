// Package server hosts the gRPC and HTTP entry points for bucketd.
//
//   - grpc.go — gRPC service implementation (primary)
//   - http.go — HTTP/JSON wrapper for non-gRPC clients
//
// Scaffolding stub — implementations land in Phase 1 (gRPC) and
// Phase 4 (HTTP wrapper).
package server
