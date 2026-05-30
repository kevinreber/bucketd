package server

import (
	"context"
	"errors"

	"github.com/kevinreber/bucketd/internal/algorithms"
	"github.com/kevinreber/bucketd/internal/backend"
	ratelimitpb "github.com/kevinreber/bucketd/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapBackendError translates a backend error into a gRPC status. Backend
// errors fall into three buckets:
//   - Caller misuse (invalid tokens / config / empty key) → InvalidArgument
//   - Context expiration / cancellation → matching gRPC codes
//   - Everything else (Redis network failures, Lua errors) → Internal
//
// Distinguishing these lets the LLM Gateway retry on Unavailable but fail
// fast on InvalidArgument.
func mapBackendError(err error) error {
	switch {
	case errors.Is(err, algorithms.ErrInvalidTokens),
		errors.Is(err, algorithms.ErrInvalidConfig),
		errors.Is(err, backend.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Errorf(codes.Internal, "backend allow failed: %v", err)
	}
}

// Server implements the bucketd.v1.RateLimiter gRPC service.
//
// Stateless aside from its Backend reference. The Backend owns all bucket
// state and is responsible for concurrency safety.
type Server struct {
	ratelimitpb.UnimplementedRateLimiterServer
	Backend Backend
}

// Backend is the storage abstraction the gRPC layer talks to. Both Memory
// (Phase 1) and Redis (Phase 2) satisfy it.
type Backend interface {
	Allow(
		ctx context.Context,
		key string,
		tokens int,
		capacity int32,
		refillRate float64,
	) (algorithms.Verdict, error)
}

// NewServer constructs a Server wired to the given backend.
func NewServer(b Backend) *Server {
	return &Server{Backend: b}
}

// Allow translates a proto AllowRequest into a Backend.Allow call and shapes
// the response. Input validation is delegated to the backend / algorithm; this
// layer only converts error codes.
func (s *Server) Allow(
	ctx context.Context,
	req *ratelimitpb.AllowRequest,
) (*ratelimitpb.AllowResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	v, err := s.Backend.Allow(
		ctx,
		req.GetKey(),
		int(req.GetTokens()),
		req.GetCapacity(),
		req.GetRefillRate(),
	)
	if err != nil {
		return nil, mapBackendError(err)
	}

	return &ratelimitpb.AllowResponse{
		Allowed:      v.Allowed,
		Remaining:    int32(v.Remaining),
		RetryAfterMs: int32(v.RetryAfterMs),
	}, nil
}

// Compile-time check that Memory satisfies the Backend interface.
var _ Backend = (*backend.Memory)(nil)
