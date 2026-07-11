// Package grpcingest is Tally's second front door: a gRPC service for
// backend SDKs that batch many events into one call. It feeds the exact same
// queue as the HTTP API, with the same rate limiting, validation,
// backpressure, and idempotency semantics — two doors, one pipeline.
package grpcingest

import (
	"context"
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	tallypb "github.com/shreyas463/tally/gen/tally/v1"
	"github.com/shreyas463/tally/internal/ingest"
	"github.com/shreyas463/tally/internal/metrics"
	"github.com/shreyas463/tally/internal/queue"
	"github.com/shreyas463/tally/internal/ratelimit"
	"github.com/shreyas463/tally/internal/store"
)

// Server implements tally.v1.TallyService.
type Server struct {
	tallypb.UnimplementedTallyServiceServer
	queue   ingest.Enqueuer
	limiter ratelimit.Limiter // nil = disabled
}

// New builds the service. Pass a nil limiter to disable rate limiting.
func New(q ingest.Enqueuer, l ratelimit.Limiter) *Server {
	return &Server{queue: q, limiter: l}
}

// Register attaches the service to a gRPC server.
func (s *Server) Register(gs *grpc.Server) {
	tallypb.RegisterTallyServiceServer(gs, s)
}

// Publish enqueues a batch. Validation problems reject individual events
// (reported per index); a full queue fails the WHOLE call with
// RESOURCE_EXHAUSTED so the client backs off and retries the batch — safe
// because storage dedupes by event_id, so re-sending already-accepted events
// cannot double-count.
func (s *Server) Publish(ctx context.Context, req *tallypb.PublishRequest) (*tallypb.PublishResponse, error) {
	if s.limiter != nil && !s.limiter.Allow(clientKey(ctx)) {
		metrics.EventsRejected.WithLabelValues("rate_limited").Inc()
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded, retry later")
	}

	resp := &tallypb.PublishResponse{}
	now := time.Now().UTC()

	for i, pe := range req.GetEvents() {
		reason := ""
		switch {
		case pe.GetEventId() == "":
			reason = "event_id is required"
		case pe.GetName() == "":
			reason = "name is required"
		}
		if reason != "" {
			metrics.EventsRejected.WithLabelValues("invalid").Inc()
			resp.Rejected = append(resp.Rejected, &tallypb.RejectedEvent{Index: int32(i), Reason: reason})
			continue
		}

		props := make(map[string]any, len(pe.GetProperties()))
		for k, v := range pe.GetProperties() {
			props[k] = v
		}

		err := s.queue.Enqueue(store.Event{
			EventID:    pe.GetEventId(),
			Name:       pe.GetName(),
			DistinctID: pe.GetDistinctId(),
			Properties: props,
			TS:         now,
		})
		switch {
		case err == nil:
			metrics.EventsAccepted.Inc()
			resp.Accepted++
		case errors.Is(err, queue.ErrFull):
			metrics.EventsRejected.WithLabelValues("queue_full").Inc()
			return nil, status.Errorf(codes.ResourceExhausted,
				"queue full after accepting %d events; retry the batch (storage dedupes by event_id)", resp.Accepted)
		case errors.Is(err, queue.ErrClosed):
			metrics.EventsRejected.WithLabelValues("shutdown").Inc()
			return nil, status.Error(codes.Unavailable, "shutting down")
		case errors.Is(err, queue.ErrUnavailable):
			metrics.EventsRejected.WithLabelValues("not_ingest").Inc()
			return nil, status.Error(codes.Unavailable, "this instance does not accept events (worker mode)")
		default:
			return nil, status.Error(codes.Internal, "failed to accept event")
		}
	}
	return resp, nil
}

// clientKey mirrors the HTTP rate-limit identity: x-api-key metadata if
// present, otherwise the peer IP.
func clientKey(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if keys := md.Get("x-api-key"); len(keys) > 0 && keys[0] != "" {
			return keys[0]
		}
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
			return host
		}
		return p.Addr.String()
	}
	return "unknown"
}
