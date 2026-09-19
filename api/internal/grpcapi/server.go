// Package grpcapi exposes the live metrics stream over gRPC (Phase 1). It
// mirrors the SSE feed: connect and receive MetricsUpdate frames, starting
// with the latest published state.
package grpcapi

import (
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metricsv1 "abi/internal/grpcapi/metricsv1"
	"abi/internal/model"
	"abi/internal/realtime"
)

// Server implements metricsv1.MetricsServiceServer.
type Server struct {
	metricsv1.UnimplementedMetricsServiceServer
	live *realtime.LiveFeed
	log  *slog.Logger
}

// New builds the gRPC service around a live feed.
func New(live *realtime.LiveFeed, log *slog.Logger) *Server {
	return &Server{live: live, log: log}
}

// StreamMetrics pushes MetricsUpdate frames until the client disconnects or
// the server shuts down.
func (s *Server) StreamMetrics(_ *metricsv1.StreamMetricsRequest,
	stream metricsv1.MetricsService_StreamMetricsServer) error {

	sub, unsub := s.live.Subscribe()
	defer unsub()

	// Snapshot first: whatever the broadcaster last published.
	if last := s.live.Last(); last.Current.BucketStart != "" {
		if err := stream.Send(toProto(last)); err != nil {
			return status.Error(codes.Canceled, "client disconnected")
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case u, ok := <-sub:
			if !ok {
				return nil
			}
			if err := stream.Send(toProto(u)); err != nil {
				return status.Error(codes.Canceled, "client disconnected")
			}
		}
	}
}

func toProto(u model.MetricsUpdate) *metricsv1.MetricsUpdate {
	out := &metricsv1.MetricsUpdate{
		Current:         toBucketProto(u.Current),
		SpeedMultiplier: u.SpeedMultiplier,
		Status:          u.Status,
	}
	for _, b := range u.Snapshot {
		out.Snapshot = append(out.Snapshot, toBucketProto(b))
	}
	if u.Anomaly != nil {
		out.Anomaly = toAnomalyProto(*u.Anomaly)
	}
	return out
}

func toBucketProto(b model.RealtimeBucket) *metricsv1.RealtimeBucket {
	return &metricsv1.RealtimeBucket{
		BucketStart:    b.BucketStart,
		Revenue:        b.Revenue,
		Orders:         b.Orders,
		ActiveSessions: b.ActiveSessions,
		UpdatedAt:      b.UpdatedAt,
		AnomalyFlag:    b.AnomalyFlag,
	}
}

func toAnomalyProto(a model.Anomaly) *metricsv1.Anomaly {
	return &metricsv1.Anomaly{
		Id:          a.ID,
		Metric:      a.Metric,
		BucketStart: a.BucketStart,
		Observed:    a.Observed,
		Expected:    a.Expected,
		ZScore:      a.ZScore,
		Severity:    a.Severity,
		Status:      a.Status,
		DetectedAt:  a.DetectedAt,
	}
}
