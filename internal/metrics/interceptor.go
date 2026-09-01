package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor times every unary RPC and records it, without
// any individual handler in internal/server needing to know metrics
// exist. Wire it in with grpc.NewServer(grpc.UnaryInterceptor(...)).
func (r *Recorder) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		r.Record(info.FullMethod, time.Since(start), err != nil)
		return resp, err
	}
}
