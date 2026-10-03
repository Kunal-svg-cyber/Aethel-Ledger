package tracing

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor generates a new trace ID for every incoming
// RPC (or reuses one already present on the context) and attaches it
// before calling the handler, so every log line emitted while handling
// this call can be correlated back to the same request.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		id := FromContext(ctx)
		if id == "" {
			id = NewID()
		}
		return handler(WithID(ctx, id), req)
	}
}
