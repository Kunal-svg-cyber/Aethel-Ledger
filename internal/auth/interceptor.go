package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// HeaderKey is the gRPC metadata key carrying the API key.
const HeaderKey = "x-api-key"

// UnaryServerInterceptor rejects calls that do not present the expected
// API key. An empty expectedKey disables the check.
func UnaryServerInterceptor(expectedKey string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if expectedKey == "" {
			return handler(ctx, req)
		}
		var presented []string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			presented = md.Get(HeaderKey)
		}
		if err := CheckKey(expectedKey, presented); err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		return handler(ctx, req)
	}
}
