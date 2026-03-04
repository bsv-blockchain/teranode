package banlist

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CreateGRPCUnaryInterceptor returns a gRPC unary server interceptor that
// rejects requests from banned IPs with codes.PermissionDenied.
func CreateGRPCUnaryInterceptor(banList Interface) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			if banList.IsBanned(p.Addr.String()) {
				return nil, status.Error(codes.PermissionDenied, "banned")
			}
		}

		return handler(ctx, req)
	}
}
