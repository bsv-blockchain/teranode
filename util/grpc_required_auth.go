package util

import (
	"context"
	"crypto/subtle"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ValidateRequiredAdminAPIKey rejects unusable service credentials without disclosing them.
// Call this before starting listeners or workers for services whose data path requires auth.
func ValidateRequiredAdminAPIKey(key string) error {
	if key == "" || key != strings.TrimSpace(key) || IsPlaceholderAdminAPIKey(key) || IsWeakAdminAPIKey(key) {
		return errors.NewConfigurationError("grpc_admin_api_key is required: supply a non-placeholder shared secret of at least %d characters (32+ random characters recommended) to the blockchain service and all its clients", MinAdminAPIKeyLength())
	}
	return nil
}

// requiredAuthInterceptors use an exception list: unknown methods require auth too.
func requiredAuthInterceptors(key string, public map[string]bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	authenticate := func(ctx context.Context, method string) (context.Context, error) {
		if public[method] {
			return ctx, nil
		}
		md, _ := metadata.FromIncomingContext(ctx)
		keys := md.Get(apiKeyHeader)
		if key == "" || len(keys) != 1 || subtle.ConstantTimeCompare([]byte(keys[0]), []byte(key)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "a valid x-api-key is required")
		}
		return context.WithValue(ctx, authenticatedKey, true), nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &authenticatedServerStream{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

type authenticatedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedServerStream) Context() context.Context { return s.ctx }
