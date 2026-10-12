package util

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type authTestStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s authTestStream) Context() context.Context { return s.ctx }

func TestRequiredAuthUnknownMethods(t *testing.T) {
	const key = "required-auth-interceptor-test-key"
	unary, stream := requiredAuthInterceptors(key, map[string]bool{"/example/Health": true})
	for _, method := range []string{"/example/NewMutation", "/new.Service/NewStream", "/example/Health"} {
		for _, auth := range []bool{false, true} {
			ctx := context.Background()
			if auth {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-api-key", key))
			}
			expected := method == "/example/Health" || auth
			called := false
			_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
				called = true
				if method != "/example/Health" {
					require.Equal(t, true, ctx.Value(authenticatedKey))
				}
				return nil, nil
			})
			require.Equal(t, expected, called)
			if expected {
				require.NoError(t, err)
			} else {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
			}
			called = false
			err = stream(nil, authTestStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, func(_ any, ss grpc.ServerStream) error {
				called = true
				if method != "/example/Health" {
					require.Equal(t, true, ss.Context().Value(authenticatedKey))
				}
				return nil
			})
			require.Equal(t, expected, called)
			if expected {
				require.NoError(t, err)
			} else {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
			}
		}
	}
}

func TestRequiredAuthRejectsInvalidOptionsBeforeListening(t *testing.T) {
	for _, opts := range []*AuthOptions{
		{RequireAuthByDefault: true},
		{RequireAuthByDefault: true, APIKey: "placeholder"},
		{RequireAuthByDefault: true, APIKey: "valid-service-test-key", ProtectedMethods: map[string]bool{"/example/Mutation": true}},
	} {
		called := false
		err := StartGRPCServer(context.Background(), ulogger.TestLogger{}, &settings.Settings{}, "invalid-auth", "bad:address", func(*grpc.Server) { called = true }, opts)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "listen")
		require.False(t, called)
	}
}

func TestValidateRequiredAdminAPIKey(t *testing.T) {
	for _, key := range []string{"", "\t\n", "testkey", "DEFAULT", "123456789012345", " space-padded-key "} {
		require.Error(t, ValidateRequiredAdminAPIKey(key))
	}
	for _, key := range []string{"1234567890123456", "a-random-service-test-key-with-32-chars"} {
		require.NoError(t, ValidateRequiredAdminAPIKey(key))
	}
}
