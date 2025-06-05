package k8sresolver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseTarget(t *testing.T) {
	t.Run("should return error when target is empty", func(t *testing.T) {
		_, _, err := parseTarget("", defaultPort)
		require.Error(t, err)
	})

	t.Run("should return error when target ends with colon", func(t *testing.T) {
		_, _, err := parseTarget(":", defaultPort)
		require.Error(t, err)
	})

	t.Run("should return error when target is invalid", func(t *testing.T) {
		_, _, err := parseTarget("invalid:", defaultPort)
		require.Error(t, err)
	})

	t.Run("should return host and default port when target is ipv4", func(t *testing.T) {
		host, port, err := parseTarget("example.com:1888", defaultPort)
		require.NoError(t, err)
		require.Equal(t, "example.com", host)
		require.Equal(t, "1888", port)
	})

	t.Run("should return host and default port when target is ipv6", func(t *testing.T) {
		host, port, err := parseTarget("[::1]:1888", defaultPort)
		require.NoError(t, err)
		require.Equal(t, "::1", host)
		require.Equal(t, "1888", port)
	})

	t.Run("should return host and default port when target is ipv4", func(t *testing.T) {
		host, port, err := parseTarget("k8s://example.com:1888", defaultPort)
		require.NoError(t, err)
		require.Equal(t, "example.com", host)
		require.Equal(t, "1888", port)
	})

	t.Run("should return host and default port when target is ipv4", func(t *testing.T) {
		host, port, err := parseTarget("k8s://validation-service.validation-service.svc.cluster.local:8081", defaultPort)
		require.NoError(t, err)
		require.Equal(t, "validation-service.validation-service.svc.cluster.local", host)
		require.Equal(t, "8081", port)
	})
}
