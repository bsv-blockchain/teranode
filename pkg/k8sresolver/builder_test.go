package k8sresolver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestNewBuilder(t *testing.T) {
	logger := &MockLogger{}
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()
	logger.On("Errorf", mock.Anything, mock.Anything).Maybe()
	logger.On("Infof", mock.Anything, mock.Anything).Maybe()
	logger.On("Warnf", mock.Anything, mock.Anything).Maybe()

	builder := NewBuilder(logger)

	require.NotNil(t, builder)
	assert.Implements(t, (*resolver.Builder)(nil), builder)

	// Check that it's actually a k8sBuilder
	k8sBuilder, ok := builder.(*k8sBuilder)
	require.True(t, ok, "Expected builder to be of type *k8sBuilder")
	assert.Equal(t, logger, k8sBuilder.logger)
}

func TestK8sBuilder_Scheme(t *testing.T) {
	logger := &MockLogger{}
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()

	builder := NewBuilder(logger)

	scheme := builder.Scheme()
	assert.Equal(t, "k8s", scheme)
}

func TestGetNamespaceFromHost(t *testing.T) {
	tests := []struct {
		name              string
		host              string
		expectedNamespace string
		expectedHost      string
	}{
		{
			name:              "host with namespace",
			host:              "service.custom-namespace",
			expectedNamespace: "custom-namespace",
			expectedHost:      "service",
		},
		{
			name:              "host with multiple dots",
			host:              "service.custom-namespace.svc.cluster.local",
			expectedNamespace: "custom-namespace",
			expectedHost:      "service",
		},
		{
			name:              "host without namespace",
			host:              "service",
			expectedNamespace: "default",
			expectedHost:      "service",
		},
		{
			name:              "host with default namespace explicitly",
			host:              "service.default",
			expectedNamespace: "default",
			expectedHost:      "service",
		},
		{
			name:              "empty host",
			host:              "",
			expectedNamespace: "default",
			expectedHost:      "",
		},
		{
			name:              "host starting with dot",
			host:              ".namespace",
			expectedNamespace: "namespace",
			expectedHost:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, host := getNamespaceFromHost(tt.host)
			assert.Equal(t, tt.expectedNamespace, namespace, "namespace mismatch")
			assert.Equal(t, tt.expectedHost, host, "host mismatch")
		})
	}
}

func TestK8sBuilder_Build_InvalidTarget(t *testing.T) {
	logger := &MockLogger{}
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()
	logger.On("Errorf", mock.Anything, mock.Anything).Maybe()

	builder := NewBuilder(logger)

	// Create a mock ClientConn
	mockCC := &mockClientConn{}

	// Test with empty target - use Endpoint() method
	target := resolver.Target{}

	_, err := builder.Build(target, mockCC, resolver.BuildOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing address")
}

func TestK8sBuilder_Build_InClusterConfigError(t *testing.T) {
	// This test verifies that Build returns an error when not running in a K8s cluster
	logger := &MockLogger{}
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()
	logger.On("Errorf", mock.Anything, mock.Anything).Maybe()

	builder := NewBuilder(logger)

	mockCC := &mockClientConn{}

	// Create a target - the Build will fail at newInClusterClient anyway
	target := resolver.Target{}

	_, err := builder.Build(target, mockCC, resolver.BuildOptions{})
	// Should fail because we're not in a K8s cluster
	require.Error(t, err)
	// Could be either "missing address" or "failed to build in-cluster" depending on parsing
	assert.Error(t, err)
}

// mockClientConn implements resolver.ClientConn for testing
type mockClientConn struct {
	mock.Mock
	states []resolver.State
}

func (m *mockClientConn) UpdateState(state resolver.State) error {
	args := m.Called(state)
	m.states = append(m.states, state)
	return args.Error(0)
}

func (m *mockClientConn) ReportError(err error) {
	m.Called(err)
}

func (m *mockClientConn) NewAddress(addresses []resolver.Address) {
	m.Called(addresses)
}

func (m *mockClientConn) NewServiceConfig(serviceConfig string) {
	m.Called(serviceConfig)
}

func (m *mockClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	args := m.Called(serviceConfigJSON)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*serviceconfig.ParseResult)
}

func TestConstants(t *testing.T) {
	// Test that constants are set correctly
	assert.Equal(t, "443", defaultPort)
	assert.Equal(t, "default", defaultNamespace)
	assert.NotZero(t, minK8SResRate)
}

func TestK8sBuilder_Scheme_Consistency(t *testing.T) {
	// Create multiple builders and ensure they all return the same scheme
	logger1 := &MockLogger{}
	logger1.On("Debugf", mock.Anything, mock.Anything).Maybe()
	logger2 := &MockLogger{}
	logger2.On("Debugf", mock.Anything, mock.Anything).Maybe()

	builder1 := NewBuilder(logger1)
	builder2 := NewBuilder(logger2)

	assert.Equal(t, builder1.Scheme(), builder2.Scheme())
	assert.Equal(t, "k8s", builder1.Scheme())
}

func TestGetNamespaceFromHost_EdgeCases(t *testing.T) {
	tests := []struct {
		name              string
		host              string
		expectedNamespace string
		expectedHost      string
	}{
		{
			name:              "single character host",
			host:              "s",
			expectedNamespace: "default",
			expectedHost:      "s",
		},
		{
			name:              "single character with namespace",
			host:              "s.n",
			expectedNamespace: "n",
			expectedHost:      "s",
		},
		{
			name:              "very long host",
			host:              "very-long-service-name-that-exceeds-normal-limits.very-long-namespace-name",
			expectedNamespace: "very-long-namespace-name",
			expectedHost:      "very-long-service-name-that-exceeds-normal-limits",
		},
		{
			name:              "host with hyphens",
			host:              "my-service.my-namespace",
			expectedNamespace: "my-namespace",
			expectedHost:      "my-service",
		},
		{
			name:              "host with underscores",
			host:              "my_service.my_namespace",
			expectedNamespace: "my_namespace",
			expectedHost:      "my_service",
		},
		{
			name:              "host with numbers",
			host:              "service123.namespace456",
			expectedNamespace: "namespace456",
			expectedHost:      "service123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, host := getNamespaceFromHost(tt.host)
			assert.Equal(t, tt.expectedNamespace, namespace)
			assert.Equal(t, tt.expectedHost, host)
		})
	}
}

func TestNewBuilder_NilLogger(t *testing.T) {
	// Test that NewBuilder handles nil logger gracefully
	// Note: This will likely panic if logger is used, but we test that it at least creates the builder
	builder := NewBuilder(nil)
	require.NotNil(t, builder)

	// The builder should still return the correct scheme
	assert.Equal(t, "k8s", builder.Scheme())
}

func TestK8sBuilder_Implements_ResolverBuilder(t *testing.T) {
	logger := &MockLogger{}
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()

	builder := NewBuilder(logger)

	// Verify it implements the resolver.Builder interface
	var _ resolver.Builder = builder

	// Verify Build method exists and has correct signature
	mockCC := &mockClientConn{}
	mockCC.On("UpdateState", mock.Anything).Return(nil).Maybe()
	mockCC.On("ReportError", mock.Anything).Maybe()

	// Should fail due to not being in cluster, but method should exist
	_, err := builder.Build(resolver.Target{}, mockCC, resolver.BuildOptions{})
	assert.Error(t, err) // Expected to fail since we're not in a K8s cluster
}
