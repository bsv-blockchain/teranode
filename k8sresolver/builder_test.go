package k8sresolver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNamespace(t *testing.T) {
	ns, host := getNamespaceFromHost("test.notdefault")
	assert.Equal(t, "notdefault", ns)
	assert.Equal(t, "test", host)

	ns, host = getNamespaceFromHost("test")
	assert.Equal(t, "default", ns)
	assert.Equal(t, "test", host)

	ns, host = getNamespaceFromHost("test.maybedefault.cluster")
	assert.Equal(t, "maybedefault", ns)
	assert.Equal(t, "test", host)
}
