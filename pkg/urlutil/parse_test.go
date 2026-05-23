package urlutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMultiHostURL_SingleHost(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1:9092/topic")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1:9092", u.Host)
	assert.Equal(t, "/topic", u.Path)
}

func TestParseMultiHostURL_SingleHostWithQuery(t *testing.T) {
	u, err := ParseMultiHostURL("aerospike://localhost:3000/namespace?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	assert.Equal(t, "aerospike", u.Scheme)
	assert.Equal(t, "localhost:3000", u.Host)
	assert.Equal(t, "/namespace", u.Path)
	assert.Equal(t, "100ms", u.Query().Get("Timeout"))
	assert.Equal(t, "100ms", u.Query().Get("LoginTimeout"))
}

func TestParseMultiHostURL_MultipleHosts(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1:9092,broker2:9092,broker3:9092/topic")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1:9092,broker2:9092,broker3:9092", u.Host)
	assert.Equal(t, "/topic", u.Path)

	hosts := strings.Split(u.Host, ",")
	assert.Len(t, hosts, 3)
	assert.Equal(t, "broker1:9092", hosts[0])
	assert.Equal(t, "broker2:9092", hosts[1])
	assert.Equal(t, "broker3:9092", hosts[2])
}

func TestParseMultiHostURL_MultipleHostsWithQuery(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1:9092,broker2:9093/topic?partitions=3&replication=2")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1:9092,broker2:9093", u.Host)
	assert.Equal(t, "/topic", u.Path)
	assert.Equal(t, "3", u.Query().Get("partitions"))
	assert.Equal(t, "2", u.Query().Get("replication"))

	hosts := strings.Split(u.Host, ",")
	assert.Len(t, hosts, 2)
	assert.Equal(t, "broker1:9092", hosts[0])
	assert.Equal(t, "broker2:9093", hosts[1])
}

func TestParseMultiHostURL_MultipleHostsWithUserInfo(t *testing.T) {
	u, err := ParseMultiHostURL("aerospike://user:pass@host1:3000,host2:3001/namespace")
	require.NoError(t, err)

	assert.Equal(t, "aerospike", u.Scheme)
	assert.Equal(t, "host1:3000,host2:3001", u.Host)
	assert.Equal(t, "/namespace", u.Path)
	assert.Equal(t, "user", u.User.Username())

	pass, ok := u.User.Password()
	assert.True(t, ok)
	assert.Equal(t, "pass", pass)
}

func TestParseMultiHostURL_MultipleHostsNoPath(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1:9092,broker2:9092")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1:9092,broker2:9092", u.Host)
	assert.Equal(t, "", u.Path)
}

func TestParseMultiHostURL_MultipleHostsQueryNoPath(t *testing.T) {
	u, err := ParseMultiHostURL("aerospike://host1:3000,host2:3001?Timeout=100ms")
	require.NoError(t, err)

	assert.Equal(t, "aerospike", u.Scheme)
	assert.Equal(t, "host1:3000,host2:3001", u.Host)
	assert.Equal(t, "", u.Path)
	assert.Equal(t, "100ms", u.Query().Get("Timeout"))
}

func TestParseMultiHostURL_NoScheme(t *testing.T) {
	u, err := ParseMultiHostURL("/just/a/path")
	require.NoError(t, err)

	assert.Equal(t, "", u.Scheme)
	assert.Equal(t, "/just/a/path", u.Path)
}

func TestParseMultiHostURL_EmptyString(t *testing.T) {
	u, err := ParseMultiHostURL("")
	require.NoError(t, err)

	assert.Equal(t, "", u.String())
}

func TestParseMultiHostURL_MemoryScheme(t *testing.T) {
	u, err := ParseMultiHostURL("memory://broker1:9092,broker2:9092,broker3:9092/test-topic")
	require.NoError(t, err)

	assert.Equal(t, "memory", u.Scheme)
	assert.Equal(t, "broker1:9092,broker2:9092,broker3:9092", u.Host)
	assert.Equal(t, "/test-topic", u.Path)
}

func TestParseMultiHostURL_AerospikeMultiHostWithQuery(t *testing.T) {
	u, err := ParseMultiHostURL("aerospike://host1:3000,host2:3001/namespace?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	assert.Equal(t, "aerospike", u.Scheme)
	assert.Equal(t, "host1:3000,host2:3001", u.Host)
	assert.Equal(t, "/namespace", u.Path)
	assert.Equal(t, "100ms", u.Query().Get("Timeout"))
	assert.Equal(t, "100ms", u.Query().Get("LoginTimeout"))

	hosts := strings.Split(u.Host, ",")
	assert.Len(t, hosts, 2)
	assert.Equal(t, "host1:3000", hosts[0])
	assert.Equal(t, "host2:3001", hosts[1])
}

func TestParseMultiHostURL_InvalidURL(t *testing.T) {
	_, err := ParseMultiHostURL("://missing-scheme")
	assert.Error(t, err)
}

func TestParseMultiHostURL_SingleHostNoPort(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1/topic")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1", u.Host)
	assert.Equal(t, "/topic", u.Path)
}

func TestParseMultiHostURL_MultipleHostsMixedPorts(t *testing.T) {
	u, err := ParseMultiHostURL("kafka://broker1:9092,broker2/topic")
	require.NoError(t, err)

	assert.Equal(t, "kafka", u.Scheme)
	assert.Equal(t, "broker1:9092,broker2", u.Host)
	assert.Equal(t, "/topic", u.Path)

	hosts := strings.Split(u.Host, ",")
	assert.Len(t, hosts, 2)
	assert.Equal(t, "broker1:9092", hosts[0])
	assert.Equal(t, "broker2", hosts[1])
}
