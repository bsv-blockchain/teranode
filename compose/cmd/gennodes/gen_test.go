package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBuildSpec_MeshAndPorts(t *testing.T) {
	keys, err := loadKeys(4)
	require.NoError(t, err)
	s := buildSpec(keys)

	require.Equal(t, 4, s.NodeCount)
	require.Len(t, s.Nodes, 4)

	seenPeerIDs := map[string]bool{}
	seenAerospikePorts := map[int]bool{}
	seenHostPorts := map[int]bool{}

	for i, n := range s.Nodes {
		require.Equal(t, i+1, n.Index)
		require.NotEmpty(t, n.PeerID)
		require.NotEmpty(t, n.PrivateKey)
		require.False(t, seenPeerIDs[n.PeerID], "duplicate peer id at node %d", n.Index)
		seenPeerIDs[n.PeerID] = true

		require.False(t, seenAerospikePorts[n.AerospikeServicePort], "duplicate aerospike port at node %d", n.Index)
		seenAerospikePorts[n.AerospikeServicePort] = true

		// Static peers must reference exactly N-1 others, never self.
		peerLines := strings.Split(n.StaticPeers, " | ")
		require.Len(t, peerLines, 3, "node %d should have N-1 peers", n.Index)
		for _, line := range peerLines {
			require.NotContains(t, line, "/dns/teranode"+strconv.Itoa(n.Index)+"/", "node %d listed itself", n.Index)
		}

		// Host ports must not collide across any node × container-port pair.
		for _, hp := range n.HostPorts {
			require.False(t, seenHostPorts[hp.Host], "host port %d collides", hp.Host)
			seenHostPorts[hp.Host] = true
			require.Less(t, hp.Host, 65536, "host port %d out of range", hp.Host)
		}
	}
}

func TestWriteAll_RendersCompleteBundle(t *testing.T) {
	keys, err := loadKeys(4)
	require.NoError(t, err)
	s := buildSpec(keys)

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, s))

	for _, name := range []string{
		"docker-compose-multinode.yml",
		"settings_multinode.conf",
		"postgres/init-multinode.sql",
	} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "expected %s to exist", name)
	}
	for i := 1; i <= 4; i++ {
		_, err := os.Stat(filepath.Join(dir, "aerospike", "aerospike-"+strconv.Itoa(i)+".conf"))
		require.NoError(t, err, "expected aerospike-%d.conf to exist", i)
	}

	composeBytes, err := os.ReadFile(filepath.Join(dir, "docker-compose-multinode.yml"))
	require.NoError(t, err)

	// Compose YAML must parse cleanly and contain the expected service set.
	var doc struct {
		Services map[string]any `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(composeBytes, &doc))
	for _, want := range []string{
		"teranode-builder", "postgres", "kafka-shared", "jaeger",
		"teranode1", "teranode2", "teranode3", "teranode4",
		"aerospike-1", "aerospike-2", "aerospike-3", "aerospike-4",
	} {
		_, ok := doc.Services[want]
		require.True(t, ok, "compose missing service %q", want)
	}
	require.Len(t, doc.Services, 1+3+4+4, "unexpected service count")
}
