package main

import (
	"os"
	"path/filepath"
	"regexp"
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

func TestWriteAll_MinerRolesAreNotSuperuser(t *testing.T) {
	keys, err := loadKeys(4)
	require.NoError(t, err)
	s := buildSpec(keys)

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, s))

	sqlBytes, err := os.ReadFile(filepath.Join(dir, "postgres", "init-multinode.sql"))
	require.NoError(t, err)
	sql := string(sqlBytes)

	// No bare SUPERUSER attribute anywhere - NOSUPERUSER embeds the substring,
	// so match on a word boundary before it.
	bareSuperuser := regexp.MustCompile(`(?:^|[^A-Z])SUPERUSER`)
	require.NotRegexp(t, bareSuperuser, sql, "init-multinode.sql grants SUPERUSER")

	// Every miner role must be provisioned with the NOSUPERUSER attribute clause.
	require.Equal(t, 4, strings.Count(sql, "NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;"),
		"expected one NOSUPERUSER attribute clause per miner role")
	for i := 1; i <= 4; i++ {
		require.Contains(t, sql, "CREATE ROLE miner"+strconv.Itoa(i)+" LOGIN")
	}
}

// TestProvisioningFilesAreNotSuperuser sweeps every hand-maintained Postgres
// provisioning file - not just the generated template covered above - since
// these are the files that actually drifted and triggered the original audit
// finding. A new provisioning file added later without being added to this
// list gets no coverage, but an existing one silently regressing to
// SUPERUSER will fail here.
func TestProvisioningFilesAreNotSuperuser(t *testing.T) {
	// Test binaries run in their own package dir; compose/cmd/gennodes -> repo root.
	repoRoot := filepath.Join("..", "..", "..")

	// (?i) + \b: "superuser" is a keyword and may be written in any case;
	// \b will not match inside "NOSUPERUSER" since O and S are both word chars.
	bareSuperuser := regexp.MustCompile(`(?i)\bsuperuser\b`)

	hardcoded := []string{
		"compose/postgres/init.sql",
		"test/postgres/init.sql",
		"deploy/docker/base/docker-services.yml",
		"deploy/kubernetes/postgres/postgres-configmap.yaml",
		"compose/cmd/gennodes/templates/init.sql.tmpl",
	}

	matches, err := filepath.Glob(filepath.Join(repoRoot, "scripts", "postgres", "init-*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "expected at least one scripts/postgres/init-*.sql file")

	files := make([]string, 0, len(hardcoded)+len(matches))
	files = append(files, hardcoded...)
	for _, m := range matches {
		rel, err := filepath.Rel(repoRoot, m)
		require.NoError(t, err)
		files = append(files, rel)
	}

	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(repoRoot, f))
		require.NoError(t, err, "%s", f)

		// Comments legitimately talk about "superuser" in prose (e.g. "needs no
		// superuser rights", "as the postgres superuser"); only the executable
		// content matters here, so strip comment lines before matching.
		code := stripComments(string(b), filepath.Ext(f))
		require.NotRegexp(t, bareSuperuser, code, "%s grants SUPERUSER", f)
	}
}

// stripComments drops full-line comments (SQL "--", YAML "#") so provisioning
// checks only see executable content, not prose that happens to mention the
// word being checked for.
func stripComments(content, ext string) string {
	var prefix string

	switch ext {
	case ".sql", ".tmpl":
		prefix = "--"
	case ".yml", ".yaml":
		prefix = "#"
	default:
		return content
	}

	lines := strings.Split(content, "\n")
	kept := make([]string, 0, len(lines))

	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			continue
		}

		kept = append(kept, line)
	}

	return strings.Join(kept, "\n")
}

// TestDevAndTestPostgresInitAreIdentical guards against compose/postgres/init.sql
// and test/postgres/init.sql drifting apart - they are hand-maintained copies of
// the same provisioning script and drift between them is exactly the class of bug
// that triggered the original audit finding.
func TestDevAndTestPostgresInitAreIdentical(t *testing.T) {
	repoRoot := filepath.Join("..", "..", "..")

	a, err := os.ReadFile(filepath.Join(repoRoot, "compose", "postgres", "init.sql"))
	require.NoError(t, err)
	b, err := os.ReadFile(filepath.Join(repoRoot, "test", "postgres", "init.sql"))
	require.NoError(t, err)

	require.Equal(t, string(a), string(b), "compose/postgres/init.sql and test/postgres/init.sql have drifted apart")
}
