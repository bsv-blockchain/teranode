package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fixtureGraph is a miniature stand-in for the real module: a few service
// packages with a dependency chain, plus the in-scope test suites.
//
//	blockchain  <- validator <- blockassembly
//	p2p (independent)
//	test/e2e/daemon/ready imports the daemon stack (blockchain, validator)
//	test/sequentialtest/double_spend imports blockchain
//	test/sequentialtest/longest_chain imports p2p
func fixtureGraph() []Pkg {
	const m = modulePath
	return []Pkg{
		{ImportPath: m, Dir: "."},
		{ImportPath: m + "/services/blockchain", Dir: "services/blockchain"},
		{ImportPath: m + "/services/validator", Dir: "services/validator", Deps: []string{m + "/services/blockchain"}},
		{ImportPath: m + "/services/blockassembly", Dir: "services/blockassembly", Deps: []string{m + "/services/validator", m + "/services/blockchain"}},
		{ImportPath: m + "/services/p2p", Dir: "services/p2p"},
		{ImportPath: m + "/test/e2e/daemon/ready", Dir: "test/e2e/daemon/ready", Deps: []string{m + "/services/blockchain", m + "/services/validator"}},
		{ImportPath: m + "/test/e2e/chainintegrity", Dir: "test/e2e/chainintegrity", Deps: []string{m + "/services/blockchain"}},
		{ImportPath: m + "/test/sequentialtest/double_spend", Dir: "test/sequentialtest/double_spend", Deps: []string{m + "/services/blockchain"}},
		{ImportPath: m + "/test/sequentialtest/longest_chain", Dir: "test/sequentialtest/longest_chain", Deps: []string{m + "/services/p2p"}},
		{ImportPath: m + "/test/tna", Dir: "test/tna", Deps: []string{m + "/services/blockchain"}}, // out-of-scope suite
	}
}

func TestCompute(t *testing.T) {
	const m = modulePath

	t.Run("global input forces full", func(t *testing.T) {
		for _, f := range []string{"go.mod", "go.sum", "Makefile", ".github/workflows/x.yaml", "settings/dev.conf", "settings.conf", ".golangci.yml", ".golangci.json", "test/scripts/run_tests_sequentially.sh", "test/scripts/affected/main.go", "Dockerfile", "test/utils/explorer/Dockerfile", "compose/docker-compose-chainintegrity.yml", "test/docker-compose.e2etest.yml"} {
			res := compute([]string{f}, fixtureGraph())
			require.True(t, res.Full, "expected full for %s", f)
		}
	})

	t.Run("any_go reflects whether go changed", func(t *testing.T) {
		require.True(t, compute([]string{"services/p2p/server.go"}, fixtureGraph()).AnyGo)
		require.False(t, compute([]string{"docs/foo.md"}, fixtureGraph()).AnyGo)
		require.True(t, compute([]string{"go.mod"}, fixtureGraph()).AnyGo, "full implies any_go")
	})

	t.Run("docs only runs nothing", func(t *testing.T) {
		res := compute([]string{"docs/foo.md", "README.md"}, fixtureGraph())
		require.False(t, res.Full)
		require.False(t, res.RunUnit)
		require.False(t, res.RunSmoke)
		require.False(t, res.RunChainIntegrity)
		require.False(t, res.RunSeq)
	})

	t.Run("blockchain change pulls in importers and e2e", func(t *testing.T) {
		res := compute([]string{"services/blockchain/server.go"}, fixtureGraph())
		require.False(t, res.Full)
		require.True(t, res.RunUnit)
		require.ElementsMatch(t, []string{
			m + "/services/blockchain",
			m + "/services/validator",
			m + "/services/blockassembly",
		}, res.UnitPkgs)
		require.NotContains(t, res.UnitPkgs, m+"/services/p2p", "p2p does not import blockchain")
		require.True(t, res.RunSmoke, "e2e/daemon/ready imports blockchain")
		require.True(t, res.RunChainIntegrity)
		require.True(t, res.RunSeq)
		require.Equal(t, []string{"test/sequentialtest/double_spend"}, res.SeqPkgs, "longest_chain depends on p2p, not blockchain")
	})

	t.Run("leaf change scopes tightly", func(t *testing.T) {
		res := compute([]string{"services/p2p/server.go"}, fixtureGraph())
		require.Equal(t, []string{m + "/services/p2p"}, res.UnitPkgs)
		require.False(t, res.RunSmoke, "ready does not import p2p in the fixture")
		require.False(t, res.RunChainIntegrity)
		require.True(t, res.RunSeq)
		require.Equal(t, []string{"test/sequentialtest/longest_chain"}, res.SeqPkgs)
	})

	t.Run("test-only change in test/ tree runs just that test", func(t *testing.T) {
		res := compute([]string{"test/sequentialtest/double_spend/foo_test.go"}, fixtureGraph())
		require.False(t, res.Full)
		require.False(t, res.RunUnit, "no app package affected")
		require.False(t, res.RunSmoke)
		require.True(t, res.RunSeq)
		require.Equal(t, []string{"test/sequentialtest/double_spend"}, res.SeqPkgs)
	})

	t.Run("embedded asset maps to its package", func(t *testing.T) {
		res := compute([]string{"services/blockchain/testdata/block.json"}, fixtureGraph())
		require.Contains(t, res.UnitPkgs, m+"/services/blockchain")
	})

	t.Run("unknown .go file forces full", func(t *testing.T) {
		res := compute([]string{"some/unlisted/pkg/file.go"}, fixtureGraph())
		require.True(t, res.Full)
	})

	t.Run("test-only package surfaced via TestImports (no helper .go)", func(t *testing.T) {
		// A sequential package whose dependency on blockchain exists ONLY through
		// its _test.go files (empty Deps, blockchain in TestImports). The closure
		// must still flag it when blockchain changes.
		g := append(fixtureGraph(), Pkg{
			ImportPath:  m + "/test/sequentialtest/testonly",
			Dir:         "test/sequentialtest/testonly",
			TestImports: []string{m + "/services/blockchain"},
		})
		res := compute([]string{"services/blockchain/server.go"}, g)
		require.Contains(t, res.SeqPkgs, "test/sequentialtest/testonly")
	})

	t.Run("test import expanded transitively", func(t *testing.T) {
		// XTestImports points at validator, which transitively deps blockchain.
		g := append(fixtureGraph(), Pkg{
			ImportPath:   m + "/test/sequentialtest/xt",
			Dir:          "test/sequentialtest/xt",
			XTestImports: []string{m + "/services/validator"},
		})
		res := compute([]string{"services/blockchain/server.go"}, g)
		require.Contains(t, res.SeqPkgs, "test/sequentialtest/xt")
	})

	t.Run("out-of-scope suite is not surfaced", func(t *testing.T) {
		res := compute([]string{"services/blockchain/server.go"}, fixtureGraph())
		require.NotContains(t, res.SeqPkgs, "test/tna")
		require.NotContains(t, res.UnitPkgs, m+"/test/tna")
	})
}

func TestIsGlobalInput(t *testing.T) {
	yes := []string{"go.mod", "go.sum", "Makefile", ".github/workflows/ci.yaml", ".github/actions/x/action.yml", "settings/dev.conf", "settings.conf", "settings_local.conf", ".golangci.yml", ".golangci.yaml", ".golangci.json", ".golangci.toml", "test/scripts/run_tests_sequentially.sh", "test/scripts/affected/main.go",
		"Dockerfile", "test/utils/cmd/tstore/Dockerfile", "compose/docker-compose-blasters.yml", "compose/docker-compose-chainintegrity.yml", "test/docker-compose-host.yml", "test/docker-compose.e2etest.yml"}
	for _, f := range yes {
		require.True(t, isGlobalInput(f), f)
	}
	no := []string{"services/blockchain/server.go", "docs/x.md", "README.md", "test/sequentialtest/x_test.go", "services/blockchain/config.json", "test/e2e/daemon/ready/ready_test.go"}
	for _, f := range no {
		require.False(t, isGlobalInput(f), f)
	}
}
