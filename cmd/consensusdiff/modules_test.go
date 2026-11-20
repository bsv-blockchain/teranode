package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

const baseGoMod = `module example.com/node

go 1.27

require (
	github.com/bsv-blockchain/go-bt/v2 v2.7.4
	github.com/bsv-blockchain/go-subtree v1.6.4
	github.com/bsv-blockchain/go-wire v1.5.0
	github.com/other/lib v1.0.0
)

replace github.com/bsv-blockchain/go-wire => github.com/fork/go-wire v1.5.1
`

var consensusModules = []string{
	"github.com/bsv-blockchain/go-bt/v2",
	"github.com/bsv-blockchain/go-subtree",
	"github.com/bsv-blockchain/go-wire",
	"github.com/bitcoin-sv/bdk/module/gobdk",
}

func mustParse(t *testing.T, s string) *modfile.File {
	t.Helper()

	f, err := parseGoMod("go.mod", []byte(s))
	require.NoError(t, err)

	return f
}

func TestReadModules(t *testing.T) {
	t.Run("comments and blank lines are ignored", func(t *testing.T) {
		modules, err := readModules(strings.NewReader("# header\n\ngithub.com/a/b  # why\ngithub.com/c/d\n"))
		require.NoError(t, err)
		require.Equal(t, []string{"github.com/a/b", "github.com/c/d"}, modules)
	})

	t.Run("a duplicate is rejected", func(t *testing.T) {
		_, err := readModules(strings.NewReader("github.com/a/b\ngithub.com/a/b\n"))
		require.ErrorContains(t, err, "listed twice")
	})

	t.Run("two fields on a line are rejected", func(t *testing.T) {
		_, err := readModules(strings.NewReader("github.com/a/b v1.0.0\n"))
		require.ErrorContains(t, err, "want one module path")
	})

	t.Run("an empty list is rejected", func(t *testing.T) {
		_, err := readModules(strings.NewReader("# nothing\n"))
		require.ErrorContains(t, err, "no modules listed")
	})
}

func TestChangedPins(t *testing.T) {
	base := mustParse(t, baseGoMod)

	t.Run("an unchanged go.mod has no changes", func(t *testing.T) {
		require.Empty(t, changedPins(base, mustParse(t, baseGoMod), consensusModules))
	})

	t.Run("a non-consensus bump is not a change", func(t *testing.T) {
		head := mustParse(t, strings.Replace(baseGoMod, "github.com/other/lib v1.0.0", "github.com/other/lib v1.1.0", 1))
		require.Empty(t, changedPins(base, head, consensusModules))
	})

	t.Run("a require bump, a replace bump and a new module are all changes", func(t *testing.T) {
		head := strings.Replace(baseGoMod, "go-bt/v2 v2.7.4", "go-bt/v2 v2.7.5", 1)
		head = strings.Replace(head, "github.com/fork/go-wire v1.5.1", "github.com/fork/go-wire v1.5.2", 1)
		head = strings.Replace(head, "github.com/other/lib v1.0.0", "github.com/other/lib v1.0.0\n\tgithub.com/bitcoin-sv/bdk/module/gobdk v1.4.0", 1)

		require.Equal(t, []pinChange{
			{path: "github.com/bsv-blockchain/go-bt/v2", base: "v2.7.4", head: "v2.7.5"},
			{path: "github.com/bsv-blockchain/go-wire", base: "v1.5.0 => github.com/fork/go-wire v1.5.1", head: "v1.5.0 => github.com/fork/go-wire v1.5.2"},
			{path: "github.com/bitcoin-sv/bdk/module/gobdk", base: none, head: "v1.4.0"},
		}, changedPins(base, mustParse(t, head), consensusModules))
	})

	t.Run("dropping a replace is a change", func(t *testing.T) {
		head := mustParse(t, strings.Replace(baseGoMod, "replace github.com/bsv-blockchain/go-wire => github.com/fork/go-wire v1.5.1\n", "", 1))
		require.Equal(t, []pinChange{
			{path: "github.com/bsv-blockchain/go-wire", base: "v1.5.0 => github.com/fork/go-wire v1.5.1", head: "v1.5.0"},
		}, changedPins(base, head, consensusModules))
	})

	t.Run("a replace for another version does not apply", func(t *testing.T) {
		f := mustParse(t, baseGoMod+"replace github.com/bsv-blockchain/go-bt/v2 v2.0.0 => ../go-bt\n")
		require.Equal(t, "v2.7.4", effectiveVersion(f, "github.com/bsv-blockchain/go-bt/v2"))
	})

	t.Run("a local directory replace has no version", func(t *testing.T) {
		f := mustParse(t, baseGoMod+"replace github.com/bsv-blockchain/go-subtree => ../go-subtree\n")
		require.Equal(t, "v1.6.4 => ../go-subtree", effectiveVersion(f, "github.com/bsv-blockchain/go-subtree"))
	})
}
