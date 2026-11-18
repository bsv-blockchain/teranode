package backfill

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// fakeFacts is a frozen store: what is readable and what already has a position.
type fakeFacts struct {
	gone     map[chainhash.Hash]bool
	existing map[chainhash.Hash]uint64
}

func (f fakeFacts) Readable(txid chainhash.Hash) (bool, error) { return !f.gone[txid], nil }

func (f fakeFacts) Existing(txid chainhash.Hash) (uint64, bool, error) {
	p, ok := f.existing[txid]
	return p, ok, nil
}

func h(s string) chainhash.Hash { return chainhash.HashH([]byte(s)) }

// The design's example: block 700,000 still holds live transactions A, B and C, and block 700,001
// holds D and E. F is unmined. Each block also holds a transaction since fully removed, and block
// 700,001 one that already had a leased position.
func exampleSource() (Source, fakeFacts) {
	src := Source{
		Blocks: []SourceBlock{
			{
				BlockID: 7, Height: 700_000, Hash: h("block 700000"), Coinbase: h("cb 700000"),
				Subtrees: []SourceSubtree{
					{Hash: h("st0"), TxIDs: []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, h("A"), h("gone1")}},
					{Hash: h("st1"), TxIDs: []chainhash.Hash{h("B"), h("C")}},
				},
			},
			{
				BlockID: 8, Height: 700_001, Hash: h("block 700001"), Coinbase: h("cb 700001"),
				Subtrees: []SourceSubtree{
					{Hash: h("st2"), TxIDs: []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, h("D"), h("leased"), h("E")}},
				},
			},
		},
		Suffix: []chainhash.Hash{h("F")},
	}

	facts := fakeFacts{
		gone:     map[chainhash.Hash]bool{h("gone1"): true},
		existing: map[chainhash.Hash]uint64{h("leased"): lane.Pack(3, 41_000)},
	}

	return src, facts
}

func TestBlockOrder(t *testing.T) {
	src, facts := exampleSource()

	order, err := BlockOrder(src.Blocks[0], facts)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{h("A"), h("B"), h("C")}, order, "subtree then node order; no placeholder, coinbase or removed transaction")

	order, err = BlockOrder(src.Blocks[1], facts)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{h("D"), h("E")}, order, "an existing position is kept, not renumbered")

	bad := facts
	bad.existing = map[chainhash.Hash]uint64{h("D"): lane.Pack(3, 0)}
	_, err = BlockOrder(src.Blocks[1], bad)
	require.Error(t, err, "an invalid existing position is an error, never a reason to renumber")
}

func TestNumberAssignsTheDesignExample(t *testing.T) {
	src, facts := exampleSource()

	m, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	seg, err := Number(context.Background(), m, src, facts, MemorySeen{})
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_001), seg.First)
	require.Equal(t, uint64(1_000_005), seg.PrefixLast)
	require.Equal(t, uint64(1_000_006), seg.Last)

	blocks := m.Blocks()
	require.Equal(t, []lane.ConversionBlock{
		{Seq: 0, BlockID: 7, Height: 700_000, First: 1_000_001, Count: 3},
		{Seq: 1, BlockID: 8, Height: 700_001, First: 1_000_004, Count: 2},
	}, blocks)

	suffix, first, err := m.SuffixTxIDs()
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{h("F")}, suffix)
	require.Equal(t, uint64(1_000_006), first)

	_, err = Number(context.Background(), m, src, facts, MemorySeen{})
	require.Error(t, err, "a finished segment is never renumbered")
}

// A crash after the first block resumes into the same manifest, byte for byte.
func TestNumberResumesAfterACrash(t *testing.T) {
	src, facts := exampleSource()
	dir := t.TempDir()

	whole, err := CreateManifest(filepath.Join(dir, "whole"), testHeader())
	require.NoError(t, err)
	want, err := Number(context.Background(), whole, src, facts, MemorySeen{})
	require.NoError(t, err)
	require.NoError(t, whole.Close())

	part, err := CreateManifest(filepath.Join(dir, "part"), testHeader())
	require.NoError(t, err)
	order, err := BlockOrder(src.Blocks[0], facts)
	require.NoError(t, err)
	_, err = part.AppendBlock(src.Blocks[0].BlockID, src.Blocks[0].Height, order)
	require.NoError(t, err)
	require.NoError(t, part.Close())

	resumed, err := ResumeManifest(filepath.Join(dir, "part"))
	require.NoError(t, err)

	defer func() { require.NoError(t, resumed.Close()) }()

	got, err := Number(context.Background(), resumed, src, facts, MemorySeen{})
	require.NoError(t, err)
	require.Equal(t, want, got, "same bounds and digest as an uninterrupted run")

	a, err := os.ReadFile(filepath.Join(dir, "whole"))
	require.NoError(t, err)
	b, err := os.ReadFile(filepath.Join(dir, "part"))
	require.NoError(t, err)
	require.Equal(t, a, b)
}

// A source that changed under a part-written manifest is refused rather than renumbered.
func TestNumberRefusesAChangedSource(t *testing.T) {
	src, facts := exampleSource()

	m, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	_, err = m.AppendBlock(src.Blocks[0].BlockID, src.Blocks[0].Height, []chainhash.Hash{h("A"), h("C"), h("B")})
	require.NoError(t, err)

	_, err = Number(context.Background(), m, src, facts, MemorySeen{})
	require.Error(t, err)

	// The same transactions recorded for another block, or at another height, are a changed
	// source too.
	order, err := BlockOrder(src.Blocks[0], facts)
	require.NoError(t, err)

	for name, at := range map[string][2]uint32{
		"another block ID": {src.Blocks[0].BlockID + 100, src.Blocks[0].Height},
		"another height":   {src.Blocks[0].BlockID, src.Blocks[0].Height - 1},
	} {
		mm, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
		require.NoError(t, err)

		_, err = mm.AppendBlock(at[0], at[1], order)
		require.NoError(t, err)

		_, err = Number(context.Background(), mm, src, facts, MemorySeen{})
		require.Error(t, err, name)
		require.NoError(t, mm.Close())
	}
}

// numberedPart writes the source's blocks, all of them or the first n, into a new manifest, and
// optionally a suffix, as a run that crashed before finishing would leave it.
func numberedPart(t *testing.T, path string, src Source, facts Facts, n int, suffix []chainhash.Hash) {
	t.Helper()

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)

	for _, b := range src.Blocks[:n] {
		order, err := BlockOrder(b, facts)
		require.NoError(t, err)

		_, err = m.AppendBlock(b.BlockID, b.Height, order)
		require.NoError(t, err)
	}

	if suffix != nil {
		_, err = m.AppendSuffix(suffix)
		require.NoError(t, err)
	}

	require.NoError(t, m.Close())
}

// A crash after the suffix record resumes to the same segment and bytes when the suffix matches,
// and is refused when the recorded suffix differs from the source's.
func TestNumberResumesAfterTheSuffix(t *testing.T) {
	src, facts := exampleSource()
	dir := t.TempDir()

	whole, err := CreateManifest(filepath.Join(dir, "whole"), testHeader())
	require.NoError(t, err)
	want, err := Number(context.Background(), whole, src, facts, MemorySeen{})
	require.NoError(t, err)
	require.NoError(t, whole.Close())

	suffix, err := SuffixOrder(src.Suffix, facts)
	require.NoError(t, err)

	numberedPart(t, filepath.Join(dir, "same"), src, facts, len(src.Blocks), suffix)

	resumed, err := ResumeManifest(filepath.Join(dir, "same"))
	require.NoError(t, err)
	got, err := Number(context.Background(), resumed, src, facts, MemorySeen{})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, resumed.Close())

	a, err := os.ReadFile(filepath.Join(dir, "whole"))
	require.NoError(t, err)
	b, err := os.ReadFile(filepath.Join(dir, "same"))
	require.NoError(t, err)
	require.Equal(t, a, b)

	numberedPart(t, filepath.Join(dir, "other"), src, facts, len(src.Blocks), []chainhash.Hash{h("not F")})

	resumed, err = ResumeManifest(filepath.Join(dir, "other"))
	require.NoError(t, err)
	_, err = Number(context.Background(), resumed, src, facts, MemorySeen{})
	require.Error(t, err, "the recorded suffix differs from the source's")
	require.NoError(t, resumed.Close())
}

// A manifest holding more blocks than the source is refused: the source lost a block.
func TestNumberRefusesAManifestLongerThanTheSource(t *testing.T) {
	src, facts := exampleSource()
	path := filepath.Join(t.TempDir(), "m")

	numberedPart(t, path, src, facts, len(src.Blocks), nil)

	short := src
	short.Blocks = src.Blocks[:1]

	m, err := ResumeManifest(path)
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	_, err = Number(context.Background(), m, short, facts, MemorySeen{})
	require.Error(t, err)
}

func TestNumberRefusesATransactionPlacedTwice(t *testing.T) {
	src, facts := exampleSource()
	src.Suffix = append(src.Suffix, h("A"))

	m, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	_, err = Number(context.Background(), m, src, facts, MemorySeen{})
	require.Error(t, err)

	src, _ = exampleSource()
	src.Blocks[1].Height = src.Blocks[0].Height

	m2, err := CreateManifest(filepath.Join(t.TempDir(), "m2"), testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m2.Close()) }()

	_, err = Number(context.Background(), m2, src, facts, MemorySeen{})
	require.Error(t, err, "blocks must rise in height")
}

// The import and numbering read nothing live: no txmeta decoder or cache, no lease or allocator
// client, no live store and no service.
func TestBackfillImportsNothingLive(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	require.NoError(t, err)

	for _, dep := range strings.Split(string(out), "\n") {
		for _, banned := range []string{
			"github.com/bsv-blockchain/teranode/services/",
			"github.com/bsv-blockchain/teranode/stores/txmetacache",
			"github.com/bsv-blockchain/teranode/stores/utxo/meta",
			"github.com/bsv-blockchain/teranode/stores/blockchain",
			"github.com/bsv-blockchain/teranode/stores/utxo/aerospike",
			"github.com/bsv-blockchain/teranode/stores/utxo/utxoset",
			"github.com/bsv-blockchain/teranode/stores/utxo/sql",
			"github.com/bsv-blockchain/teranode/stores/utxo/pebble",
			"github.com/bsv-blockchain/teranode/stores/blob",
		} {
			require.False(t, strings.HasPrefix(dep, banned), "backfill depends on %s", dep)
		}

		// The UTXO store interface package itself, whose prefix the helper packages share.
		require.NotEqual(t, "github.com/bsv-blockchain/teranode/stores/utxo", dep, "backfill depends on the UTXO store package")
	}
}
