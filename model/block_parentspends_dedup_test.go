package model

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// These tests drive validOrderAndBlessed on whole blocks to pin the duplicate
// input check: the verdict and error class a block gets, which transaction is
// reported, and what wins when a block has more than one problem.

// pvSpend is one input of a test transaction.
type pvSpend struct {
	parent int // pvExternal, pvMissing, or the global index of an in-block tx
	vout   uint32
}

const (
	pvExternal = -1 // the dummy parent seeded in the utxo store, mined on chain
	pvMissing  = -2 // a parent the utxo store does not know
)

// pvBlock is a block under test: txs[i] are the inputs of global tx i; txs[0] is
// the coinbase. Subtrees hold perSubtree txs each.
type pvBlock struct {
	block *Block
	deps  *validationDependencies
	store *mockSubtreeStore
	hash  func(i int) chainhash.Hash
}

func pvTxHash(i int) chainhash.Hash {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i)+1)

	return chainhash.Hash(sha256.Sum256(buf[:]))
}

var pvMissingHash = chainhash.HashH([]byte("pv-missing-parent"))

// buildPVBlock builds a block of len(txs) transactions (txs[0] = coinbase
// placeholder, its entry is used as the coinbase's inpoints in the meta) cut into
// subtrees of perSubtree txs (a power of two).
func buildPVBlock(t testing.TB, txs [][]pvSpend, perSubtree int) *pvBlock {
	t.Helper()

	headerBytes, _ := hex.DecodeString(block1Header)
	header, err := NewBlockHeaderFromBytes(headerBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	var (
		subtrees []*subtreepkg.Subtree
		roots    []*chainhash.Hash
	)

	metaStore := &mockSubtreeStore{data: map[string][]byte{}}

	for start := 0; start < len(txs); start += perSubtree {
		end := min(start+perSubtree, len(txs))

		subtree, err := subtreepkg.NewTreeByLeafCount(perSubtree)
		require.NoError(t, err)

		for i := start; i < end; i++ {
			if i == 0 {
				require.NoError(t, subtree.AddCoinbaseNode())
			} else {
				require.NoError(t, subtree.AddNode(pvTxHash(i), 1, 0))
			}
		}

		meta := subtreepkg.NewSubtreeMeta(subtree)

		for i := start; i < end; i++ {
			inputs := make([]*bt.Input, 0, len(txs[i]))

			for _, s := range txs[i] {
				parent := pvTxHash(s.parent)

				switch s.parent {
				case pvExternal:
					parent = dummyParentHash
				case pvMissing:
					parent = pvMissingHash
				}

				in := &bt.Input{PreviousTxOutIndex: s.vout}
				require.NoError(t, in.PreviousTxIDAdd(&parent))

				inputs = append(inputs, in)
			}

			var inpoints subtreepkg.TxInpoints
			if len(inputs) == 0 {
				inpoints = subtreepkg.NewTxInpoints()
			} else {
				inpoints, err = subtreepkg.NewTxInpointsFromInputs(inputs)
				require.NoError(t, err)
			}

			meta.TxInpoints[i-start] = inpoints
		}

		metaBytes, err := meta.Serialize()
		require.NoError(t, err)

		metaStore.data[string(subtree.RootHash()[:])] = metaBytes

		subtrees = append(subtrees, subtree)
		roots = append(roots, subtree.RootHash())
	}

	block, err := NewBlock(header, coinbase, roots, uint64(len(txs)), 123, 0, 0)
	require.NoError(t, err)

	block.SubtreeSlices = subtrees
	block.txMap = txmap.NewSplitSwissMapUint64(uint32(len(txs))) //nolint:gosec // test

	for i := 1; i < len(txs); i++ {
		h := pvTxHash(i)
		require.NoError(t, block.txMap.Put(h, uint64(i)))
	}

	// testing.TB twin of createTestUTXOStore + seedDummyParent, so benchmarks can
	// build blocks too
	storeURL, err := url.Parse("sqlitememory:///pv")
	require.NoError(t, err)

	store, err := sql.New(context.Background(), ulogger.TestLogger{}, settings.NewSettings(), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	_, _, err = store.SpendAndCreate(context.Background(), dummyParentTx, 1,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}), utxo.WithCreateOnly())
	require.NoError(t, err)

	return &pvBlock{
		block: block,
		store: metaStore,
		hash:  pvTxHash,
		deps: &validationDependencies{
			txMetaStore:           store,
			subtreeStore:          metaStore,
			currentChain:          []*BlockHeader{},
			currentBlockHeaderIDs: []uint32{1},
			oldBlockIDsMap:        txmap.NewSyncedMap[chainhash.Hash, []uint32](),
		},
	}
}

func (p *pvBlock) validate(dirs []string, concurrency int) error {
	return p.block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, p.deps, concurrency, dirs, 2)
}

// pvValidBlock is a block with no problem: tx i spends the external parent at
// vout i and, from tx 3 on, output 0 of tx i-2 (always earlier in the block).
func pvValidBlock(n int) [][]pvSpend {
	txs := make([][]pvSpend, n)
	for i := 1; i < n; i++ {
		txs[i] = []pvSpend{{pvExternal, uint32(i)}} //nolint:gosec // test
		if i >= 3 {
			txs[i] = append(txs[i], pvSpend{i - 2, 0})
		}
	}

	return txs
}

type pvOutcome int

const (
	pvOK pvOutcome = iota
	pvInvalidDup
	pvInvalidOrder
	pvIncomplete
)

func classify(t testing.TB, err error) pvOutcome {
	t.Helper()

	switch {
	case err == nil:
		return pvOK
	case errors.Is(err, errors.ErrBlockInvalid) && strings.Contains(err.Error(), "has duplicate inputs"):
		return pvInvalidDup
	case errors.Is(err, errors.ErrBlockInvalid) && strings.Contains(err.Error(), "comes before parent transaction"):
		return pvInvalidOrder
	case errors.Is(err, errors.ErrBlockIncomplete):
		return pvIncomplete
	}

	require.FailNow(t, "unexpected error", "%v", err)

	return pvOK
}

func TestParentSpends_ValidBlocks(t *testing.T) {
	for _, shape := range []struct{ txs, perSubtree int }{
		{1, 1}, {1, 4}, {2, 2}, {4, 4}, {5, 4}, {16, 4}, {33, 8}, {100, 16}, {257, 64},
	} {
		t.Run(fmt.Sprintf("txs=%d/subtree=%d", shape.txs, shape.perSubtree), func(t *testing.T) {
			p := buildPVBlock(t, pvValidBlock(shape.txs), shape.perSubtree)

			for _, workers := range []int{1, 3, 16} {
				require.NoError(t, p.validate(nil, workers))
			}
		})
	}
}

func TestParentSpends_NoSubtrees(t *testing.T) {
	p := buildPVBlock(t, pvValidBlock(1), 1)
	p.block.SubtreeSlices = nil
	p.block.Subtrees = nil

	require.NoError(t, p.validate(nil, 4))
}

// TestParentSpends_CoinbaseSkipped pins that the coinbase placeholder's inpoints
// (a real coinbase has none; the meta must not matter) never count as spends.
func TestParentSpends_CoinbaseSkipped(t *testing.T) {
	txs := pvValidBlock(8)
	txs[0] = []pvSpend{{pvExternal, 1}} // same inpoint as tx 1

	p := buildPVBlock(t, txs, 4)
	require.NoError(t, p.validate(nil, 4))
}

// TestParentSpends_DuplicateShapes pins the verdict and the reported tx for
// duplicates within a tx, within a subtree and across subtrees, for every
// worker count.
func TestParentSpends_DuplicateShapes(t *testing.T) {
	tests := []struct {
		name       string
		perSubtree int
		mutate     func(txs [][]pvSpend)
		wantTx     int // global index of the reported tx
	}{
		{"within one tx", 4, func(txs [][]pvSpend) { txs[2] = append(txs[2], txs[2][0]) }, 2},
		{"adjacent txs", 4, func(txs [][]pvSpend) { txs[3][0] = txs[2][0] }, 3},
		{"across subtrees", 4, func(txs [][]pvSpend) { txs[13][0] = txs[2][0] }, 13},
		{"first and last", 4, func(txs [][]pvSpend) { txs[1][0] = txs[15][0] }, 15},
		{
			"two pairs, earliest later-tx wins", 4,
			func(txs [][]pvSpend) {
				txs[14][0] = txs[1][0] // pair (1,14)
				txs[6][0] = txs[10][0] // pair (6,10)
			},
			10,
		},
		{
			"three-way, second spender reported", 4,
			func(txs [][]pvSpend) {
				txs[9][0] = txs[4][0]
				txs[12][0] = txs[4][0]
			},
			9,
		},
		{"same parent different vout is not a duplicate then a real one", 4, func(txs [][]pvSpend) {
			txs[5][0] = pvSpend{pvExternal, 99}
			txs[6][0] = pvSpend{pvExternal, 99}
		}, 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txs := pvValidBlock(16)
			tc.mutate(txs)

			p := buildPVBlock(t, txs, tc.perSubtree)

			for _, workers := range []int{1, 2, 4, 16} {
				for rep := 0; rep < 5; rep++ {
					err := p.validate(nil, workers)
					require.Equal(t, pvInvalidDup, classify(t, err), "workers %d: %v", workers, err)
					require.Contains(t, err.Error(), p.hashOf(tc.wantTx).String(), "workers %d", workers)
				}
			}
		})
	}
}

func (p *pvBlock) hashOf(i int) chainhash.Hash { return p.hash(i) }

// TestParentSpends_ForcedCollisions runs the same duplicate and non-duplicate
// blocks with a hash that collides for every inpoint, so every verdict comes
// from the real inpoints fetched back from the subtree meta.
func TestParentSpends_ForcedCollisions(t *testing.T) {
	prev := inpointDedupHash
	inpointDedupHash = func(subtreepkg.Inpoint) uint64 { return 7 }

	t.Cleanup(func() { inpointDedupHash = prev })

	t.Run("valid", func(t *testing.T) {
		p := buildPVBlock(t, pvValidBlock(64), 8)
		require.NoError(t, p.validate(nil, 4))
	})

	t.Run("duplicate across subtrees", func(t *testing.T) {
		txs := pvValidBlock(64)
		txs[40][0] = txs[3][0]
		txs[20][1] = txs[50][0]

		p := buildPVBlock(t, txs, 8)

		err := p.validate(nil, 4)
		require.Equal(t, pvInvalidDup, classify(t, err))
		require.Contains(t, err.Error(), p.hashOf(40).String())
	})

	t.Run("meta unreadable on the second read is an error, not a pass", func(t *testing.T) {
		txs := pvValidBlock(16)
		txs[9][0] = txs[2][0]

		p := buildPVBlock(t, txs, 4)
		p.deps.subtreeStore = &readOnceStore{mockSubtreeStore: p.store, reads: map[string]int{}}

		err := p.validate(nil, 4)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "has duplicate inputs")
	})
}

// readOnceStore serves each subtree's meta once; later reads fail.
type readOnceStore struct {
	*mockSubtreeStore
	mu    sync.Mutex
	reads map[string]int
}

func (r *readOnceStore) GetIoReader(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	r.mu.Lock()
	r.reads[string(key)]++
	n := r.reads[string(key)]
	r.mu.Unlock()

	if n > 1 {
		return nil, errors.NewProcessingError("meta gone")
	}

	return r.mockSubtreeStore.GetIoReader(ctx, key, ft, opts...)
}

// TestParentSpends_ErrorPrecedence pins which error a block with several problems
// gets. Within a subtree the order is the order of the checks the transactions go
// through (as before the duplicate check was batched): the first problem in tx
// order wins and a duplicate within the subtree beats the parent-chain check that
// follows the subtree's transactions.
func TestParentSpends_ErrorPrecedence(t *testing.T) {
	const perSubtree = 16 // one subtree: the order is deterministic

	// tx 5 spends tx 9, which comes later in the block: an order violation
	orderViolation := func(txs [][]pvSpend, at int) { txs[at] = append(txs[at], pvSpend{9, 1}) }
	// the tx spends a parent the store does not know: block incomplete
	missingParent := func(txs [][]pvSpend, at int) { txs[at] = append(txs[at], pvSpend{pvMissing, 0}) }
	// the tx duplicates tx 1's input
	dup := func(txs [][]pvSpend, at int) { txs[at][0] = txs[1][0] }

	tests := []struct {
		name   string
		mutate func(txs [][]pvSpend)
		want   pvOutcome
	}{
		{"dup before order violation", func(txs [][]pvSpend) { dup(txs, 3); orderViolation(txs, 5) }, pvInvalidDup},
		{"order violation before dup", func(txs [][]pvSpend) { orderViolation(txs, 3); dup(txs, 5) }, pvInvalidOrder},
		{"dup and order violation in the same tx", func(txs [][]pvSpend) { dup(txs, 5); orderViolation(txs, 5) }, pvInvalidDup},
		{"dup beats a missing parent of the same subtree", func(txs [][]pvSpend) { dup(txs, 12); missingParent(txs, 4) }, pvInvalidDup},
		{"dup beats a missing parent, dup first", func(txs [][]pvSpend) { dup(txs, 4); missingParent(txs, 12) }, pvInvalidDup},
		{"missing parent alone", func(txs [][]pvSpend) { missingParent(txs, 4) }, pvIncomplete},
		{"order violation alone", func(txs [][]pvSpend) { orderViolation(txs, 4) }, pvInvalidOrder},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txs := pvValidBlock(perSubtree)
			tc.mutate(txs)

			p := buildPVBlock(t, txs, perSubtree)

			for _, workers := range []int{1, 4} {
				for rep := 0; rep < 5; rep++ {
					require.Equal(t, tc.want, classify(t, p.validate(nil, workers)), "memory path, workers %d", workers)
				}
			}

			// the disk-backed map checks inline per tx and is the old behaviour:
			// the in-memory path must agree with it on single-subtree blocks
			require.Equal(t, tc.want, classify(t, p.validate([]string{t.TempDir()}, 4)), "disk path")
		})
	}
}

// TestParentSpends_CrossSubtreeProblemsAreDeterministic documents the change in
// precedence for problems in different subtrees. Before, which one a block got
// depended on which subtree's goroutine got there first. Now every per-tx and
// parent-chain error is found first and the cross-subtree duplicate is checked
// once all subtrees passed, so the answer no longer depends on scheduling.
func TestParentSpends_CrossSubtreeProblemsAreDeterministic(t *testing.T) {
	t.Run("duplicate across subtrees and a missing parent: incomplete", func(t *testing.T) {
		txs := pvValidBlock(16)
		txs[2][0] = txs[11][0]                           // cross-subtree duplicate, reported at tx 11
		txs[3] = append(txs[3], pvSpend{pvMissing, 0})   // missing parent in subtree 0
		txs[14] = append(txs[14], pvSpend{pvMissing, 1}) // and in subtree 3

		p := buildPVBlock(t, txs, 4)

		for _, workers := range []int{1, 2, 8} {
			for rep := 0; rep < 10; rep++ {
				require.Equal(t, pvIncomplete, classify(t, p.validate(nil, workers)), "workers %d", workers)
			}
		}
	})

	t.Run("duplicate across subtrees and an order violation elsewhere: order violation", func(t *testing.T) {
		txs := pvValidBlock(16)
		txs[2][0] = txs[11][0]
		txs[6] = append(txs[6], pvSpend{9, 1})

		p := buildPVBlock(t, txs, 4)

		for _, workers := range []int{1, 2, 8} {
			for rep := 0; rep < 10; rep++ {
				require.Equal(t, pvInvalidOrder, classify(t, p.validate(nil, workers)), "workers %d", workers)
			}
		}
	})
}

// TestParentSpends_RandomBlocksVsReference compares the verdict and reported tx
// with a plain map over the same block, for random blocks.
func TestParentSpends_RandomBlocksVsReference(t *testing.T) {
	rng := rand.New(rand.NewSource(5))

	for iter := 0; iter < 40; iter++ {
		n := 1 + rng.Intn(70)
		txs := make([][]pvSpend, n)

		for i := 1; i < n; i++ {
			for k := 0; k <= rng.Intn(3); k++ {
				txs[i] = append(txs[i], pvSpend{pvExternal, uint32(rng.Intn(max(1, n*3)))}) //nolint:gosec // test
			}
		}

		// reference: first tx (block order) spending a spent outpoint
		seen := map[pvSpend]bool{}
		want := -1

	scan:
		for i := 1; i < n; i++ {
			for _, s := range txs[i] {
				if seen[s] {
					want = i
					break scan
				}

				seen[s] = true
			}
		}

		p := buildPVBlock(t, txs, []int{2, 4, 8, 16}[rng.Intn(4)])
		err := p.validate(nil, 1+rng.Intn(6))

		if want < 0 {
			require.NoError(t, err, "iter %d", iter)
			continue
		}

		require.Equal(t, pvInvalidDup, classify(t, err), "iter %d", iter)
		require.Contains(t, err.Error(), p.hashOf(want).String(), "iter %d", iter)
	}
}

// releasingStore serves the subtree meta and, once a meta was read a second time
// (the re-read of the duplicate check), drops the nodes of every subtree of the
// block as a concurrent release of the loaded body would.
type releasingStore struct {
	*mockSubtreeStore
	mu       sync.Mutex
	reads    map[string]int
	subtrees []*subtreepkg.Subtree
}

type releasingReader struct {
	io.ReadCloser
	release func()
}

func (r releasingReader) Close() error {
	err := r.ReadCloser.Close()
	r.release()

	return err
}

func (r *releasingStore) GetIoReader(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	rc, err := r.mockSubtreeStore.GetIoReader(ctx, key, ft, opts...)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.reads[string(key)]++
	second := r.reads[string(key)] > 1
	r.mu.Unlock()

	if !second {
		return rc, nil
	}

	return releasingReader{rc, func() {
		for _, st := range r.subtrees {
			st.Nodes = nil
		}
	}}, nil
}

// TestParentSpends_NodesReleasedDuringDetect pins that a subtree whose nodes are
// released between finding the duplicate and naming its transaction yields an
// error, not an index out of range panic.
func TestParentSpends_NodesReleasedDuringDetect(t *testing.T) {
	prev := inpointDedupHash
	inpointDedupHash = func(subtreepkg.Inpoint) uint64 { return 3 }

	t.Cleanup(func() { inpointDedupHash = prev })

	txs := pvValidBlock(16)
	txs[2] = append(txs[2], txs[2][0]) // one tx spending an outpoint twice

	p := buildPVBlock(t, txs, 4)
	p.deps.subtreeStore = &releasingStore{mockSubtreeStore: p.store, reads: map[string]int{}, subtrees: p.block.SubtreeSlices}

	var err error

	require.NotPanics(t, func() { err = p.validate(nil, 4) })
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "%v", err)
}
