package subtreevalidation

import (
	"context"
	"maps"
	"math/rand/v2"
	"net/url"
	"regexp"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// readCountingUtxoStore wraps a real utxo.Store and counts every record read the
// conflict check can issue: single Gets, GetMetas, and each item of a
// BatchDecorate.
type readCountingUtxoStore struct {
	utxo.Store
	reads atomic.Int64
}

func (c *readCountingUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	c.reads.Add(1)
	return c.Store.Get(ctx, hash, f...)
}

func (c *readCountingUtxoStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	c.reads.Add(1)
	return c.Store.GetMeta(ctx, hash, data)
}

func (c *readCountingUtxoStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	c.reads.Add(int64(len(items)))
	return c.Store.BatchDecorate(ctx, items, f...)
}

func newSQLUtxoStore(t *testing.T, ctx context.Context, tSettings *settings.Settings) utxo.Store {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, utxoStoreURL)
	require.NoError(t, err)

	return utxoStore
}

// createConflicting stores tx flagged Conflicting without spending its inputs:
// the store records it as a conflicting child of each parent, not as a spender.
func createConflicting(t *testing.T, ctx context.Context, store utxo.Store, tx *bt.Tx) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)
}

// spendAndCreate stores tx as the recorded spender of its inputs, even where a
// parent is conflicting.
func spendAndCreate(t *testing.T, ctx context.Context, store utxo.Store, tx *bt.Tx, opts ...utxo.CreateOption) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, 122, utxo.WithSpendOnly(), utxo.WithIgnoreConflicting(true))
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, tx, 122, append([]utxo.CreateOption{utxo.WithCreateOnly()}, opts...)...)
	require.NoError(t, err)
}

// buildConflictingChain stores the issue 2037 shape: counter is the recorded
// spender of the parentTx1 output that tx1 also spends, and tx1 heads a linear
// chain of n conflicting transactions, each a conflicting child of the one before.
// With recordedSpenders, each chain member after tx1 is also the recorded spender
// of the output it spends, so it is its own counter-spender. The chain is returned
// head first.
func buildConflictingChain(t *testing.T, ctx context.Context, store utxo.Store, n int, recordedSpenders bool, counterOpts ...utxo.CreateOption) (*bt.Tx, []chainhash.Hash) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	counter := tx1.Clone()
	counter.Version = 2
	spendAndCreate(t, ctx, store, counter, counterOpts...)

	chain := make([]chainhash.Hash, 0, n)

	prev := tx1
	createConflicting(t, ctx, store, prev)
	chain = append(chain, *prev.TxIDChainHash())

	for len(chain) < n {
		child := spendingChild(t, prev, 0)

		if recordedSpenders {
			_, _, err = store.SpendAndCreate(ctx, child, 123, utxo.WithSpendOnly(), utxo.WithIgnoreConflicting(true))
			require.NoError(t, err)
		}

		createConflicting(t, ctx, store, child)
		chain = append(chain, *child.TxIDChainHash())
		prev = child
	}

	return counter, chain
}

// newCachedCheckServer returns a server over store, and a block whose single
// cached subtree names conflictingTxs in its conflicting-node trailer. The block's
// candidate chain is blockIDs.
func newCachedCheckServer(t *testing.T, store utxo.Store, tSettings *settings.Settings, conflictingTxs []chainhash.Hash, blockIDs []uint32) (*Server, *model.Block, []chainhash.Hash) {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(conflictingTxs) + 1)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, txHash := range conflictingTxs {
		require.NoError(t, st.AddNode(txHash, 1, 1))
		require.NoError(t, st.AddConflictingNode(txHash))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	subtreeStore := memory.New()
	rootHash := st.RootHash()
	require.NoError(t, subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, serialized))

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return(blockIDs, nil)

	prevHash := chainhash.HashH([]byte("candidate-parent"))
	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      12345678,
			Bits:           model.NBit{},
			Nonce:          1,
		},
		Height:   200,
		Subtrees: []*chainhash.Hash{rootHash},
	}

	s := &Server{
		logger:           ulogger.NewErrorTestLogger(t),
		settings:         tSettings,
		utxoStore:        store,
		subtreeStore:     subtreeStore,
		blockchainClient: mockBlockchain,
	}

	return s, block, []chainhash.Hash{*rootHash}
}

// Test_checkCachedSubtreesAgainstCandidateChain_scaling reproduces the issue 2037
// wedge: a block whose cached subtrees name every member of a linear conflicting
// chain. Checked one transaction at a time, every member re-walks the rest of the
// chain, so store reads grow as N^2/2 (a 1,788-transaction chain took about five
// hours). Checked as one batch, each record is read a constant number of times.
//
// Both chain shapes are covered: members linked only as conflicting children
// (their parent records no spender), and members that are also their parent's
// recorded spender, so each is its own counter-spender and its cone is walked as
// a counter-spender cone.
//
// The bound is c = 8 reads per conflicting transaction. For these shapes the
// batched check needs four: its own inpoints and its parent's spenders (phase 1),
// one read as a member of the walk (phase 2), and one GetMeta as a member of the
// counter-conflicting union (phase 3). One more is for the reads that discover
// which cached transactions are conflicting, when those come from the store
// rather than the subtree trailer, two are the check at the block's height
// (checkStoredTxAtHeight: the tx's body and its parent), and one is headroom.
func Test_checkCachedSubtreesAgainstCandidateChain_scaling(t *testing.T) {
	const chainLength = 200

	for _, recordedSpenders := range []bool{false, true} {
		t.Run(map[bool]string{false: "conflicting children", true: "recorded spenders"}[recordedSpenders], func(t *testing.T) {
			ctx := context.Background()
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.SpendBatcherDurationMillis = 1

			store := newSQLUtxoStore(t, ctx, tSettings)
			_, chain := buildConflictingChain(t, ctx, store, chainLength, recordedSpenders)

			countingStore := &readCountingUtxoStore{Store: store}
			s, block, cachedSubtrees := newCachedCheckServer(t, countingStore, tSettings, chain, []uint32{77})

			require.NoError(t, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))

			reads := countingStore.reads.Load()
			t.Logf("%d store reads for a %d-transaction conflicting chain", reads, chainLength)

			require.LessOrEqualf(t, reads, int64(8*chainLength),
				"the cached-subtree conflict check must read a conflicting chain O(N) times, got %d reads for N=%d (O(N^2) is the per-transaction re-walk of issue 2037)",
				reads, chainLength)
		})
	}
}

// verdict is the class of a conflict-check outcome: what block validation acts on.
type verdict string

const (
	verdictPass       verdict = "pass"
	verdictTxInvalid  verdict = "tx-invalid"
	verdictProcessing verdict = "processing"
)

func verdictOf(err error) verdict {
	switch {
	case err == nil:
		return verdictPass
	case errors.Is(err, errors.ErrTxInvalid):
		return verdictTxInvalid
	default:
		return verdictProcessing
	}
}

type outpoint struct {
	tx   *bt.Tx
	vout uint32
}

// buildTx returns a tx spending inputs into nOutputs outputs. salt keeps hashes
// distinct between txs that spend the same inputs.
func buildTx(t *testing.T, inputs []outpoint, nOutputs int, salt uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = salt

	var total uint64

	for _, in := range inputs {
		out := in.tx.Outputs[in.vout]
		require.NoError(t, tx.From(in.tx.TxIDChainHash().String(), in.vout, out.LockingScript.String(), out.Satoshis))
		total += out.Satoshis
	}

	for _, in := range tx.Inputs {
		in.UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	for i := 0; i < nOutputs; i++ {
		require.NoError(t, tx.AddP2PKHOutputFromScript(parentTx1.Outputs[0].LockingScript, total/uint64(2*nOutputs)))
	}

	return tx
}

const (
	equivalenceOnChainBlockID  = uint32(77)
	equivalenceOffChainBlockID = uint32(99)
)

// buildRandomConflictGraph stores one seed's random graph: recorded spenders and
// conflicting txs over shared outputs, so conflicting txs have counter-spenders,
// counter-spender cones, own cones through both spends and conflicting children,
// and members of one tx's sets in another's. Some conflicting txs are also the
// recorded spender of their inputs. Some txs are mined on the candidate chain,
// some on another chain, and at most one output is frozen. It returns the
// conflicting txs and every tx it stored.
func buildRandomConflictGraph(t *testing.T, ctx context.Context, store utxo.Store, tSettings *settings.Settings, rng *rand.Rand, seed uint64) (conflicting []chainhash.Hash, stored []*bt.Tx) {
	t.Helper()

	const txsPerSeed = 20

	var unspent, spent []outpoint

	for r := 0; r < 3; r++ {
		fake := bt.NewTx()
		require.NoError(t, fake.AddP2PKHOutputFromScript(parentTx1.Outputs[0].LockingScript, 1_000_000_000))
		fake.LockTime = uint32(seed)*1000 + uint32(r)

		root := buildTx(t, []outpoint{{tx: fake, vout: 0}}, 3, uint32(seed)*1000+uint32(r))
		_, _, err := store.SpendAndCreate(ctx, root, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		stored = append(stored, root)

		for v := range root.Outputs {
			unspent = append(unspent, outpoint{tx: root, vout: uint32(v)})
		}
	}

	for i := 0; i < txsPerSeed; i++ {
		isConflicting := rng.IntN(2) == 0

		pool, fromSpent := unspent, false
		if isConflicting && len(spent) > 0 && rng.IntN(10) < 6 {
			pool, fromSpent = spent, true
		}

		if len(pool) == 0 {
			continue
		}

		nIn := 1 + rng.IntN(2)
		picked := map[int]struct{}{}

		var inputs []outpoint

		for len(inputs) < nIn && len(picked) < len(pool) {
			idx := rng.IntN(len(pool))
			if _, ok := picked[idx]; ok {
				continue
			}

			picked[idx] = struct{}{}
			inputs = append(inputs, pool[idx])
		}

		tx := buildTx(t, inputs, 1+rng.IntN(3), uint32(seed)*1000+100+uint32(i))

		var opts []utxo.CreateOption

		switch roll := rng.IntN(40); {
		case roll < 3:
			opts = append(opts, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: equivalenceOnChainBlockID, BlockHeight: 100}))
		case roll < 6:
			opts = append(opts, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: equivalenceOffChainBlockID, BlockHeight: 100}))
		}

		// a conflicting tx over unspent outputs is sometimes also their
		// recorded spender, which makes it its own counter-spender
		recordsSpend := !isConflicting || !fromSpent && rng.IntN(2) == 0

		if isConflicting {
			if recordsSpend {
				_, _, err := store.SpendAndCreate(ctx, tx, 123, utxo.WithSpendOnly(), utxo.WithIgnoreConflicting(true))
				require.NoError(t, err)
			}

			_, _, err := store.SpendAndCreate(ctx, tx, 123, append(opts, utxo.WithConflicting(true), utxo.WithCreateOnly())...)
			require.NoError(t, err)

			conflicting = append(conflicting, *tx.TxIDChainHash())
		} else {
			spendAndCreate(t, ctx, store, tx, opts...)
		}

		stored = append(stored, tx)

		if recordsSpend {
			// in index order, not map order, so a seed always builds the same graph
			for _, idx := range slices.Sorted(maps.Keys(picked)) {
				spent = append(spent, pool[idx])
			}

			remaining := unspent[:0:0]

			for idx, out := range unspent {
				if _, ok := picked[idx]; !ok {
					remaining = append(remaining, out)
				}
			}

			unspent = remaining
		}

		for v := range tx.Outputs {
			unspent = append(unspent, outpoint{tx: tx, vout: uint32(v)})
		}
	}

	// at most one frozen output: the graph is dense enough that a frozen output
	// anywhere tends to reach most cones
	if len(unspent) > 0 && rng.IntN(3) == 0 {
		out := unspent[rng.IntN(len(unspent))]
		freezeOutput(t, ctx, store, out.tx, out.vout, tSettings)
	}

	return conflicting, stored
}

// perTxVerdict is the reference: checkCounterConflictingOnCurrentChain run on
// every conflicting tx, reduced the way the batch defines its verdict: any
// tx-invalid wins, then any processing error, else pass. Where the
// per-transaction results do not mix classes, that is also exactly what a loop
// returned; where they do mix, a loop's answer depended on its order. It also
// reports whether any per-tx check failed on a missing record.
func perTxVerdict(t *testing.T, ctx context.Context, s *Server, conflicting []chainhash.Hash, blockIds map[uint32]bool) (want verdict, sawNotFound bool) {
	t.Helper()

	want = verdictPass

	for _, txHash := range conflicting {
		err := s.checkCounterConflictingOnCurrentChain(ctx, txHash, blockIds)
		sawNotFound = sawNotFound || errors.Is(err, errors.ErrTxNotFound)

		switch verdictOf(err) {
		case verdictTxInvalid:
			want = verdictTxInvalid
		case verdictProcessing:
			if want == verdictPass {
				want = verdictProcessing
			}
		}
	}

	return want, sawNotFound
}

// requireBatchMatchesPerTx compares the batched verdict, got, with the per-tx
// reference, want, and reports whether they differed in the one way allowed.
//
// They must be equal, except where the reference is processing and the batch is
// tx-invalid. A per-tx check that fails (a frozen output in the tx's own cone, a
// missing record) gives up before its mined check; the batch carries on and
// mined-checks what it could read, and a proven tx-invalid wins. Then the batch
// must name a tx that is mined on the candidate chain and is in the
// counter-conflicting set of some conflicting tx, as lenientCounterConflicting
// derives it from the records the store still holds.
func requireBatchMatchesPerTx(t *testing.T, ctx context.Context, store utxo.Store, conflicting []chainhash.Hash, blockIds map[uint32]bool, seed uint64, want verdict, gotErr error) (got verdict, oneWay bool) {
	t.Helper()

	got = verdictOf(gotErr)
	if want == got {
		return got, false
	}

	require.Truef(t, want == verdictProcessing && got == verdictTxInvalid,
		"seed %d: batched verdict %q differs from the per-transaction check's %q (batch: %v)", seed, got, want, gotErr)

	var minedNamed []chainhash.Hash

	for _, named := range regexp.MustCompile(`[0-9a-f]{64}`).FindAllString(gotErr.Error(), -1) {
		hash, err := chainhash.NewHashFromStr(named)
		require.NoError(t, err)

		namedMeta := &meta.Data{}
		if store.GetMeta(ctx, hash, namedMeta) != nil {
			continue
		}

		for _, blockID := range namedMeta.BlockIDs {
			if blockIds[blockID] {
				minedNamed = append(minedNamed, *hash)
				break
			}
		}
	}

	require.NotEmptyf(t, minedNamed, "seed %d: a tx-invalid verdict the per-tx checks did not reach must name a tx mined on the candidate chain (batch: %v)", seed, gotErr)

	for _, txHash := range conflicting {
		members := lenientCounterConflicting(t, ctx, store, txHash)

		for _, mined := range minedNamed {
			if _, ok := members[mined]; ok {
				return got, true
			}
		}
	}

	require.Failf(t, "unproven tx-invalid", "seed %d: the mined tx the batch names is in no conflicting tx's counter-conflicting set (batch: %v)", seed, gotErr)

	return got, true
}

// lenientCounterConflicting is the test's own derivation of txHash's
// counter-conflicting set: the tx, the recorded spenders of the outputs it
// spends, and their descendants through recorded spends and conflicting
// children. Unlike utxo.GetCounterConflictingTxHashes it never fails: a record
// that cannot be read, an out-of-range input and the frozen sentinel are leaves,
// so it yields every member reachable from what the store still holds.
func lenientCounterConflicting(t *testing.T, ctx context.Context, store utxo.Store, txHash chainhash.Hash) map[chainhash.Hash]struct{} {
	t.Helper()

	members := map[chainhash.Hash]struct{}{}

	txMeta, err := store.Get(ctx, &txHash, fields.TxInpoints)
	if err != nil || txMeta == nil {
		return members
	}

	members[txHash] = struct{}{}

	// walked is separate from members: a tx that is its own counter-spender is a
	// member from the start, and its cone must still be walked, as
	// utxo.GetCounterConflictingTxHashes walks it
	walked := map[chainhash.Hash]struct{}{}

	var queue []chainhash.Hash

	visit := func(hash chainhash.Hash) {
		if hash.Equal(subtreepkg.FrozenBytesTxHash) {
			return
		}

		members[hash] = struct{}{}

		if _, ok := walked[hash]; !ok {
			walked[hash] = struct{}{}
			queue = append(queue, hash)
		}
	}

	for _, inpoint := range txMeta.TxInpoints.GetTxInpoints() {
		parentMeta, err := store.Get(ctx, &inpoint.Hash, fields.Utxos)
		if err != nil || parentMeta == nil || len(parentMeta.SpendingDatas) <= int(inpoint.Index) {
			continue
		}

		if spendingData := parentMeta.SpendingDatas[inpoint.Index]; spendingData != nil && spendingData.TxID != nil {
			visit(*spendingData.TxID)
		}
	}

	for len(queue) > 0 {
		hash := queue[0]
		queue = queue[1:]

		hashMeta, err := store.Get(ctx, &hash, fields.Utxos, fields.ConflictingChildren)
		if err != nil || hashMeta == nil {
			continue
		}

		for _, child := range hashMeta.ConflictingChildren {
			visit(child)
		}

		for _, spendingData := range hashMeta.SpendingDatas {
			if spendingData != nil && spendingData.TxID != nil {
				visit(*spendingData.TxID)
			}
		}
	}

	return members
}

// Test_checkCounterConflictingOnCurrentChainBatch_equivalence pins the batched
// check to the per-transaction check it replaces, over seeded random graphs (see
// buildRandomConflictGraph), with the walk unbounded.
//
//   - Cached path, every record present: the batched verdict through
//     checkCachedSubtreesAgainstCandidateChain equals the per-tx reference.
//   - Fresh path's failOnMissingRecords policy, with some records deleted: the
//     batched verdict equals the per-tx reference, in which a missing record is a
//     retryable ErrTxNotFound, so a gap anywhere either path looks yields the same
//     class.
//
// Both allow one difference (see requireBatchMatchesPerTx): the batch proves
// tx-invalid from evidence a failing per-tx check never reached.
func Test_checkCounterConflictingOnCurrentChainBatch_equivalence(t *testing.T) {
	const seeds = 60

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.ConflictingChildrenMaxNodes = 0
	// the generator issues hundreds of sequential spends; don't wait out a
	// batcher window on each
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDurationMillis = 1

	blockIds := map[uint32]bool{equivalenceOnChainBlockID: true}

	t.Run("cached path, every record present", func(t *testing.T) {
		seen := map[verdict]int{}
		oneWay := 0

		for seed := uint64(1); seed <= seeds; seed++ {
			ctx := context.Background()
			store := newSQLUtxoStore(t, ctx, tSettings)

			conflicting, _ := buildRandomConflictGraph(t, ctx, store, tSettings, rand.New(rand.NewPCG(seed, 2037)), seed)
			if len(conflicting) == 0 {
				continue
			}

			s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflicting, []uint32{equivalenceOnChainBlockID})

			want, sawNotFound := perTxVerdict(t, ctx, s, conflicting, blockIds)
			require.Falsef(t, sawNotFound, "seed %d: every record is present", seed)

			got, differs := requireBatchMatchesPerTx(t, ctx, store, conflicting, blockIds, seed, want, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
			if differs {
				oneWay++
			}

			seen[got]++
		}

		t.Logf("batched verdicts over %d seeds: %v, %d proven tx-invalid by the batch only", seeds, seen, oneWay)

		// the generator must exercise every verdict, or the comparison proves little
		for _, v := range []verdict{verdictPass, verdictTxInvalid, verdictProcessing} {
			require.Positivef(t, seen[v], "no seed produced verdict %q; retune the generator", v)
		}
	})

	t.Run("fresh-path policy, some records missing", func(t *testing.T) {
		seen := map[verdict]int{}
		notFoundSeeds := 0
		oneWay := 0

		for seed := uint64(1); seed <= seeds; seed++ {
			ctx := context.Background()
			rng := rand.New(rand.NewPCG(seed, 2038))
			store := newSQLUtxoStore(t, ctx, tSettings)

			conflicting, stored := buildRandomConflictGraph(t, ctx, store, tSettings, rng, seed)
			if len(conflicting) == 0 {
				continue
			}

			// prune at most one record, anywhere: a conflicting tx, a parent, a
			// counter-spender or a cone member. The graph is dense enough that more
			// reaches nearly every check.
			if rng.IntN(2) == 0 {
				require.NoError(t, store.Delete(ctx, stored[rng.IntN(len(stored))].TxIDChainHash()))
			}

			s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

			want, sawNotFound := perTxVerdict(t, ctx, s, conflicting, blockIds)
			if sawNotFound {
				notFoundSeeds++
			}

			_, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, conflicting, blockIds, failOnMissingRecords)
			got, differs := requireBatchMatchesPerTx(t, ctx, store, conflicting, blockIds, seed, want, err)
			if differs {
				oneWay++
			}

			seen[got]++
		}

		t.Logf("batched verdicts over %d seeds: %v, %d with a per-tx ErrTxNotFound, %d proven tx-invalid by the batch only", seeds, seen, notFoundSeeds, oneWay)

		require.Positive(t, notFoundSeeds, "no seed pruned a record the check reads; retune the generator")

		for _, v := range []verdict{verdictPass, verdictTxInvalid, verdictProcessing} {
			require.Positivef(t, seen[v], "no seed produced verdict %q; retune the generator", v)
		}
	})
}

// buildIndependentConflicts stores n unrelated double spends. Each has its own
// parent, the parent's recorded spender (the counter-spender, stored with
// counterOpts) and a conflicting tx over the same output; no cone has a
// descendant. It returns the conflicting txs.
func buildIndependentConflicts(t *testing.T, ctx context.Context, store utxo.Store, n int, counterOpts ...utxo.CreateOption) []chainhash.Hash {
	t.Helper()

	conflicting := make([]chainhash.Hash, 0, n)

	for i := 0; i < n; i++ {
		salt := uint32(50_000 + 10*i)

		funding := bt.NewTx()
		require.NoError(t, funding.AddP2PKHOutputFromScript(parentTx1.Outputs[0].LockingScript, 1_000_000))
		funding.LockTime = salt

		parent := buildTx(t, []outpoint{{tx: funding, vout: 0}}, 1, salt+1)
		_, _, err := store.SpendAndCreate(ctx, parent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		spendAndCreate(t, ctx, store, buildTx(t, []outpoint{{tx: parent, vout: 0}}, 1, salt+2), counterOpts...)

		tx := buildTx(t, []outpoint{{tx: parent, vout: 0}}, 1, salt+3)
		createConflicting(t, ctx, store, tx)
		conflicting = append(conflicting, *tx.TxIDChainHash())
	}

	return conflicting
}

// The walk budget is utxostore_conflictingChildrenMaxNodes times the number of
// roots the batch walks (its conflicting txs and their counter-spenders). That is
// the old aggregate bound: N per-transaction walks of at most maxNodes each. A
// block of many small, unrelated conflicts must pass wherever every per-tx walk
// did, however many there are; a single cone too big for the old budget must
// still fail closed.
func Test_checkCounterConflictingOnCurrentChainBatch_budgetScalesWithRoots(t *testing.T) {
	ctx := context.Background()

	t.Run("many independent conflicts with empty cones pass", func(t *testing.T) {
		const conflicts = 20

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = 3

		store := newSQLUtxoStore(t, ctx, tSettings)
		conflicting := buildIndependentConflicts(t, ctx, store, conflicts)

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflicting, []uint32{77})

		for _, txHash := range conflicting {
			require.NoError(t, s.checkCounterConflictingOnCurrentChain(ctx, txHash, map[uint32]bool{77: true}),
				"each per-transaction walk fits the budget")
		}

		// 2*conflicts nodes in all, far past maxNodes, but within maxNodes per root
		require.NoError(t, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
	})

	t.Run("a chain passes where every per-tx walk does", func(t *testing.T) {
		const chainLength = 10

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = chainLength

		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, chainLength, false)

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})

		for _, txHash := range chain {
			require.NoError(t, s.checkCounterConflictingOnCurrentChain(ctx, txHash, map[uint32]bool{77: true}))
		}

		require.NoError(t, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
	})

	t.Run("one oversized cone still fails closed", func(t *testing.T) {
		const coneLength = 10

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = 5

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false)

		// two roots (tx1 and its counter-spender), so a budget of 10, against a
		// counter-spender cone of 10 more
		prev := counter
		for i := 0; i < coneLength; i++ {
			child := spendingChild(t, prev, 0)
			spendAndCreate(t, ctx, store, child)
			prev = child
		}

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})

		require.ErrorIs(t, s.checkCounterConflictingOnCurrentChain(ctx, chain[0], map[uint32]bool{77: true}), errors.ErrUtxoWalkLimitExceeded,
			"the per-transaction walk exceeds the budget too")

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrUtxoWalkLimitExceeded)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)
	})
}

// A trailer can name a transaction whose record, or whose parent's record, has
// been pruned since the subtree was blessed. Its counter-spenders cannot be
// derived, so it is dropped from the check and the block passes on the rest, as
// the per-transaction loop's ErrTxNotFound skip did. Dropping one must not hide a
// real conflict on another.
func Test_checkCounterConflictingOnCurrentChainBatch_notFoundDropped(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	newFixture := func(t *testing.T, counterOpts ...utxo.CreateOption) (utxo.Store, []chainhash.Hash) {
		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, 3, false, counterOpts...)

		// a conflicting tx whose parent the store no longer holds
		orphanParent := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 1, 9001)
		_, _, err := store.SpendAndCreate(ctx, orphanParent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		orphan := spendingChild(t, orphanParent, 0)
		createConflicting(t, ctx, store, orphan)
		require.NoError(t, store.Delete(ctx, orphanParent.TxIDChainHash()))

		// and one whose own record is gone
		pruned := chainhash.HashH([]byte("pruned-conflicting-tx"))

		return store, append([]chainhash.Hash{pruned, *orphan.TxIDChainHash()}, chain...)
	}

	t.Run("dropped and the rest passes", func(t *testing.T) {
		store, conflictingTxs := newFixture(t)
		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflictingTxs, []uint32{77})

		notFound, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, conflictingTxs, map[uint32]bool{77: true}, skipMissingRecords)
		require.NoError(t, err)
		require.ElementsMatch(t, conflictingTxs[:2], notFound)

		require.NoError(t, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
	})

	t.Run("a real conflict on another tx still rejects", func(t *testing.T) {
		store, conflictingTxs := newFixture(t, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100}))
		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflictingTxs, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
	})
}

// Decision for records missing further out than a transaction and its parents:
// a cone member the store does not hold is a leaf of the walk, and a
// counter-conflicting member whose GetMeta is not found counts as not mined; the
// rest of the union is still checked. The per-transaction check abandoned the
// whole transaction on any missing member, so here it passes a block whose
// counter-spender is mined on the candidate chain, and the batch rejects it.
func Test_checkCounterConflictingOnCurrentChainBatch_missingMemberMidCone(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	store := newSQLUtxoStore(t, ctx, tSettings)
	counter, chain := buildConflictingChain(t, ctx, store, 3, false, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100}))

	// the counter-spender's child, a member of the counter-conflicting cone, is gone
	counterChild := spendingChild(t, counter, 0)
	spendAndCreate(t, ctx, store, counterChild)
	require.NoError(t, store.Delete(ctx, counterChild.TxIDChainHash()))

	s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})

	err := s.checkCounterConflictingOnCurrentChain(ctx, chain[0], map[uint32]bool{77: true})
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the per-transaction check gives up on a missing member")

	err = s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
	require.ErrorIs(t, err, errors.ErrTxInvalid)
	require.Contains(t, err.Error(), counter.TxIDChainHash().String(), "the verdict names the mined counter-conflicting tx")

	t.Run("a store fault on another member does not hide the mined one", func(t *testing.T) {
		faulty := &faultyGetMetaStore{Store: store, fail: chain[1]}
		s, block, cachedSubtrees := newCachedCheckServer(t, faulty, tSettings, chain, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrTxInvalid, "the mined counter-spender was read, so the conflict is proven")
		require.Contains(t, err.Error(), counter.TxIDChainHash().String())
	})

	t.Run("a store fault on the mined member is a processing error", func(t *testing.T) {
		faulty := &faultyGetMetaStore{Store: store, fail: *counter.TxIDChainHash()}
		s, block, cachedSubtrees := newCachedCheckServer(t, faulty, tSettings, chain, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.NotErrorIs(t, err, errors.ErrTxInvalid, "nothing read proves a conflict, so the verdict is retryable")
	})
}

// faultyGetMetaStore fails GetMeta for one hash with a storage error.
type faultyGetMetaStore struct {
	utxo.Store
	fail chainhash.Hash
}

func (f *faultyGetMetaStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if hash.Equal(f.fail) {
		return errors.NewStorageError("store down")
	}

	return f.Store.GetMeta(ctx, hash, data)
}

// A frozen output anywhere the per-transaction check would have looked is a
// processing error from the batch too: on a counter-spender's own input (the
// counter-spender is the frozen sentinel), deep in a counter-spender's cone, and
// in a conflicting transaction's own cone.
func Test_checkCounterConflictingOnCurrentChainBatch_frozen(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	check := func(t *testing.T, store utxo.Store, conflictingTxs []chainhash.Hash, wantMessage string) {
		t.Helper()

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflictingTxs, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)
		require.Contains(t, err.Error(), wantMessage)
	}

	t.Run("frozen direct counter", func(t *testing.T) {
		store := newSQLUtxoStore(t, ctx, tSettings)

		_, _, err := store.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		freezeOutput(t, ctx, store, parentTx1, 2, tSettings)
		createConflicting(t, ctx, store, tx1)

		check(t, store, []chainhash.Hash{*tx1.TxIDChainHash()}, "counter conflicting tx is frozen")
	})

	t.Run("frozen deep in counter cone", func(t *testing.T) {
		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 3, false)

		counterChild := spendingChild(t, counter, 0)
		spendAndCreate(t, ctx, store, counterChild)
		freezeOutput(t, ctx, store, counterChild, 0, tSettings)

		check(t, store, chain, "a counter-conflicting tx has frozen child")
	})

	t.Run("frozen in own cone", func(t *testing.T) {
		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, 3, false)

		// the chain tip's output is unspent; freezing it puts the sentinel in
		// every chain member's own cone and in no counter-spender's
		tip, err := store.Get(ctx, &chain[len(chain)-1], fields.Tx)
		require.NoError(t, err)
		freezeOutput(t, ctx, store, tip.Tx, 0, tSettings)

		check(t, store, chain, "child transaction is frozen")
	})
}

// The fresh path asks the same question of every transaction the store flags
// Conflicting, from concurrent decorate batches. It must ask it once for the
// whole set after every batch has decorated, not once per transaction inside
// each batch, and a genuine conflict must still surface as tx-invalid through its
// wrapping. Unlike the cached path, a transaction whose counter-spenders cannot
// be derived is still a retryable failure there, as it was before.
func Test_processTxMetaUsingStore_batchedConflictCheck(t *testing.T) {
	const chainLength = 60

	ctx := context.Background()

	newFixture := func(t *testing.T, counterOpts ...utxo.CreateOption) (*readCountingUtxoStore, *Server, []chainhash.Hash) {
		tSettings := test.CreateBaseTestSettings(t)
		// several concurrent decorate batches, each seeing conflicting txs
		tSettings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 8
		tSettings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 4

		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, chainLength, false, counterOpts...)

		countingStore := &readCountingUtxoStore{Store: store}

		return countingStore, &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: countingStore}, chain
	}

	run := func(s *Server, txHashes []chainhash.Hash) error {
		_, err := s.processTxMetaUsingStore(ctx, txHashes, make([]metaSliceItem, len(txHashes)), map[uint32]bool{77: true}, true, false, true)
		return err
	}

	t.Run("one batched check", func(t *testing.T) {
		countingStore, s, chain := newFixture(t)

		require.NoError(t, run(s, chain))

		// c = 6: one decorate read per tx, the batched check's four (see the
		// cached-path scaling test), and one of headroom. The per-transaction
		// check needed ~N^2/2.
		reads := countingStore.reads.Load()
		t.Logf("%d store reads for a %d-transaction conflicting chain", reads, chainLength)
		require.LessOrEqual(t, reads, int64(6*chainLength))
	})

	t.Run("a genuine conflict is tx-invalid", func(t *testing.T) {
		_, s, chain := newFixture(t, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100}))

		require.ErrorIs(t, run(s, chain), errors.ErrTxInvalid)
	})

	t.Run("an underivable tx is still a retryable failure", func(t *testing.T) {
		countingStore, s, chain := newFixture(t)

		// chain[1]'s parent, chain[0], is gone; chain[0] is not asked about
		require.NoError(t, countingStore.Delete(ctx, &chain[0]))

		err := run(s, chain[1:])
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)
	})
}

// The per-transaction check tests a frozen output against
// CoinbasePlaceholderHashValue in some positions and FrozenBytesTxHash in others;
// the batch uses one of them per position. That is parity only while go-subtree
// defines both as the same hash. If they ever diverge, this fails, and both
// checks have to be revisited position by position.
func Test_frozenSentinelsAreOneValue(t *testing.T) {
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, subtreepkg.FrozenBytesTxHash)
}

// The verdict names what condemned the block. A mined direct counter-spender is
// named with the conflicting tx it was spent against, so an operator can see
// which cached entry led to the rejection. A mined tx deeper in a counter-spender
// cone is named alone: the walk does not record which root reached it.
func Test_checkCounterConflictingOnCurrentChainBatch_namesTheConflict(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})

	t.Run("direct counter-spender", func(t *testing.T) {
		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false, mined)
		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.Contains(t, err.Error(), chain[0].String())
		require.Contains(t, err.Error(), counter.TxIDChainHash().String())
	})

	t.Run("deep in a counter-spender cone", func(t *testing.T) {
		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false)

		counterChild := spendingChild(t, counter, 0)
		spendAndCreate(t, ctx, store, counterChild, mined)

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.Contains(t, err.Error(), counterChild.TxIDChainHash().String())
		require.NotContains(t, err.Error(), chain[0].String())
	})
}

// inpointlessRecordStore wraps a real utxo.Store and hands back the records in
// stripped the way the utxoset store does below the checkpoint: no stored
// inpoints, and with noBody no transaction body either.
type inpointlessRecordStore struct {
	utxo.Store
	stripped map[chainhash.Hash]struct{}
	noBody   bool
}

func (s *inpointlessRecordStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	txMeta, err := s.Store.Get(ctx, hash, f...)
	if err != nil || txMeta == nil {
		return txMeta, err
	}

	if _, ok := s.stripped[*hash]; ok {
		txMeta.TxInpoints = subtreepkg.TxInpoints{}

		if s.noBody {
			txMeta.Tx = nil
		}
	}

	return txMeta, nil
}

// bodyBarrierStore is an inpointlessRecordStore whose body reads (fields.Tx alone)
// each wait until want of them are in flight, or fail after a few seconds.
type bodyBarrierStore struct {
	inpointlessRecordStore
	want      int32
	bodyReads atomic.Int32
	arrived   chan struct{}
}

func (s *bodyBarrierStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if len(f) == 1 && f[0] == fields.Tx {
		if s.bodyReads.Add(1) == s.want {
			close(s.arrived)
		}

		select {
		case <-s.arrived:
		case <-time.After(3 * time.Second):
			return nil, errors.NewStorageError("body read %s waited alone: bodies are read one by one", hash.String())
		}
	}

	return s.inpointlessRecordStore.Get(ctx, hash, f...)
}

// Test_checkCounterConflictingOnCurrentChainBatch_recordWithoutInpoints pins the
// batch to utxo.GetCounterConflictingTxHashes's reading of a record that stores
// no inpoints: what it spends comes from its body, and a record carrying neither
// is a processing error. Reading the empty inpoints as "spends nothing" finds no
// counter-spender, so a block whose conflicting transaction lost to a mined
// counter-spender would pass.
func Test_checkCounterConflictingOnCurrentChainBatch_recordWithoutInpoints(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})

	t.Run("counter-spenders come from the body", func(t *testing.T) {
		sqlStore := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, sqlStore, 1, false, mined)

		store := &inpointlessRecordStore{Store: sqlStore, stripped: map[chainhash.Hash]struct{}{chain[0]: {}}}
		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		_, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, chain, map[uint32]bool{77: true}, failOnMissingRecords)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.Contains(t, err.Error(), counter.TxIDChainHash().String())
	})

	// Every body is read in one concurrent pass. The store holds each body read
	// until all of them have arrived, so a serial read per record times out and the
	// check fails with a processing error instead of the tx-invalid verdict.
	t.Run("bodies are read together, not one by one", func(t *testing.T) {
		sqlStore := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, sqlStore, 3, false, mined)

		stripped := make(map[chainhash.Hash]struct{}, len(chain))
		for _, h := range chain {
			stripped[h] = struct{}{}
		}

		store := &bodyBarrierStore{
			inpointlessRecordStore: inpointlessRecordStore{Store: sqlStore, stripped: stripped},
			want:                   int32(len(chain)), //nolint:gosec // a test chain length fits
			arrived:                make(chan struct{}),
		}
		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		_, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, chain, map[uint32]bool{77: true}, failOnMissingRecords)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.Contains(t, err.Error(), counter.TxIDChainHash().String())
		require.Equal(t, int32(len(chain)), store.bodyReads.Load(), "one body read per record") //nolint:gosec // a test chain length fits
	})

	t.Run("neither inpoints nor body is a processing error", func(t *testing.T) {
		sqlStore := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, sqlStore, 1, false, mined)

		store := &inpointlessRecordStore{Store: sqlStore, stripped: map[chainhash.Hash]struct{}{chain[0]: {}}, noBody: true}
		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		_, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, chain, map[uint32]bool{77: true}, failOnMissingRecords)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)
	})
}
