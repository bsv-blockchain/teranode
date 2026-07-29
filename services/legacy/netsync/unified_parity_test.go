package netsync

/*
SCOPE REDUCTION NOTE: Run B drives the same utxoStore.Create/Spend sequence
that blockvalidation.BlockValidation.createAndSpendUTXOsForBatch performs,
directly against a fresh sqlitememory store (without constructing a real
BlockValidation, whose unexported fields cannot be set from this package).
Full system parity — flag-off node vs flag-on node producing identical block
hashes, heights, and UTXO counts — is verified by the Step-4 live soak.

Known fidelity gaps in Run B (deferred to Step-4 live soak):
  - WithLocked is omitted: the real createAndSpendUTXOsForBatch creates UTXOs
    locked by default (quickValidateSkipsUtxoLock=false path); Run B calls
    Create without utxo.WithLocked, so locked-UTXO behaviour is not exercised.
  - SubtreeIdx per-slot is not exercised: Run B uses a single flat blockID per
    block and does not thread per-slot subtree indices through Create.
  - Subtree-file parity is not compared: Run B writes no subtree files; the
    two runs differ in what is written to the blob store.
*/

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// parityCorpus holds the three test blocks and enough metadata for the parity checks.
type parityCorpus struct {
	// blocks are the three bsvutil.Block instances at heights 500, 501, 502.
	blocks []*bsvutil.Block
	// heights are the integer heights for each block (parallel to blocks).
	heights []int32
	// nonCBHashes contains all non-coinbase tx hashes across all three blocks.
	nonCBHashes []chainhash.Hash
	// spentByLaterTx is the subset of nonCBHashes whose outputs are consumed by
	// a later transaction (tx_a, tx_b, tx_c).
	spentByLaterTx map[chainhash.Hash]bool
	// spenderOf maps each spent tx hash to the expected spender tx hash.
	// Used in cross-run spender-identity assertions.
	spenderOf map[chainhash.Hash]chainhash.Hash
}

// buildParityCorpus constructs the 3-block corpus:
//
//	Block 500: coinbase + tx_a (external input) + tx_b (external input)
//	Block 501: coinbase + tx_c (spends tx_a[0]) + tx_d (spends tx_c[0], same block)
//	Block 502: coinbase + tx_e (spends tx_b[0])
func buildParityCorpus(t *testing.T) *parityCorpus {
	t.Helper()

	makeCoinbase := func(height int32) *wire.MsgTx {
		cb := wire.NewMsgTx(1)
		cb.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
			SignatureScript:  []byte{byte(height & 0xff), 0x01}, //nolint:gosec // test coinbase script
			Sequence:         0xffffffff,
		})
		cb.AddTxOut(&wire.TxOut{Value: 5000, PkScript: []byte{0x76, 0xa9, 0x14}})
		return cb
	}

	makeTx := func(prevHash chainhash.Hash, prevIdx uint32, value int64, tag byte) *wire.MsgTx {
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prevHash, Index: prevIdx},
			SignatureScript:  []byte{0x00, tag},
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: value, PkScript: []byte{0x76, 0xa9, 0x14, tag}})
		return tx
	}

	// Block 1 (height 500): coinbase + tx_a + tx_b
	externalA := chainhash.Hash{0xde, 0xad, 0x01}
	externalB := chainhash.Hash{0xde, 0xad, 0x02}
	txA := makeTx(externalA, 0, 1000, 0xaa)
	txB := makeTx(externalB, 0, 1000, 0xbb)
	txAHash := txA.TxHash()
	txBHash := txB.TxHash()

	msgBlock1 := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
		Transactions: []*wire.MsgTx{makeCoinbase(500), txA, txB},
	}
	block1 := bsvutil.NewBlock(msgBlock1)
	block1.SetHeight(500)

	// Block 2 (height 501): coinbase + tx_c (spends tx_a[0]) + tx_d (spends tx_c[0])
	txC := makeTx(txAHash, 0, 999, 0xcc)
	txCHash := txC.TxHash()
	txD := makeTx(txCHash, 0, 998, 0xdd)

	msgBlock2 := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now().Add(time.Minute), Bits: 0x1d00ffff},
		Transactions: []*wire.MsgTx{makeCoinbase(501), txC, txD},
	}
	block2 := bsvutil.NewBlock(msgBlock2)
	block2.SetHeight(501)

	// Block 3 (height 502): coinbase + tx_e (spends tx_b[0])
	txE := makeTx(txBHash, 0, 999, 0xee)

	msgBlock3 := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now().Add(2 * time.Minute), Bits: 0x1d00ffff},
		Transactions: []*wire.MsgTx{makeCoinbase(502), txE},
	}
	block3 := bsvutil.NewBlock(msgBlock3)
	block3.SetHeight(502)

	txDHash := txD.TxHash()
	txEHash := txE.TxHash()

	corpus := &parityCorpus{
		blocks:  []*bsvutil.Block{block1, block2, block3},
		heights: []int32{500, 501, 502},
		nonCBHashes: []chainhash.Hash{
			txAHash, txBHash, // block 1
			txCHash, txDHash, // block 2
			txEHash, // block 3
		},
		spentByLaterTx: map[chainhash.Hash]bool{
			txAHash: true, // consumed by tx_c in block 2
			txBHash: true, // consumed by tx_e in block 3
			txCHash: true, // consumed by tx_d in same block 2
		},
		spenderOf: map[chainhash.Hash]chainhash.Hash{
			txAHash: txCHash, // tx_c spends tx_a[0]
			txBHash: txEHash, // tx_e spends tx_b[0]
			txCHash: txDHash, // tx_d spends tx_c[0]
		},
	}
	return corpus
}

// makeSpendValidator returns a MockValidator whose ValidateFunc calls store.Spend
// for each non-coinbase tx, mirroring what PreValidateTransactions asks the real
// validator to do with WithSkipUtxoCreation=true + WithOutpointOnlySpend=true.
func makeSpendValidator(store utxo.Store) *validator.MockValidator {
	mv := &validator.MockValidator{}
	mv.ValidateFunc = func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
		if tx.IsCoinbase() {
			return &meta.Data{}, nil
		}
		if _, _, err := store.SpendAndCreate(ctx, tx, 500, utxo.WithSpendOnly(), utxo.WithSkipUTXOHashCheck(true), utxo.WithIgnoreLocked(true)); err != nil {
			return nil, err
		}
		return &meta.Data{}, nil
	}
	return mv
}

// runInlinePipeline drives Run A: calls prepareSubtrees for each block in the
// corpus, using the inline route (LegacyUnifiedBelowCheckpoint=false). The
// blockchain mock assigns block IDs 101, 102, 103 for each block in order.
func runInlinePipeline(t *testing.T, ctx context.Context, corpus *parityCorpus, storeA utxo.Store, subtreeStore *memory.Memory) {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = false

	// Override the store URL to use a unique DB name for Run A.
	u, err := url.Parse("sqlitememory:///parity_run_a")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	mockBC := &blockchain.Mock{}
	mockBC.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(101), nil).Once()
	mockBC.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(102), nil).Once()
	mockBC.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(103), nil).Once()

	sm := &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		utxoStore:        storeA,
		blockchainClient: mockBC,
		validationClient: makeSpendValidator(storeA),
		subtreeStore:     subtreeStore,
		ctx:              ctx,
	}

	for _, block := range corpus.blocks {
		_, _, _, prepErr := sm.prepareSubtrees(ctx, block)
		require.NoError(t, prepErr, "Run A prepareSubtrees failed at height %d", block.Height())
	}
}

// runDirectCreateSpend drives Run B: for each non-coinbase tx in the corpus,
// calls storeB.Create then storeB.Spend directly, mirroring the
// createAndSpendUTXOsForBatch sequence. Block IDs 201, 202, 203 are assigned
// in order.
func runDirectCreateSpend(t *testing.T, ctx context.Context, corpus *parityCorpus, storeB utxo.Store) {
	t.Helper()

	blockIDs := []uint32{201, 202, 203}

	for i, block := range corpus.blocks {
		height := uint32(corpus.heights[i]) //nolint:gosec // test heights are small
		blockID := blockIDs[i]

		txs := block.Transactions()

		// Phase 1: Create all non-coinbase txs (skip index 0 = coinbase).
		for _, bsvTx := range txs[1:] {
			btTx, convErr := wireMsgTxToBt(t, bsvTx.MsgTx())
			require.NoError(t, convErr, "Run B: wire→bt.Tx conversion failed")
			_, _, err := storeB.SpendAndCreate(ctx, btTx, height,
				utxo.WithSkipExtendedInputs(true),
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
					BlockID:     blockID,
					BlockHeight: height,
				}),
				utxo.WithCreateOnly(),
			)
			require.NoError(t, err, "Run B Create failed for tx in block %d", height)
		}

		// Phase 2: Spend all non-coinbase txs (the Spend call marks each tx's
		// inputs' parents as spent).
		for _, bsvTx := range txs[1:] {
			btTx, convErr := wireMsgTxToBt(t, bsvTx.MsgTx())
			require.NoError(t, convErr, "Run B: wire→bt.Tx conversion failed")
			_, _, err := storeB.SpendAndCreate(ctx, btTx, height,
				utxo.WithSpendOnly(), utxo.WithSkipUTXOHashCheck(true), utxo.WithIgnoreLocked(true))
			require.NoError(t, err, "Run B Spend failed for tx in block %d", height)
		}
	}
}

// compareStores checks that storeA (inline) and storeB (direct) hold equivalent
// UTXO state for all non-coinbase txs in the corpus.
func compareStores(
	t *testing.T,
	ctx context.Context,
	storeA, storeB utxo.Store,
	corpus *parityCorpus,
) {
	t.Helper()

	// 1. Existence + BlockIDs non-empty for every non-coinbase tx.
	for _, h := range corpus.nonCBHashes {
		h := h // capture

		mA, err := storeA.Get(ctx, &h, fields.BlockIDs)
		require.NoError(t, err, "Run A: Get failed for tx %s", h)
		require.NotNil(t, mA, "Run A: nil meta for tx %s", h)
		require.NotEmpty(t, mA.BlockIDs, "Run A: tx %s has no BlockIDs", h)

		mB, err := storeB.Get(ctx, &h, fields.BlockIDs)
		require.NoError(t, err, "Run B: Get failed for tx %s", h)
		require.NotNil(t, mB, "Run B: nil meta for tx %s", h)
		require.NotEmpty(t, mB.BlockIDs, "Run B: tx %s has no BlockIDs", h)
	}

	// 2. BlockID structural invariant: all txs in the same block share one
	// BlockID; different blocks produce different BlockIDs. Checked independently
	// for each run (exact IDs differ between runs A and B).
	checkBlockIDStructure := func(runName string, store utxo.Store, blockHashNonCB [3][]chainhash.Hash) {
		t.Helper()
		blockIDs := make([]uint32, 3)
		for bIdx, hashes := range blockHashNonCB {
			var blockID uint32
			for txIdx, h := range hashes {
				h := h
				m, err := store.Get(ctx, &h, fields.BlockIDs)
				require.NoError(t, err, "%s: Get failed for tx %s in block %d", runName, h, bIdx)
				require.NotNil(t, m, "%s: nil meta for tx %s", runName, h)
				require.NotEmpty(t, m.BlockIDs, "%s: tx %s has no BlockIDs", runName, h)

				if txIdx == 0 {
					blockID = m.BlockIDs[0]
				} else {
					require.Equal(t, blockID, m.BlockIDs[0],
						"%s: txs in block %d must share the same BlockID", runName, bIdx)
				}
			}
			blockIDs[bIdx] = blockID
		}
		// All three blocks must have distinct BlockIDs.
		require.NotEqual(t, blockIDs[0], blockIDs[1], "%s: block 0 and block 1 must have distinct BlockIDs", runName)
		require.NotEqual(t, blockIDs[1], blockIDs[2], "%s: block 1 and block 2 must have distinct BlockIDs", runName)
		require.NotEqual(t, blockIDs[0], blockIDs[2], "%s: block 0 and block 2 must have distinct BlockIDs", runName)
	}

	// Map corpus hashes by block: block1→[txA, txB], block2→[txC, txD], block3→[txE].
	// nonCBHashes order matches this layout (see buildParityCorpus).
	blockNonCB := [3][]chainhash.Hash{
		{corpus.nonCBHashes[0], corpus.nonCBHashes[1]}, // block 500: tx_a, tx_b
		{corpus.nonCBHashes[2], corpus.nonCBHashes[3]}, // block 501: tx_c, tx_d
		{corpus.nonCBHashes[4]},                        // block 502: tx_e
	}

	checkBlockIDStructure("Run A", storeA, blockNonCB)
	checkBlockIDStructure("Run B", storeB, blockNonCB)

	// 3. Spent-ness: for txs consumed by later txs, SpendingDatas must be non-empty.
	// SpendingDatas is only populated when fields.Utxos is explicitly requested.
	for h := range corpus.spentByLaterTx {
		h := h // capture

		mA, err := storeA.Get(ctx, &h, fields.Utxos)
		require.NoError(t, err, "Run A: Get(Utxos) failed for spent tx %s", h)
		require.NotNil(t, mA, "Run A: nil meta for spent tx %s", h)
		require.NotEmpty(t, mA.SpendingDatas, "Run A: spent tx %s must have SpendingDatas", h)
		require.NotNil(t, mA.SpendingDatas[0], "Run A: SpendingDatas[0] nil for spent tx %s", h)

		mB, err := storeB.Get(ctx, &h, fields.Utxos)
		require.NoError(t, err, "Run B: Get(Utxos) failed for spent tx %s", h)
		require.NotNil(t, mB, "Run B: nil meta for spent tx %s", h)
		require.NotEmpty(t, mB.SpendingDatas, "Run B: spent tx %s must have SpendingDatas", h)
		require.NotNil(t, mB.SpendingDatas[0], "Run B: SpendingDatas[0] nil for spent tx %s", h)
	}

	// 4. CROSS-RUN: block-group parity.
	//
	// Build a tx-hash → block-group-index map for each run by normalising each
	// run's numeric BlockIDs to their first-seen rank (0, 1, 2). The two maps
	// must be identical: the same set of tx hashes must land in group 0, 1, and
	// 2 in both runs. A unified route that groups txs into different blocks than
	// the inline route will fail here even if both passes internal consistency.
	blockGroupMap := func(runName string, store utxo.Store) map[chainhash.Hash]int {
		t.Helper()
		rankOf := make(map[uint32]int) // numeric BlockID → normalised rank
		result := make(map[chainhash.Hash]int, len(corpus.nonCBHashes))
		for _, h := range corpus.nonCBHashes {
			h := h
			m, err := store.Get(ctx, &h, fields.BlockIDs)
			require.NoError(t, err, "%s: Get(BlockIDs) failed for tx %s", runName, h)
			require.NotNil(t, m, "%s: nil meta for tx %s", runName, h)
			require.NotEmpty(t, m.BlockIDs, "%s: tx %s has no BlockIDs", runName, h)
			bid := m.BlockIDs[0]
			if _, seen := rankOf[bid]; !seen {
				rankOf[bid] = len(rankOf)
			}
			result[h] = rankOf[bid]
		}
		return result
	}

	groupA := blockGroupMap("Run A", storeA)
	groupB := blockGroupMap("Run B", storeB)
	require.Equal(t, groupA, groupB,
		"cross-run block-group mismatch: Run A and Run B assigned tx hashes to different block groups")

	// 5. CROSS-RUN: spender-identity parity.
	//
	// For each spent output, read SpendingDatas[0].TxID from both stores and
	// require that the spender tx-hash matches. The spender hashes are determined
	// by wire tx bytes (same input script, same outpoints → same txid) and are
	// therefore identical across runs despite differing numeric BlockIDs.
	// This catches wrong-spender attribution independent of block assignment.
	for spentHash, expectedSpender := range corpus.spenderOf {
		spentHash := spentHash             // capture
		expectedSpender := expectedSpender // capture

		mA, err := storeA.Get(ctx, &spentHash, fields.Utxos)
		require.NoError(t, err, "cross-run: Run A Get(Utxos) failed for spent tx %s", spentHash)
		require.NotNil(t, mA, "cross-run: Run A nil meta for spent tx %s", spentHash)
		require.GreaterOrEqual(t, len(mA.SpendingDatas), 1,
			"cross-run: Run A SpendingDatas empty for spent tx %s", spentHash)
		require.NotNil(t, mA.SpendingDatas[0], "cross-run: Run A SpendingDatas[0] nil for spent tx %s", spentHash)
		require.NotNil(t, mA.SpendingDatas[0].TxID, "cross-run: Run A SpendingDatas[0].TxID nil for spent tx %s", spentHash)

		mB, err := storeB.Get(ctx, &spentHash, fields.Utxos)
		require.NoError(t, err, "cross-run: Run B Get(Utxos) failed for spent tx %s", spentHash)
		require.NotNil(t, mB, "cross-run: Run B nil meta for spent tx %s", spentHash)
		require.GreaterOrEqual(t, len(mB.SpendingDatas), 1,
			"cross-run: Run B SpendingDatas empty for spent tx %s", spentHash)
		require.NotNil(t, mB.SpendingDatas[0], "cross-run: Run B SpendingDatas[0] nil for spent tx %s", spentHash)
		require.NotNil(t, mB.SpendingDatas[0].TxID, "cross-run: Run B SpendingDatas[0].TxID nil for spent tx %s", spentHash)

		require.Equal(t, expectedSpender, *mA.SpendingDatas[0].TxID,
			"cross-run: Run A wrong spender for tx %s (expected %s)", spentHash, expectedSpender)
		require.Equal(t, expectedSpender, *mB.SpendingDatas[0].TxID,
			"cross-run: Run B wrong spender for tx %s (expected %s)", spentHash, expectedSpender)
		require.Equal(t, *mA.SpendingDatas[0].TxID, *mB.SpendingDatas[0].TxID,
			"cross-run: spender mismatch for spent tx %s: Run A=%s Run B=%s",
			spentHash, mA.SpendingDatas[0].TxID, mB.SpendingDatas[0].TxID)
	}
}

// TestUnifiedRouteParity proves that the inline route (LegacyUnifiedBelowCheckpoint=false,
// Run A) and the direct Create+Spend sequence that blockvalidation performs in the unified
// route (Run B) produce equivalent UTXO-store state for a 3-block corpus spanning
// cross-block and same-block spend relationships.
func TestUnifiedRouteParity(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	corpus := buildParityCorpus(t)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Run A store: inline pipeline populates this.
	storeURLa, err := url.Parse("sqlitememory:///parity_run_a")
	require.NoError(t, err)
	storeA, err := utxosql.New(ctx, logger, tSettings, storeURLa)
	require.NoError(t, err)
	defer func() { _ = storeA.Close(ctx) }()

	// Run B store: direct Create+Spend sequence populates this.
	storeURLb, err := url.Parse("sqlitememory:///parity_run_b")
	require.NoError(t, err)
	storeB, err := utxosql.New(ctx, logger, tSettings, storeURLb)
	require.NoError(t, err)
	defer func() { _ = storeB.Close(ctx) }()

	// Shared subtree blob store (the inline pipeline writes subtree files here; Run B
	// does not need it but we provide one so RunA's subtreeStore field is non-nil).
	subtreeStore := memory.New()

	// Run A: drive the inline pipeline.
	runInlinePipeline(t, ctx, corpus, storeA, subtreeStore)

	// Run B: drive the direct Create+Spend sequence.
	runDirectCreateSpend(t, ctx, corpus, storeB)

	// Compare the two stores.
	compareStores(t, ctx, storeA, storeB, corpus)
}

// wireMsgTxToBt serialises a wire.MsgTx to bytes and decodes it as a bt.Tx,
// matching the approach used in handle_block.go for the coinbase tx.
func wireMsgTxToBt(t *testing.T, wireTx *wire.MsgTx) (*bt.Tx, error) {
	t.Helper()
	var buf bytes.Buffer
	if err := wireTx.Serialize(&buf); err != nil {
		return nil, err
	}
	return bt.NewTxFromBytes(buf.Bytes())
}
