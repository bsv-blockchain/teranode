package postgres

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

func TestDAHSchemaObjectsExist(t *testing.T) {
	store, ctx := setupTestStore(t)

	for _, q := range []struct{ name, sql string }{
		{"spends.spent_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='spends_p00' AND column_name='spent_at_height'`},
		// mined_at_height stays a plain column (read by hash in the sweep's GREATEST
		// formula); it is deliberately NOT indexed (uncorrelated → see schema.go).
		{"txs.mined_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='txs_p00' AND column_name='mined_at_height'`},
		// The dead spends spent_at_height BRIN was dropped (commit ecdc43b4f): it only
		// served the removed O(table) backstop, so there must be NO such index.
		{"no spends spent_at_height brin", `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_spent_at_height_brin')`},
		// Plain (spent_at_height) btree drives the band enumeration (ordered range scan,
		// no spill). It replaced the covering (spent_at_height, prev_tx_hash) INCLUDE
		// (prev_output_idx) form: under early-DAH churn the VM is never all-visible so the
		// covering index-only scan heap-fetched anyway (measured 91% on live mainnet) — 60
		// GB for no index-only benefit. Both the covering and old two-column forms must be
		// gone. See the regime-change note in schema.go.
		{"height btree spends", `SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_height_btree'`},
		{"no covering INCLUDE spends btree", `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_h_hash_oidx_btree')`},
		{"no old two-column spends btree", `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_h_hash_btree')`},
		// The spends-driven sweep no longer scans txs by mined_at_height, so there must
		// be NO index on it (a btree would hurt the hot mine UPDATE's HOT ratio).
		{"no txs mined_at_height index", `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname LIKE 'txs_p00_mined_at_height%')`},
		// pending_deletes is now the only pruner path (always-on): the txs
		// px_delete_at_height BRIN is always backfilled into it and dropped.
		{"no txs px_delete_at_height brin", `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`},
		{"pending_deletes leaf", `SELECT 1 FROM pg_class WHERE relname='pending_deletes_p00' AND relkind='r'`},
		{"pending_deletes dah btree", `SELECT 1 FROM pg_indexes WHERE indexname='px_pd_dah_p00'`},
		// pending_unmined side-table is also always-on (maintained by the projection).
		{"pending_unmined leaf", `SELECT 1 FROM pg_class WHERE relname='pending_unmined_p00' AND relkind='r'`},
		{"pending_unmined since btree", `SELECT 1 FROM pg_indexes WHERE indexname='px_pu_since_p00'`},
		{"dah_watermark table", `SELECT 1 FROM information_schema.tables WHERE table_name='dah_watermark'`},
		{"dah_watermark seed row", `SELECT last_swept_height FROM dah_watermark WHERE id = 1`},
	} {
		var ok int
		err := store.pool.QueryRow(ctx, q.sql).Scan(&ok)
		require.NoError(t, err, "missing schema object: %s", q.name)
	}
}

// newMinedSingleOutputTx creates a transaction and stores it pre-mined via
// store.Create with utxo.WithMinedBlockInfo at the given height. The tx is the
// canonical testExtendedTx, which has exactly two spendable P2PKH outputs (no
// OP_RETURN / unspendable outputs). Paired with spendAllOutputs, the parent is
// left genuinely fully spent (count(spends) == count(outputs)), which is what
// Task 4's Worker 2 sweep relies on to stamp delete_at_height.
func newMinedSingleOutputTx(t *testing.T, store *Store, height uint32) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        uint32(height),
		BlockHeight:    height,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := store.Create(ctx, tx, height, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	return tx
}

// spendAllOutputs builds a child tx that spends every spendable output of
// parentTx, creates it in the store, then calls store.Spend at spendHeight.
// It self-checks that the parent is genuinely fully spent afterwards so that
// downstream DAH-sweep tests have a correct foundation.
func spendAllOutputs(t *testing.T, store *Store, parentTx *bt.Tx, spendHeight uint32) {
	t.Helper()
	ctx := context.Background()

	vouts := make([]uint32, 0, len(parentTx.Outputs))
	for i, out := range parentTx.Outputs {
		if out == nil {
			continue
		}
		vouts = append(vouts, uint32(i))
	}
	require.NotEmpty(t, vouts, "parent tx must have at least one spendable output")

	child := getSpendingTx(t, parentTx, vouts...)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)

	// Self-check: the parent must now be fully spent.
	// Spendable-output count is the stored txs.spendable_count scalar.
	parentHash := parentTx.TxIDChainHash()[:]
	var spendCount, outputCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1`, parentHash).Scan(&spendCount))
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spendable_count FROM txs WHERE hash=$1`,
		parentHash).Scan(&outputCount))
	require.Equal(t, outputCount, spendCount,
		"parent must be fully spent (count(spends) == count(spendable outputs))")
}

// newUnminedSingleOutputTx creates a transaction WITHOUT mined info (unmined_since set,
// block_ids NULL). Uses testExtendedTx and stores it at the given createHeight.
func newUnminedSingleOutputTx(t *testing.T, store *Store) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 10) // create at height 10, no WithMinedBlockInfo
	require.NoError(t, err)
	return tx
}

// mineTx marks tx as mined at minedHeight by calling SetMinedMulti.
// It sets the store's block height to minedHeight first so that mined_at_height is stamped correctly.
func mineTx(t *testing.T, store *Store, tx *bt.Tx, minedHeight uint32) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.SetBlockHeight(minedHeight))
	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:        minedHeight,
		BlockHeight:    minedHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
}

func TestSetMinedStampsFullySpentAndTagsHeight(t *testing.T) {
	// Design-C (S6), Setter-C reconcile (proc v15): when a tx is mined onto the longest
	// chain and is already fully spent (spent-before-mined ordering) — as evidenced by
	// the MAINTAINED spent_bits bitmap (folded by the background sweep) — then
	// SetMinedMulti must stamp delete_at_height directly from the bitmap AND record
	// mined_at_height. The mine path reads bit_count(spent_bits) = spendable_count rather
	// than re-aggregating the spends table, so the bitmap must be folded first (via the
	// sweep) — this is the division of labour: the sweep owns fully-spent detection,
	// the mine path only stamps a tx the bitmap already shows complete.
	store, ctx := setupTestStore(t)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUnminedSingleOutputTx(t, store)
	spendAllOutputs(t, store, tx, 50) // fully spent at height 50 while unmined

	// Fold the spends via the sweep so bit_count(spent_bits) reaches spendable_count
	// while the tx is still unmined (the proc folds but does not stamp — mined gate).
	require.NoError(t, store.SetBlockHeight(55))
	_, err := procSweepUpTo(store, ctx, 55)
	require.NoError(t, err)

	mineTx(t, store, tx, 60) // SetMinedMulti at height 60; GREATEST(lsh=50,60)=60

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "S6: set-mined must stamp delete_at_height for a fully-spent (counter-complete) tx")
	require.Equal(t, int64(60)+1+ret, *dah, "DAH = GREATEST(lastSpendHeight=50, minedHeight=60)+1+retention")

	var mh *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT mined_at_height FROM txs WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&mh))
	require.NotNil(t, mh, "set-mined must tag mined_at_height")
}

func TestSpendTagsHeightAndDoesNotStampInline(t *testing.T) {
	store, ctx := setupTestStore(t)

	parent := newMinedSingleOutputTx(t, store, 100)
	spendAllOutputs(t, store, parent, 101)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "spend must not stamp delete_at_height inline")

	var h *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_at_height FROM spends WHERE prev_tx_hash=$1`, parent.TxIDChainHash()[:]).Scan(&h))
	require.NotNil(t, h)
	require.Equal(t, int64(101), *h)
}

// ---------------------------------------------------------------------------
// Task 4 helpers
// ---------------------------------------------------------------------------

// spendOneOutput spends a SINGLE output (vout) of an already-created parent:
// it builds a child via getSpendingTx, creates the child in the store, and
// calls store.Spend at the given height. Unlike spendAllOutputs it does NOT
// self-assert the parent is fully spent (used by the partial-spend test).
func spendOneOutput(t *testing.T, store *Store, parentTx *bt.Tx, vout uint32, height uint32) {
	t.Helper()
	ctx := context.Background()

	child := getSpendingTx(t, parentTx, vout)
	_, err := store.Create(ctx, child, height)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, height)
	require.NoError(t, err)
}

// unspendAll reverses every recorded spend of parentTx by reading the parent's
// spend rows and calling store.Unspend. Unspend is ownership-checked, so each
// reconstructed *utxo.Spend must carry the stored spending_data token (read back
// here), mirroring how a real reorg caller derives its spends.
func unspendAll(t *testing.T, store *Store, parentTx *bt.Tx) {
	t.Helper()
	ctx := context.Background()

	parentHash := parentTx.TxIDChainHash()
	rows, err := store.pool.Query(ctx,
		`SELECT prev_output_idx, spending_data FROM spends WHERE prev_tx_hash=$1`, parentHash[:])
	require.NoError(t, err)

	var spends []*utxo.Spend
	for rows.Next() {
		var vout int64
		var sdBytes []byte
		require.NoError(t, rows.Scan(&vout, &sdBytes))
		sd, sdErr := spendpkg.NewSpendingDataFromBytes(sdBytes)
		require.NoError(t, sdErr)
		spends = append(spends, &utxo.Spend{
			TxID:         parentHash,
			Vout:         uint32(vout),
			SpendingData: sd,
		})
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.NotEmpty(t, spends, "parent must have spend rows to unspend")

	require.NoError(t, store.Unspend(ctx, spends))
}

func TestSweepStampsFullySpentMinedParent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // pre-mined; 2 outputs
	spendAllOutputs(t, store, parent, 101)          // fully spent at height 101

	n, err := procSweepUpTo(store, ctx, 105)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah)
	// completion height = last spend height (101); pre-mined so mined_at_height is NULL.
	require.Equal(t, int64(101)+1+ret, *dah)
}

func TestSweepClearsDAHAfterUnspend(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	parent := newMinedSingleOutputTx(t, store, 100)
	spendAllOutputs(t, store, parent, 101)
	_, err := procSweepUpTo(store, ctx, 105)
	require.NoError(t, err)

	// Sweep must have stamped the fully-spent + mined parent.
	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "sweep must stamp fully-spent mined parent")

	// Unspend now clears DAH directly (Change 1): no second sweep needed.
	unspendAll(t, store, parent)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "Unspend must clear DAH directly, without a re-sweep")
}

func TestUnspendClearsDAH(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100) // pre-mined
	spendAllOutputs(t, store, parent, 101)          // fully spent at 101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before unspend")

	unspendAll(t, store, parent)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "Unspend must clear delete_at_height for affected parents")
}

// TestSweepIgnoresUnspendableOpReturnOutputs is a regression test for the
// pruning bug where a tx carrying an OP_RETURN / data output could never be
// stamped for deletion: the "fully spent" check compared count(spends) to
// count(all outputs), but an unspendable output never gets a spends row, so the
// equality could never hold. Outputs are now flagged spendable, and the check
// compares against spendable outputs only.
func TestSweepIgnoresUnspendableOpReturnOutputs(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// Parent = canonical 2 spendable outputs + an appended zero-value
	// OP_FALSE OP_RETURN data output (vout 2). The data output can never be spent.
	parent := testExtendedTx(t)
	parent.Outputs = append(parent.Outputs, &bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef}),
	})

	_, err := store.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	}))
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	// All three outputs are stored on the txs row, but the OP_RETURN one is
	// flagged unspendable (spendable_count excludes it).
	var spendableCount, totalCount int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT spendable_count, out_count
		FROM txs WHERE hash=$1`, parentHash[:]).Scan(&spendableCount, &totalCount))
	require.Equal(t, 3, totalCount, "all outputs must be stored")
	require.Equal(t, 2, spendableCount, "the OP_RETURN output must be flagged spendable=false")

	// Spend only the two spendable outputs (0 and 1); the OP_RETURN output remains.
	child := getSpendingTx(t, parent, 0, 1)
	_, err = store.Create(ctx, child, 101)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	n, err := procSweepUpTo(store, ctx, 105)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parentHash[:]).Scan(&dah))
	require.NotNil(t, dah,
		"a mined parent whose spendable outputs are all spent must be stamped even with an unspent OP_RETURN output")
	// completion height = last spend height (101); mined_at_height (100) is lower.
	require.Equal(t, int64(101)+1+ret, *dah)
}

func TestRewindDAHWatermark(t *testing.T) {
	store, ctx := setupTestStore(t)
	_, err := store.pool.Exec(ctx, `UPDATE dah_part_watermark SET last_swept_height = 500`)
	require.NoError(t, err)
	require.NoError(t, store.RewindDAHWatermark(ctx, 480))
	var h int64
	require.NoError(t, store.pool.QueryRow(ctx, `SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&h))
	require.Equal(t, int64(480), h)
	require.NoError(t, store.RewindDAHWatermark(ctx, 600)) // must NOT advance
	require.NoError(t, store.pool.QueryRow(ctx, `SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&h))
	require.Equal(t, int64(480), h)
}

func TestSweepSkipsPartiallySpentAndUnmined(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	parent := newMinedSingleOutputTx(t, store, 100) // 2 outputs
	spendOneOutput(t, store, parent, 0, 101)        // only 1 of 2 spent
	_, err := procSweepUpTo(store, ctx, 105)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "partially-spent tx must not be stamped")
}

func TestSweepDoesNotProcessAboveSafeTip(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	parent := newMinedSingleOutputTx(t, store, 100)
	spendAllOutputs(t, store, parent, 105) // fully spent at the tip

	_, err := procSweepUpTo(store, ctx, store.dahSafeTip(2)) // safeTip=103 < 105
	require.NoError(t, err)
	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "spends at the open tip must not be swept until below safe_tip")

	require.NoError(t, store.SetBlockHeight(110)) // tip advances; spend at 105 now below safeTip=108
	_, err = procSweepUpTo(store, ctx, store.dahSafeTip(2))
	require.NoError(t, err)
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah)
}

// newUniqueUnminedTx creates a fresh parentless transaction with randomised
// output satoshis so that every call produces a distinct txid. It has no
// inputs (fee = 0) and two P2PKH outputs. Use it instead of
// newUnminedSingleOutputTx when a test needs multiple independent parent txs
// in the same store (the canonical testExtendedTx always has the same txid
// and would cause a primary-key collision on the second insert).
func newUniqueUnminedTx(t *testing.T, store *Store) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := bt.NewTx()
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	_, err := store.Create(ctx, tx, 10) // unmined — no WithMinedBlockInfo
	require.NoError(t, err)
	return tx
}

func TestWorker2LoopStampsBacklog(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const N = 30
	for i := 0; i < N; i++ {
		p := newUniqueUnminedTx(t, store)
		mineTx(t, store, p, 100)
		spendAllOutputs(t, store, p, 101)
	}
	require.NoError(t, store.SetBlockHeight(110)) // tip above spends so safeTip>=101

	svc := &postgresPrunerService{store: store, logger: store.logger}
	svc.Start(ctx)

	require.Eventually(t, func() bool {
		var n int
		_ = store.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM txs WHERE delete_at_height IS NOT NULL`).Scan(&n)
		return n == N
	}, 10*time.Second, 100*time.Millisecond, "Worker 2 loop must stamp the backlog")
}

// TestWatermarkResumeAfterRestart documents and guards the crash-safety property
// of Worker 2's durable state: spent_at_height (in spends) and mined_at_height
// (in txs) are committed to durable tables before any sweep runs. If the process
// crashes between the spend commit and the Worker 2 sweep, a fresh sweep resuming
// from the persisted watermark (which starts at 0) will still find and stamp the
// parent — the height tags survive the simulated crash.
func TestWatermarkResumeAfterRestart(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	// Fully-spent mined parent, but DO NOT run the cursor (simulate a crash
	// between the spend commit and Worker 2 processing it).
	p := newMinedSingleOutputTx(t, store, 100)
	spendAllOutputs(t, store, p, 101)

	// Watermark is still at its seeded value; the height tags are durable.
	var wm int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT COALESCE(MIN(last_swept_height), 0) FROM dah_part_watermark`).Scan(&wm))
	require.Equal(t, int64(0), wm, "no sweep ran yet")

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, p.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "not yet stamped")

	// "Restart": a fresh sweep resuming from the persisted watermark still finds
	// and stamps the parent (the spent_at_height tag survived the simulated crash).
	_, err := procSweepUpTo(store, ctx, store.dahSafeTip(2))
	require.NoError(t, err)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, p.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "parent stamped after restart resume — crash-safe")
}

func TestDAHParityBothCompletionOrders(t *testing.T) {
	store, ctx := setupTestStore(t)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// Order 1 — mined FIRST, then fully spent later (completing event = spend).
	// create unmined, mine at 150, spend all at 160 → completion = max(160,150) = 160.
	a := newUniqueUnminedTx(t, store)
	mineTx(t, store, a, 150)
	spendAllOutputs(t, store, a, 160)

	// Order 2 — fully spent while UNMINED, then mined (completing event = set-mined).
	// create unmined, spend all at 150, mine at 160 → completion = max(150,160) = 160.
	b := newUniqueUnminedTx(t, store)
	spendAllOutputs(t, store, b, 150)
	mineTx(t, store, b, 160)

	// Sweep with the tip well above 160 so both are in-range and below safe_tip.
	// SetBlockHeight must come AFTER the final mineTx call (which itself calls
	// store.SetBlockHeight internally) so the sweep sees height=200.
	require.NoError(t, store.SetBlockHeight(200))
	_, err := procSweepUpTo(store, ctx, store.dahSafeTip(2))
	require.NoError(t, err)

	for name, tx := range map[string]*bt.Tx{"order1-spend-completes": a, "order2-mine-completes": b} {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&dah))
		require.NotNil(t, dah, "%s: must be stamped", name)
		require.Equal(t, int64(160)+1+ret, *dah, "%s: DAH must be completion(160)+1+retention", name)
	}
}

// TestSetMinedStampsAllOpReturnTxInline locks in DAH eligibility for a tx whose every
// output is non-spendable (all OP_RETURN): spendable_count=0, out_count>0. Such a tx is
// trivially "fully spent" (there are no spendable outputs to spend) and has NO spends,
// so the spends-driven sweep never sees it. It is therefore stamped INLINE at mine time
// (SetMinedMulti), at completion = mined height. The sweep is a no-op for it. Guards
// against a regression that would silently leak these forever now that the sweep no
// longer scans txs by mined_at_height.
func TestSetMinedStampsAllOpReturnTxInline(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := testExtendedTx(t)
	opReturn := bscript.NewFromBytes([]byte{0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef})
	tx.Outputs = []*bt.Output{
		{Satoshis: 0, LockingScript: opReturn},
		{Satoshis: 0, LockingScript: opReturn},
	}
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	var outCount, spendableCount int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT out_count, spendable_count FROM txs WHERE hash=$1`, h[:]).Scan(&outCount, &spendableCount))
	require.Equal(t, int32(2), outCount, "both outputs stored")
	require.Equal(t, int32(0), spendableCount, "no output is spendable")

	// Before mining: not stamped.
	var preDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, h[:]).Scan(&preDAH))
	require.Nil(t, preDAH, "unmined all-OP_RETURN tx must not be stamped")

	// Mine on the longest chain → SetMinedMulti stamps delete_at_height INLINE.
	mineTx(t, store, tx, 100)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, h[:]).Scan(&dah))
	require.NotNil(t, dah, "all-OP_RETURN tx (spendable_count=0, out_count>0) must be DAH-stamped inline at mine")
	require.Equal(t, int64(100)+1+ret, *dah, "inline DAH must be mined(100)+1+retention")

	// The spends-driven sweep is a no-op for it (no spends → never a candidate); the
	// stamp stays put.
	_, err = procSweepUpTo(store, ctx, 105)
	require.NoError(t, err)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, h[:]).Scan(&dah))
	require.Equal(t, int64(100)+1+ret, *dah, "sweep must not change the inline stamp")
}

// procSweepUpTo drives one server-side DAH sweep up to toH via the dah_sweep_batch
// procedure (installed by store.New -> createSchema) and returns the rows stamped,
// matching the signature of the removed Go sweepDAHUpTo so the DAH behaviour tests
// read unchanged. retention comes from settings, exactly as the procedure reads it.
func procSweepUpTo(store *Store, ctx context.Context, toH int64) (int, error) {
	retention := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta
	return int(store.sweepAllPartitionsOnce(ctx, toH, retention)), nil
}
