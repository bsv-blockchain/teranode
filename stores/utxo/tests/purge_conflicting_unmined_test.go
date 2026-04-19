// Package tests provides tests for the RepairConflictingChains function.
package tests

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupSQLiteFileStore creates a file-based SQLite store in t.TempDir().
// File SQLite uses WAL mode which allows concurrent reads and writes —
// required by RepairConflictingChains because SetConflicting issues writes
// while other queries are running on separate connections.
func setupSQLiteFileStore(ctx context.Context, t *testing.T) utxo.Store {
	t.Helper()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	dbPath := filepath.Join(t.TempDir(), "repair-test.db")
	storeURL, err := url.Parse(fmt.Sprintf("sqlite:///%s", dbPath))
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// testBlockchainQuerier is a local implementation of utxo.BlockchainQuerier for testing.
type testBlockchainQuerier struct {
	bestBlockHash   *chainhash.Hash
	bestBlockHeight uint32
	blockHeaderIDs  []uint32
}

func (tbq *testBlockchainQuerier) GetBestBlockHeaderInfo(ctx context.Context) (utxo.BlockHeaderInfo, error) {
	return utxo.BlockHeaderInfo{Hash: tbq.bestBlockHash, Height: tbq.bestBlockHeight}, nil
}

func (tbq *testBlockchainQuerier) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return tbq.blockHeaderIDs, nil
}

// newQuerier creates a simple querier with no blocks on the best chain.
func newQuerier() *testBlockchainQuerier {
	h := &chainhash.Hash{}
	return &testBlockchainQuerier{
		bestBlockHash:   h,
		bestBlockHeight: 0,
		blockHeaderIDs:  []uint32{},
	}
}

// makeTxSpendingOutput builds a minimal transaction that spends parentTxHash[vout].
// satoshis is used to make the output unique (and thus the txid unique).
func makeTxSpendingOutput(t *testing.T, parentTx *bt.Tx, vout uint32, satoshis uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	err := tx.From(
		parentTx.TxIDChainHash().String(),
		vout,
		parentTx.Outputs[vout].LockingScript.String(),
		uint64(parentTx.Outputs[vout].Satoshis),
	)
	require.NoError(t, err)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis)
	require.NoError(t, err)
	return tx
}

// TestRepairConflictingChains_CleanState verifies that running repair on a clean store produces zeroed report.
func TestRepairConflictingChains_CleanState(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.CaseAFixed)
	require.Equal(t, 0, report.CaseCFixed)
	require.Equal(t, 0, report.UnminedSinceFixed)
}

// TestRepairConflictingChains_CaseA_SingleLoser sets up a double-spend scenario where
// TX_B is an unmined loser (not marked Conflicting) while TX_A is the winner per SpendingData.
// Repair must detect TX_B as Case A and mark it Conflicting=true.
func TestRepairConflictingChains_CaseA_SingleLoser(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	// Create TX_PARENT with a real, unique output.
	parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	// TX_A spends parentTx[0] — this sets SpendingData[0] = TX_A.
	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// TX_B also has an input pointing to parentTx[0], but we only Create() it (no Spend).
	// This means SpendingData[0] on parentTx still points to TX_A — TX_B is the loser but
	// was never marked Conflicting (simulating the bug).
	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	// Create with unmined_since > 0 (blockHeight 100 means it is unmined)
	_, err = store.Create(ctx, txB, 100)
	require.NoError(t, err)

	querier := newQuerier()
	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	// Non-fatal errors may occur when parentTx's input references an external tx not in the store.
	// The repair should still detect and fix TX_B.
	require.Equal(t, 1, report.CaseAFixed, "TX_B should be detected as Case A loser")

	// TX_B must now be marked Conflicting=true.
	txBHash := txB.TxIDChainHash()
	meta, err := store.Get(ctx, txBHash, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Conflicting, "TX_B must be Conflicting after repair")
}

// TestRepairConflictingChains_CaseA_CascadeToChildren verifies that MarkConflictingRecursively
// propagates the Conflicting mark through TX_B → TX_B_CHILD → TX_B_GRANDCHILD.
func TestRepairConflictingChains_CaseA_CascadeToChildren(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	// TX_PARENT
	parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	// TX_A wins parentTx[0].
	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// TX_B is the loser — only Created, not Spent against parentTx[0].
	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 100)
	require.NoError(t, err)

	// TX_B_CHILD spends TX_B[0] — Created and Spent to establish the parent relationship.
	txBChild := makeTxSpendingOutput(t, txB, 0, 5000)
	_, err = store.Create(ctx, txBChild, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txBChild, store.GetBlockHeight()+1, utxo.IgnoreFlags{
		IgnoreConflicting: true,
		IgnoreLocked:      true,
	})
	require.NoError(t, err)

	// TX_B_GRANDCHILD spends TX_B_CHILD[0].
	txBGrandchild := makeTxSpendingOutput(t, txBChild, 0, 2000)
	_, err = store.Create(ctx, txBGrandchild, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txBGrandchild, store.GetBlockHeight()+1, utxo.IgnoreFlags{
		IgnoreConflicting: true,
		IgnoreLocked:      true,
	})
	require.NoError(t, err)

	querier := newQuerier()
	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	// Non-fatal errors from external (unresolvable) parent inputs are expected; repair still runs.
	require.Equal(t, 1, report.CaseAFixed, "only the root loser TX_B is the Case A entry")

	// All three (TX_B, TX_B_CHILD, TX_B_GRANDCHILD) must be Conflicting.
	for _, tx := range []*bt.Tx{txB, txBChild, txBGrandchild} {
		h := tx.TxIDChainHash()
		m, gErr := store.Get(ctx, h, fields.Conflicting)
		require.NoError(t, gErr)
		require.NotNil(t, m)
		require.True(t, m.Conflicting, "tx %s must be Conflicting after cascade", h.String())
	}
}

// TestRepairConflictingChains_DryRun verifies that dryRun=true reports findings but writes nothing.
func TestRepairConflictingChains_DryRun(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	// Same setup as CaseA_SingleLoser.
	parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 100)
	require.NoError(t, err)

	querier := newQuerier()

	// Dry run — must report CaseA but not fix.
	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseAFixed, "dry run must still report CaseAFixed")

	// TX_B must NOT be marked Conflicting (nothing was written).
	txBHash := txB.TxIDChainHash()
	meta, err := store.Get(ctx, txBHash, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.False(t, meta.Conflicting, "TX_B must remain non-conflicting during dry run")
}

// TestRepairConflictingChains_UnminedSinceFix verifies step 0.
// SQL store returns nil for ScanInconsistentUnminedTxs, so step 0 is a no-op.
// The test verifies no error is returned and report.UnminedSinceFixed == 0.
func TestRepairConflictingChains_UnminedSinceFix(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteMemoryStore(ctx, t)

	// Create a transaction that is mined (has block_ids) — under normal SQL operation,
	// ScanInconsistentUnminedTxs returns nil, so no unmined_since fix is attempted.
	parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	// Create with block info (mined).
	_, err = store.Create(ctx, parentTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}),
	)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	// SQL store returns nil from ScanInconsistentUnminedTxs — step 0 is skipped.
	require.Equal(t, 0, report.UnminedSinceFixed)
}

// TestRepairConflictingChains_CaseC_InvertedWinnerLoser verifies that Case C (inverted winner/loser)
// is detected and repaired via ProcessConflicting.
//
// Setup:
//   - TX_A is in the unmined list; SpendingData says TX_A won PARENT[0].
//   - TX_B is confirmed on the best chain AND is in PARENT's ConflictingChildren
//     (i.e. SetConflicting(TX_B) was called previously, adding (PARENT, TX_B) to the DB).
//   - The bug: GetCounterConflictingTxHashes(TX_A) only traverses TX_A's own ConflictingChildren,
//     missing TX_B which is stored as (PARENT, TX_B). The fix queries parentMeta.ConflictingChildren.
//
// Expected: report.CaseCFixed == 1, TX_A becomes Conflicting=true after ProcessConflicting.
func TestRepairConflictingChains_CaseC_InvertedWinnerLoser(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	const bestChainBlockID uint32 = 42

	// rootTx is a known mined tx whose outputs we can spend.
	// It acts as the "genesis" for the test chain — stored in the DB as confirmed.
	// We use a hardcoded hex tx here; its own parents do not need to be in the store
	// because ProcessConflicting never traverses above parentTx.
	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// parentTx is built via makeTxSpendingOutput so its inputs carry a proper PreviousTxScript.
	// This is critical: ProcessConflicting calls SetConflicting([txA], true) which uses
	// UTXOHashFromInput on txA's inputs — txA.PreviousTxScript must be non-nil.
	// Similarly, updateParentConflictingChildren on txA requires parentTx to be in the store,
	// which it is. And parentTx.Inputs[0].PreviousTxID = rootTx.hash, which is also in the store.
	parentTx := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100) // unmined
	require.NoError(t, err)

	// TX_A: unmined, recorded as winner (SpendingData[0] = TX_A).
	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// TX_B: the real winner, confirmed on best chain.
	// Created with MinedBlockInfo so it has BlockIDs=[bestChainBlockID] and unmined_since=0.
	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: bestChainBlockID, BlockHeight: 1}),
	)
	require.NoError(t, err)

	// SetConflicting(TX_B, true) adds (PARENT, TX_B) to conflicting_children in the DB
	// and sets TX_B.Conflicting=true. This simulates the state after SetConflicting ran (as
	// part of detecting TX_B as a double-spend) but ProcessConflicting never completed — so
	// SpendingData still says TX_A is the winner.
	// TX_B must remain Conflicting=true: ProcessConflicting requires the winning tx to be
	// flagged before it can execute the 5-phase commit.
	txBHash := txB.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txBHash}, true)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{bestChainBlockID},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseCFixed, "Case C should be detected and fixed")
	require.Equal(t, 0, report.CaseAFixed, "TX_A's loser state is handled by ProcessConflicting in Case C, not Case A")

	// TX_A must now be Conflicting=true (ProcessConflicting flipped the roles).
	txAMeta, err := store.Get(ctx, txA.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, txAMeta)
	require.True(t, txAMeta.Conflicting, "TX_A must be marked Conflicting after Case C repair")
}

// TestRepairConflictingChains_CaseC_DryRun verifies that dryRun=true reports Case C without fixing.
func TestRepairConflictingChains_CaseC_DryRun(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	const bestChainBlockID uint32 = 43

	rootTx43, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx43, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)
	parentTx := makeTxSpendingOutput(t, rootTx43, 0, rootTx43.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: bestChainBlockID, BlockHeight: 1}),
	)
	require.NoError(t, err)
	txBHash := txB.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txBHash}, true)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{bestChainBlockID},
	}

	// dry-run: report but don't write
	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseCFixed, "Case C should be reported")

	// TX_A must still be Conflicting=false — nothing written.
	txAMeta, err := store.Get(ctx, txA.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, txAMeta.Conflicting, "dryRun must not modify TX_A")
}

// TestRepairConflictingChains_CaseASkippedAfterCaseC verifies that when a tx is detected as
// Case A but ProcessConflicting (Case C) already marked it Conflicting=true, it is skipped in
// the Case A sweep (freshMeta.Conflicting check at end of repair).
func TestRepairConflictingChains_CaseASkippedAfterCaseC(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	const bestChainBlockID uint32 = 44

	rootTx44, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx44, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)
	parentTx := makeTxSpendingOutput(t, rootTx44, 0, rootTx44.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	// TX_A: unmined, recorded winner.
	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// TX_B: confirmed on best chain, in PARENT's ConflictingChildren → Case C winner.
	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: bestChainBlockID, BlockHeight: 1}),
	)
	require.NoError(t, err)
	txBHash := txB.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txBHash}, true)
	require.NoError(t, err)

	// TX_A is also in caseALosers from some OTHER input perspective (simulate by marking
	// TX_A manually as a loser that would be in caseALosers, then verifying the re-check
	// skips it because Case C ProcessConflicting already set Conflicting=true on TX_A).
	//
	// In practice this is tested end-to-end: after Case C ProcessConflicting runs, TX_A
	// gets Conflicting=true. The Case A sweep re-checks freshMeta.Conflicting and skips it.
	// We verify the net result: report.CaseAFixed == 0 (TX_A not double-counted).

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{bestChainBlockID},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseCFixed)
	require.Equal(t, 0, report.CaseAFixed, "TX_A must not be double-counted: Case C ProcessConflicting already fixed it")
}

// TestRepairConflictingChains_CaseD_OrphanConflictingParent reproduces the production
// state seen on mainnet-eu-1: a parent tx is marked Conflicting=true with UnminedSince>0
// and its recorded spender (per SpendingData) is a non-conflicting unmined child, but the
// parent has no ConflictingChildren. The unmined iterator filters parent out
// (conflicting=true), so BlockAssembler.validateParentChain fails every restart with
// "parent is unmined but not in processing list". Case D detects this orphan mark (no real
// conflict evidence) and unmarks the parent so the iterator re-includes it.
func TestRepairConflictingChains_CaseD_OrphanConflictingParent(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	// rootTx is a known mined tx on the best chain so grandparent-SpendingData validation passes.
	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// parentTx spends rootTx[0] — Create+Spend so rootTx.SpendingData[0] = parentTx.
	parentTx := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100) // unmined
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// childTx spends parentTx[0] — Create+Spend so parentTx.SpendingData[0] = childTx.
	childTx := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, childTx, 100) // unmined
	require.NoError(t, err)
	_, err = store.Spend(ctx, childTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// The bug: parentTx is marked Conflicting=true with no real conflict (no sibling, no
	// ConflictingChildren). SetConflicting does not cascade, so childTx stays non-conflicting.
	parentHash := parentTx.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentHash}, true)
	require.NoError(t, err)

	// Sanity: parent is orphan-conflicting, child is not.
	parentMeta, err := store.Get(ctx, parentHash, fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)
	require.True(t, parentMeta.Conflicting, "precondition: parent must be Conflicting=true")
	require.Empty(t, parentMeta.ConflictingChildren, "precondition: parent has no ConflictingChildren")

	childMeta, err := store.Get(ctx, childTx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, childMeta.Conflicting, "precondition: child must be Conflicting=false")

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseDFixed, "parent orphan-conflicting must be detected and unmarked")
	require.Equal(t, 0, report.CaseAFixed, "child is the recorded winner; no Case A")
	require.Equal(t, 0, report.CaseCFixed, "no sibling on best chain; no Case C")

	// Parent must be Conflicting=false after repair.
	parentMeta, err = store.Get(ctx, parentHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, parentMeta.Conflicting, "parent must be unmarked after Case D repair")

	// Child must remain Conflicting=false — no cascade.
	childMeta, err = store.Get(ctx, childTx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, childMeta.Conflicting, "child must remain non-conflicting")
}

// TestRepairConflictingChains_CaseD_DryRun verifies dryRun reports Case D without writing.
func TestRepairConflictingChains_CaseD_DryRun(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	parentTx := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	childTx := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, childTx, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, childTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	parentHash := parentTx.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentHash}, true)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseDFixed, "dryRun must still report Case D")

	// Parent must remain Conflicting=true — dryRun writes nothing.
	parentMeta, err := store.Get(ctx, parentHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, parentMeta.Conflicting, "dryRun must not unmark parent")
}

// TestRepairConflictingChains_CaseD_LegitConflictNotUnmarked verifies the safety check:
// a parent that is legitimately conflicting (its own input was won by a different tx per
// grandparent's SpendingData) must NOT be unmarked by Case D even if it otherwise looks
// like an orphan (no ConflictingChildren, recorded spender is non-conflicting).
func TestRepairConflictingChains_CaseD_LegitConflictNotUnmarked(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// parentWinner spends rootTx[0] — Create+Spend so rootTx.SpendingData[0] = parentWinner.
	// This parent is the true spender of rootTx[0].
	parentWinner := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/3)
	_, err = store.Create(ctx, parentWinner, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentWinner, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// parentLoser also claims rootTx[0] but loses — Create only (no Spend), so
	// rootTx.SpendingData[0] still equals parentWinner.
	parentLoser := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/4)
	_, err = store.Create(ctx, parentLoser, 100)
	require.NoError(t, err)

	// childOfLoser spends parentLoser[0] — Create+Spend so parentLoser.SpendingData[0] = childOfLoser.
	// childOfLoser is non-conflicting for the Case D scan to pick up parentLoser as a candidate.
	childOfLoser := makeTxSpendingOutput(t, parentLoser, 0, 5000)
	_, err = store.Create(ctx, childOfLoser, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, childOfLoser, store.GetBlockHeight()+1, utxo.IgnoreFlags{
		IgnoreConflicting: true,
	})
	require.NoError(t, err)

	// Mark parentLoser Conflicting=true. Unlike the orphan case, this mark is correct:
	// rootTx.SpendingData[0] = parentWinner, so parentLoser IS a legit loser.
	parentLoserHash := parentLoser.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentLoserHash}, true)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	// Pre-check: verify parentLoser.SpendingData[0] actually records childOfLoser.
	// If Spend didn't record it, the cascade has nothing to follow.
	preCheck, err := store.Get(ctx, parentLoserHash, fields.Utxos)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(preCheck.SpendingDatas), 1)
	require.NotNil(t, preCheck.SpendingDatas[0], "precondition: parentLoser.SpendingDatas[0] must be set")
	require.NotNil(t, preCheck.SpendingDatas[0].TxID)
	require.Equal(t, *childOfLoser.TxIDChainHash(), *preCheck.SpendingDatas[0].TxID,
		"precondition: parentLoser.SpendingDatas[0] must name childOfLoser")

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.CaseDFixed, "legit conflicting parent must NOT be unmarked by Case D")
	require.Equal(t, 1, report.CaseDCascaded, "legit conflicting parent with non-conflicting child must cascade")

	// parentLoser must stay Conflicting=true — grandparent evidence showed it was a legit loser.
	parentMeta, err := store.Get(ctx, parentLoserHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, parentMeta.Conflicting, "legit conflicting parent must remain marked")

	// childOfLoser must now be Conflicting=true — cascaded from parentLoser.
	childMeta, err := store.Get(ctx, childOfLoser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, childMeta.Conflicting, "child of legit conflicting parent must be cascaded to Conflicting=true")
}

// TestRepairConflictingChains_CaseD_ChainedOrphans verifies the iterative worklist:
// two orphan-conflicting ancestors stacked — grandparent is ALSO orphan-conflicting,
// with grandparent.ConflictingChildren still listing parent (stale back-reference from
// an earlier SetConflicting cascade). After parent is unmarked, grandparent must also
// be detected and unmarked in the same repair run.
func TestRepairConflictingChains_CaseD_ChainedOrphans(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// Chain: grandparent → parent → child. Only child is non-conflicting.
	grandparent := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, grandparent, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, grandparent, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	parent := makeTxSpendingOutput(t, grandparent, 0, grandparent.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parent, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parent, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	child := makeTxSpendingOutput(t, parent, 0, 5000)
	_, err = store.Create(ctx, child, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// Mark grandparent conflicting FIRST — this adds grandparent to rootTx.ConflictingChildren
	// but leaves parent/child untouched.
	gpHash := grandparent.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*gpHash}, true)
	require.NoError(t, err)

	// Then mark parent conflicting separately — this adds parent to grandparent.ConflictingChildren.
	// Grandparent is now orphan-conflicting with a stale entry pointing at parent (once parent
	// is itself unmarked, grandparent's conflictingCs will reference a non-conflicting tx).
	pHash := parent.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*pHash}, true)
	require.NoError(t, err)

	// Sanity: both ancestors conflicting, child is not.
	pMeta, err := store.Get(ctx, pHash, fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)
	require.True(t, pMeta.Conflicting)

	gpMeta, err := store.Get(ctx, gpHash, fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)
	require.True(t, gpMeta.Conflicting)
	require.Contains(t, gpMeta.ConflictingChildren, *pHash, "grandparent must list parent in its ConflictingChildren")

	childMeta, err := store.Get(ctx, child.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, childMeta.Conflicting)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, report.CaseDFixed, "both parent and grandparent must be unmarked in one run")

	// Parent must be Conflicting=false.
	pMeta, err = store.Get(ctx, pHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, pMeta.Conflicting, "parent must be unmarked after Case D")

	// Grandparent must also be Conflicting=false — discovered via chase-up.
	gpMeta, err = store.Get(ctx, gpHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, gpMeta.Conflicting, "grandparent must be unmarked after chained Case D")

	// Child remains non-conflicting.
	childMeta, err = store.Get(ctx, child.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, childMeta.Conflicting)
}

// TestRepairConflictingChains_CaseD_LegitCascadeWithNilParentSD reproduces the mainnet
// pathology where a legit-losing parent has no SpendingDatas populated (spentUtxos=0)
// because it was already Conflicting=true at the time its child ran Spend, and some
// code paths skip the SD write. cascadeConflictingViaSpendingData(parent) walks
// parent.SD and finds zero descendants, so the real child stays visible to the unmined
// iterator and validateParentChain keeps tripping across restarts.
//
// The fix: step 1 records the direct children that spend each orphan-candidate parent.
// If the parent turns out to be a legit loser, the cascade uses those direct children
// as additional seeds — their own SpendingDatas are properly populated.
func TestRepairConflictingChains_CaseD_LegitCascadeWithNilParentSD(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// parentWinner legitimately spends rootTx[0] — Create+Spend populates rootTx.SD[0].
	parentWinner := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/3)
	_, err = store.Create(ctx, parentWinner, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentWinner, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// parentLoser also claims rootTx[0] — Create only, no Spend. rootTx.SD[0] stays = parentWinner.
	parentLoser := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/4)
	_, err = store.Create(ctx, parentLoser, 100)
	require.NoError(t, err)

	// Mark parentLoser conflicting — this is the ACTUAL conflict; parentLoser is a real
	// double-spend loser of rootTx[0].
	parentLoserHash := parentLoser.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentLoserHash}, true)
	require.NoError(t, err)

	// childOfLoser is an unmined non-conflicting tx whose input names parentLoser[0].
	// Critically we do NOT call Spend — so parentLoser.SD[0] stays nil, mirroring the
	// mainnet state where spentUtxos=0 on a conflicting parent whose children never
	// recorded their spend on the parent side.
	childOfLoser := makeTxSpendingOutput(t, parentLoser, 0, 5000)
	_, err = store.Create(ctx, childOfLoser, 100)
	require.NoError(t, err)

	// Precondition: parentLoser has nil SpendingDatas for all outputs (no Spend ran).
	preCheck, err := store.Get(ctx, parentLoserHash, fields.Utxos)
	require.NoError(t, err)
	for i, sd := range preCheck.SpendingDatas {
		require.Nil(t, sd, "precondition: parentLoser.SpendingDatas[%d] must be nil", i)
	}

	// childOfLoser must start non-conflicting (otherwise step 1's iterator skips it).
	childHash := childOfLoser.TxIDChainHash()
	cMeta, err := store.Get(ctx, childHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, cMeta.Conflicting, "precondition: childOfLoser starts non-conflicting")

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.CaseDFixed, "legit loser must NOT be unmarked")
	require.Equal(t, 1, report.CaseDCascaded, "legit loser with nil SD must still cascade via tracked direct children")

	// parentLoser stays Conflicting=true.
	pMeta, err := store.Get(ctx, parentLoserHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, pMeta.Conflicting, "legit conflicting parent must remain marked")

	// childOfLoser must now be Conflicting=true — cascaded from the direct-children seed.
	// Without the fix cascadeConflictingViaSpendingData walks parent.SD (all nil) and
	// finds zero descendants, so childOfLoser would stay non-conflicting.
	cMeta, err = store.Get(ctx, childHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, cMeta.Conflicting, "direct child must be cascaded even when parent.SD is empty")
}

// TestRepairConflictingChains_CaseC_StaleSiblingSkipped verifies that a ConflictingChildren
// entry which is on the best chain but no longer Conflicting=true is skipped, not enqueued
// as a Case C winner. Previously such an entry would cause ProcessConflicting to return
// "tx is not conflicting" and — now that DB errors are fatal — abort the whole repair.
//
// This happens when a tx was SetConflicting(true) earlier (adding it to a parent's
// conflictingCs list) and then SetConflicting(false) later. The SQL
// updateParentConflictingChildren helper only ever INSERTs — it never deletes — so the
// stale back-reference lingers even after the child is unmarked.
func TestRepairConflictingChains_CaseC_StaleSiblingSkipped(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	const bestChainBlockID uint32 = 50

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	parentTx := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	_, err = store.Create(ctx, parentTx, 100)
	require.NoError(t, err)

	// txA is the unmined non-conflicting spender of parentTx[0].
	txA := makeTxSpendingOutput(t, parentTx, 0, 10000)
	_, err = store.Create(ctx, txA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, txA, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// txB is on the best chain but not currently Conflicting=true — it WAS marked
	// conflicting at some point (adding it to parentTx.conflictingCs) and later unmarked.
	// SetConflicting(false) does not remove the back-reference, so the entry lingers.
	txB := makeTxSpendingOutput(t, parentTx, 0, 20000)
	_, err = store.Create(ctx, txB, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: bestChainBlockID, BlockHeight: 1}),
	)
	require.NoError(t, err)
	txBHash := txB.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txBHash}, true)
	require.NoError(t, err)
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txBHash}, false)
	require.NoError(t, err)

	// Sanity: parentTx.ConflictingChildren still lists txB, but txB.Conflicting=false.
	pMeta, err := store.Get(ctx, parentTx.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Contains(t, pMeta.ConflictingChildren, *txBHash, "stale back-reference must still be present")
	bMeta, err := store.Get(ctx, txBHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, bMeta.Conflicting, "txB is not currently Conflicting=true")

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{bestChainBlockID},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err, "stale non-conflicting sibling must not cause a fatal error")
	require.Equal(t, 0, report.CaseCFixed, "stale sibling must NOT be enqueued as Case C winner")
}

// TestRepairConflictingChains_CaseD_OrphanBlocksParentDetection reproduces the mainnet
// pathology where a parent's ConflictingChildren list references a child that is itself
// orphan-conflicting (no credible reason to be Conflicting=true). The orphan child can
// never be reached via the unmined iterator (conflicting filter hides it) and its mere
// presence in the list blocks the parent from being classified as a Case D orphan. The
// classifyConflictingChildren helper recurses one level to detect this and flags both
// the parent and the orphan child for unmarking in the same repair pass.
func TestRepairConflictingChains_CaseD_OrphanBlocksParentDetection(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	// parentX spends rootTx[0] and has (at least) two outputs: [0] will be spent by
	// the blocker, [1] will be spent by goodChild.
	parentX := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/2)
	// Second output so we can spend a different vout with goodChild. PayToAddress adds
	// another output — output[0] is the first PayToAddress, output[1] is the change.
	err = parentX.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", rootTx.Outputs[0].Satoshis/4)
	require.NoError(t, err)
	_, err = store.Create(ctx, parentX, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentX, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// blocker spends parentX[0] — Create+Spend so parentX.SpendingDatas[0] = blocker.
	blocker := makeTxSpendingOutput(t, parentX, 0, 5000)
	_, err = store.Create(ctx, blocker, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, blocker, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// goodChild spends parentX[1] — Create+Spend so parentX.SpendingDatas[1] = goodChild.
	goodChild := makeTxSpendingOutput(t, parentX, 1, 7000)
	_, err = store.Create(ctx, goodChild, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, goodChild, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	// Mark blocker conflicting — this adds blocker to parentX.ConflictingChildren.
	// blocker has no legit reason to be conflicting: parentX.SpendingDatas[0] names
	// blocker itself as the spender, so no sibling won the output.
	blockerHash := blocker.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*blockerHash}, true)
	require.NoError(t, err)

	// Mark parentX conflicting too. parentX is also orphan: rootTx (grandparent) has
	// SpendingDatas[0] = parentX, so no sibling won. But parent's ConflictingChildren
	// now contains blocker which is still Conflicting=true — the old
	// hasActiveConflictingChildren check would see blocker as "active" and refuse to
	// enqueue parentX as a Case D candidate, leaving the pair stuck forever.
	parentHash := parentX.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentHash}, true)
	require.NoError(t, err)

	// Sanity: parentX.ConflictingChildren includes blocker, blocker is still Conflicting=true.
	pMeta, err := store.Get(ctx, parentHash, fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)
	require.True(t, pMeta.Conflicting)
	require.Contains(t, pMeta.ConflictingChildren, *blockerHash, "parentX.ConflictingChildren must include blocker")

	bMeta, err := store.Get(ctx, blockerHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, bMeta.Conflicting, "blocker starts Conflicting=true")

	gcMeta, err := store.Get(ctx, goodChild.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, gcMeta.Conflicting, "goodChild starts non-conflicting")

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, report.CaseDFixed, "both parentX and blocker must be unmarked in one run")

	// parentX must be Conflicting=false.
	pMeta, err = store.Get(ctx, parentHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, pMeta.Conflicting, "parentX must be unmarked")

	// blocker must be Conflicting=false — discovered via classifyConflictingChildren.
	bMeta, err = store.Get(ctx, blockerHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, bMeta.Conflicting, "orphan blocker must be unmarked")

	// goodChild stays Conflicting=false.
	gcMeta, err = store.Get(ctx, goodChild.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, gcMeta.Conflicting)
}

// TestRepairConflictingChains_CaseD_CascadeDryRun verifies that dryRun reports the
// cascade case without writing anything.
func TestRepairConflictingChains_CaseD_CascadeDryRun(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	_, err = store.Create(ctx, rootTx, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 0}),
	)
	require.NoError(t, err)

	parentWinner := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/3)
	_, err = store.Create(ctx, parentWinner, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, parentWinner, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
	require.NoError(t, err)

	parentLoser := makeTxSpendingOutput(t, rootTx, 0, rootTx.Outputs[0].Satoshis/4)
	_, err = store.Create(ctx, parentLoser, 100)
	require.NoError(t, err)

	childOfLoser := makeTxSpendingOutput(t, parentLoser, 0, 5000)
	_, err = store.Create(ctx, childOfLoser, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, childOfLoser, store.GetBlockHeight()+1, utxo.IgnoreFlags{
		IgnoreConflicting: true,
	})
	require.NoError(t, err)

	parentLoserHash := parentLoser.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*parentLoserHash}, true)
	require.NoError(t, err)

	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 1,
		blockHeaderIDs:  []uint32{1},
	}

	report, err := utxo.PurgeConflictingUnmined(ctx, store, querier, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.CaseDCascaded, "dryRun must still report cascade")

	// childOfLoser must remain Conflicting=false — dryRun writes nothing.
	childMeta, err := store.Get(ctx, childOfLoser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, childMeta.Conflicting, "dryRun must not cascade")
}
