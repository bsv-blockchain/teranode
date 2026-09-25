package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/stretchr/testify/require"
)

// Store a real SQLite graph rooted in a transaction mined on the fixture's
// authoritative chain. Returned hashes exclude that mined anchor.
func storeRecoverySelectionChain(t *testing.T, b *BlockAssembler, count int) []chainhash.Hash {
	t.Helper()
	header, _ := b.CurrentBlock()
	_, blockMeta, err := b.blockchainClient.GetBlockHeader(t.Context(), header.Hash())
	require.NoError(t, err)
	txs := generateTestTransactions(t, count+1)
	_, _, err = b.utxoStore.SpendAndCreate(t.Context(), txs[0], 1, utxo.WithCreateOnly(),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockMeta.ID, BlockHeight: blockMeta.Height}))
	require.NoError(t, err)
	hashes := make([]chainhash.Hash, 0, count)
	for i := 1; i <= count; i++ {
		txs[i].Inputs[0].PreviousTxOutIndex = 0
		txs[i].Inputs[0].PreviousTxSatoshis = txs[i-1].Outputs[0].Satoshis
		require.NoError(t, txs[i].Inputs[0].PreviousTxIDAdd(txs[i-1].TxIDChainHash()))
		_, _, err = b.utxoStore.SpendAndCreate(t.Context(), txs[i], 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		hashes = append(hashes, *txs[i].TxIDChainHash())
	}
	return hashes
}

func selectionHashes(txs []*utxo.UnminedTransaction) []chainhash.Hash {
	hashes := make([]chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		hashes = append(hashes, tx.Hash)
	}
	return hashes
}

func TestPrepareUnminedRecoveryHydratesAndOrdersAncestors(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 3)
	// Only the already-assembled child was in the captured input; ancestors
	// can have been created after the index scan passed their partition.
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[2], chain[2]}, nil)
	require.NoError(t, err)
	require.Equal(t, chain, selectionHashes(selected))
	for i, tx := range selected {
		require.NotEmpty(t, tx.TxInpoints.ParentTxHashes)
		if i > 0 {
			require.Contains(t, tx.TxInpoints.ParentTxHashes, chain[i-1])
		}
	}
}

func TestPrepareUnminedRecoveryLockedRequiresAcceptedHandoff(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	require.NoError(t, b.utxoStore.SetLocked(t.Context(), []chainhash.Hash{chain[0]}, true))
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Empty(t, selected, "unqueued in-flight parent and its descendants are deferred")
	selected, err = b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1], chain[0]}, func(hash chainhash.Hash) bool {
		return hash == chain[0]
	})
	require.NoError(t, err)
	require.Equal(t, chain, selectionHashes(selected))
	parent, err := b.utxoStore.Get(t.Context(), &chain[0], fields.Locked)
	require.NoError(t, err)
	require.True(t, parent.Locked, "online recovery must not unlock an accepted transaction")
}

// Fault injection retains the real store for every read and write; SQL has no
// multi-record Creating field, so that Aerospike state is injected at its read seam.
type recoverySelectionReadFault struct {
	utxo.Store
	hash       chainhash.Hash
	creating   bool
	err        error
	parents    []chainhash.Hash
	batchSizes []int
	reads      map[chainhash.Hash]int
}

func (s *recoverySelectionReadFault) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, requested ...fields.FieldName) error {
	s.batchSizes = append(s.batchSizes, len(items))
	if err := s.Store.BatchDecorate(ctx, items, requested...); err != nil {
		return err
	}
	for _, item := range items {
		if s.reads != nil {
			s.reads[item.Hash]++
		}
		if item.Hash == s.hash {
			if s.err != nil {
				item.Err = s.err
			} else if item.Data != nil {
				item.Data.Creating = s.creating
				if s.parents != nil {
					item.Data.TxInpoints.ParentTxHashes = s.parents
				}
			}
		}
	}
	return nil
}

func TestPrepareUnminedRecoveryCyclesAndCancellation(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	b.utxoStore = &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], parents: []chainhash.Hash{chain[1]}}
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Empty(t, selected)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	selected, err = b.prepareUnminedRecovery(ctx, []chainhash.Hash{chain[1]}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, selected)
}

func TestPrepareUnminedRecoveryBatchesFreshMetadataOnce(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	b.settings.BlockAssembly.ParentValidationBatchSize = 2
	chain := storeRecoverySelectionChain(t, b, 4)
	fault := &recoverySelectionReadFault{Store: b.utxoStore, reads: make(map[chainhash.Hash]int)}
	b.utxoStore = fault
	selected, err := b.prepareUnminedRecovery(t.Context(), append(chain, chain...), nil)
	require.NoError(t, err)
	require.Equal(t, chain, selectionHashes(selected))
	require.Equal(t, []int{2, 2, 1}, fault.batchSizes, "initial graph and mined ancestor are fetched in bounded batches")
	for _, count := range fault.reads {
		require.Equal(t, 1, count, "each metadata record must be fetched once per selection")
	}
}

func TestPrepareUnminedRecoveryExcludesMinedBeyondThousandHeaders(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	header, height := b.CurrentBlock()
	initialHeight := height
	_, minedMeta, err := b.blockchainClient.GetBlockHeader(t.Context(), header.Hash())
	require.NoError(t, err)
	_, err = b.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{&chain[0]}, utxo.MinedBlockInfo{
		BlockID: minedMeta.ID, BlockHeight: minedMeta.Height,
	})
	require.NoError(t, err)
	// Retain an indexed inconsistency: chain membership must override it.
	require.NoError(t, b.utxoStore.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{chain[0]}, false))
	for i := uint32(0); i < 1001; i++ {
		coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)
		encodedHeight := []byte{byte(height + 1)}
		if height+1 >= 128 {
			encodedHeight = append(encodedHeight, byte((height+1)>>8))
		}
		coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(append([]byte{byte(len(encodedHeight))}, encodedHeight...))
		next := &model.BlockHeader{
			Version: header.Version, HashPrevBlock: header.Hash(), HashMerkleRoot: &chainhash.Hash{},
			Timestamp: header.Timestamp + 1, Bits: header.Bits, Nonce: i + 1,
		}
		require.NoError(t, b.blockchainClient.AddBlock(t.Context(), &model.Block{
			Header: next, CoinbaseTx: coinbase, TransactionCount: 1, Subtrees: []*chainhash.Hash{},
		}, "", options.WithMinedSet(true)))
		header = next
		height++
	}
	bestHeader, bestMeta, err := b.blockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, initialHeight+1001, bestMeta.Height, "fixture must persist a real chain longer than the former exclusion window")
	require.Equal(t, header.Hash(), bestHeader.Hash())
	ids, err := b.blockchainClient.GetBlockHeaderIDs(t.Context(), bestHeader.Hash(), uint64(bestMeta.Height)+1)
	require.NoError(t, err)
	require.Len(t, ids, int(bestMeta.Height)+1)
	require.Contains(t, ids, minedMeta.ID, "full chain includes the original mined anchor")
	b.setBestBlockHeader(header, height)
	selected, err := b.prepareUnminedRecovery(t.Context(), chain, nil)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{chain[1]}, selectionHashes(selected), "exclude old mined parent while retaining its valid unmined child")
	parent, err := b.utxoStore.Get(t.Context(), &chain[0], fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, parent.UnminedSince, "selection must not repair store flags as a side effect")
}

func TestPrepareUnminedRecoveryCreatingAndReadFailure(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	fault := &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], creating: true}
	b.utxoStore = fault
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, func(chainhash.Hash) bool { return true })
	require.Error(t, err)
	require.Nil(t, selected, "accepted handoff with incomplete creation must preserve the previous assembly and queue")
	fault.creating = false
	fault.err = errors.NewStorageError("injected transient metadata failure")
	selected, err = b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.ErrorIs(t, err, fault.err)
	require.Nil(t, selected, "transient errors must abort before destructive rebuild")
	fault.err = nil
	selected, err = b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Equal(t, chain, selectionHashes(selected))
}

func TestPrepareUnminedRecoveryMissingAndConflictingParents(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	fault := &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], err: errors.ErrTxNotFound}
	b.utxoStore = fault
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Empty(t, selected)
	fault.err = nil
	_, err = fault.Store.(*utxosql.Store).RawDB().ExecContext(t.Context(), "UPDATE transactions SET conflicting = true WHERE hash = $1", chain[0][:])
	require.NoError(t, err)
	selected, err = b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Empty(t, selected)
}

// A successful selection authorizes discarding the captured queue prefix. A
// transient exclusion must therefore fail the whole selection when that prefix
// or the current template still owns the transaction (or a dependent child).
func TestPrepareUnminedRecoveryPreservesAcceptedTransientTransactions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		creating      bool
		missing       bool
		locked        bool
		acceptedChild bool
	}{
		{name: "creating accepted transaction", creating: true},
		{name: "missing accepted transaction", missing: true},
		{name: "creating ancestor of accepted child", creating: true, acceptedChild: true},
		{name: "missing ancestor of accepted child", missing: true, acceptedChild: true},
		{name: "unacknowledged locked ancestor of accepted child", locked: true, acceptedChild: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
			chain := storeRecoverySelectionChain(t, b, 3)
			if tc.locked {
				require.NoError(t, b.utxoStore.SetLocked(t.Context(), []chainhash.Hash{chain[0]}, true))
			}
			fault := &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], creating: tc.creating}
			if tc.missing {
				fault.err = errors.ErrTxNotFound
			}
			b.utxoStore = fault
			owned := chain[0]
			if tc.acceptedChild {
				owned = chain[2]
			}
			accepted := func(hash chainhash.Hash) bool { return hash == owned }
			selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{owned}, accepted)
			require.Error(t, err, "must not authorize discarding an accepted transaction with unknown eligibility")
			require.Nil(t, selected)
			fault.creating, fault.err = false, nil
			require.NoError(t, fault.Store.SetLocked(t.Context(), []chainhash.Hash{chain[0]}, false))
			selected, err = b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{owned}, accepted)
			require.NoError(t, err)
			if tc.acceptedChild {
				require.Equal(t, chain, selectionHashes(selected))
			} else {
				require.Equal(t, chain[:1], selectionHashes(selected))
			}
		})
	}
}

func TestPrepareUnminedRecoveryDefersUnacceptedCreatingTransactions(t *testing.T) {
	b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	chain := storeRecoverySelectionChain(t, b, 2)
	b.utxoStore = &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], creating: true}
	selected, err := b.prepareUnminedRecovery(t.Context(), []chainhash.Hash{chain[1]}, nil)
	require.NoError(t, err)
	require.Empty(t, selected, "unaccepted index rows do not prevent recovery of other eligible transactions")
}

func TestUnminedRecoveryTransientSelectionRetainsPublishedQueue(t *testing.T) {
	for _, creating := range []bool{true, false} {
		name := "missing"
		if creating {
			name = "creating"
		}
		t.Run(name, func(t *testing.T) {
			b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) {
				// Keep the real dispatcher from consuming the newly published batch
				// before recovery captures it; recovery itself does not wait this window.
				s.BlockAssembly.DoubleSpendWindow = time.Hour
			})
			chain := storeRecoverySelectionChain(t, b, 2)
			rows, err := b.prepareUnminedRecovery(t.Context(), chain, nil)
			require.NoError(t, err)
			b.subtreeProcessor.AddBatch([]subtree.Node{*rows[0].Node, *rows[1].Node}, []*subtree.TxInpoints{rows[0].TxInpoints, rows[1].TxInpoints})
			before := recoveryCandidateHashes(t, b)
			require.Equal(t, int64(2), b.subtreeProcessor.QueueLength())
			fault := &recoverySelectionReadFault{Store: b.utxoStore, hash: chain[0], creating: creating}
			if !creating {
				fault.err = errors.ErrTxNotFound
			}
			b.utxoStore = fault
			header, _ := b.CurrentBlock()
			err = b.subtreeProcessor.RecoverUnmined(t.Context(), header, nil, b.prepareUnminedRecovery)
			require.Error(t, err)
			require.Equal(t, int64(2), b.subtreeProcessor.QueueLength(), "published handoffs remain owned after incomplete metadata")
			require.Equal(t, before, recoveryCandidateHashes(t, b), "read-only failure preserves the previous template")
			require.False(t, b.subtreeProcessor.RecoveryPending(), "selection has not started destructive reconstruction")
			fault.creating, fault.err = false, nil
			require.NoError(t, b.subtreeProcessor.RecoverUnmined(t.Context(), header, nil, b.prepareUnminedRecovery))
			require.Equal(t, int64(2), b.subtreeProcessor.QueueLength(), "repair leaves queued handoffs for normal dequeue")
			require.Equal(t, before, recoveryCandidateHashes(t, b), "repair preserves announced template while queue is delayed")
		})
	}
}
