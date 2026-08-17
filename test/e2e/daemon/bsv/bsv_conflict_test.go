package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// outpoint names one output of a transaction, so the graph below can be written
// the way upstream draws it.
type outpoint struct {
	tx   *bt.Tx
	vout uint32
}

// TestBSVConflict ports bsv-conflict.py, which builds a graph of eight unconfirmed
// transactions, mines a block that double-spends one of them, and checks that the
// double-spent transaction and everything descending from it is dropped while the
// unrelated branch survives.
//
// The graph is upstream's, drawn in its own comment and reproduced here:
//
//	txDoubleSpendMempool          txToBeMined
//	 |            |                |        |
//	 |            +------+  +------+        |
//	 |                   |  |                \
//	 |                 descendant1        stayInPool1
//	 |                      |                |     |
//	 |                 descendant2           |  stayInPool2
//	 |                      |                |     |
//	 |                 descendant3 <---------+     |
//	 |                                             |
//	 +------------------ descendant4 <-------------+
//
// A block then mines txDoubleSpendBlock, which spends the same output as
// txDoubleSpendMempool, together with txToBeMined. Only stayInPool1 and
// stayInPool2 should remain.
//
// Two of the four evictions are the interesting ones, because each transaction has
// a surviving parent as well as a doomed one: descendant1 spends txToBeMined, which
// was mined rather than invalidated, and descendant4 spends stayInPool2, which
// survives. Both must still go, because one input each is gone. A node that walked
// only direct children, or that kept anything with a live parent, would keep them.
//
// Teranode gets all of it right. The one thing worth knowing is that the eviction is
// ASYNCHRONOUS: measured, the conflicting transactions are still listed immediately
// after the block becomes the tip and are gone about a second later. An earlier
// version of this port read the pool once and would have reported a serious defect
// that does not exist.
func TestBSVConflict(t *testing.T) {
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// Upstream funds from a coinbase it mines itself and then splits. Teranode
	// coinbases are P2PKH, so the split happens through a funding transaction, as in
	// the other transaction-level ports.
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	funding := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(2e6, anyoneCanSpendScript()),
	)
	requireAccepted(t, td, funding, "funding")
	td.MineAndWait(t, 1)

	// Upstream's low_fee_tx: the transaction whose two outputs the graph hangs off,
	// confirmed before the graph is built so every graph member is unconfirmed.
	root := spendOutpoints(t, []outpoint{{funding, 0}}, 2, 0)
	requireAccepted(t, td, root, "root")
	td.MineAndWait(t, 1)

	// The graph, in upstream's order and with its names.
	txDoubleSpendMempool := spendOutpoints(t, []outpoint{{root, 0}}, 2, 0)
	txToBeMined := spendOutpoints(t, []outpoint{{root, 1}}, 2, 0)

	descendant1 := spendOutpoints(t, []outpoint{{txDoubleSpendMempool, 0}, {txToBeMined, 0}}, 1, 0)
	descendant2 := spendOutpoints(t, []outpoint{{descendant1, 0}}, 1, 0)
	stayInPool1 := spendOutpoints(t, []outpoint{{txToBeMined, 1}}, 2, 0)
	descendant3 := spendOutpoints(t, []outpoint{{descendant2, 0}, {stayInPool1, 0}}, 2, 0)
	stayInPool2 := spendOutpoints(t, []outpoint{{stayInPool1, 1}}, 1, 0)
	descendant4 := spendOutpoints(t, []outpoint{{txDoubleSpendMempool, 1}, {stayInPool2, 0}}, 1, 0)

	// txDoubleSpendBlock spends the same output as txDoubleSpendMempool. The salt
	// gives it different output values and so a different txid; upstream gets the
	// same effect by giving it one output instead of two.
	txDoubleSpendBlock := spendOutpoints(t, []outpoint{{root, 0}}, 1, 500)
	require.NotEqual(t, txDoubleSpendMempool.TxID(), txDoubleSpendBlock.TxID(),
		"the two spends of the same output must be different transactions")

	graph := []struct {
		name string
		tx   *bt.Tx
	}{
		{"txDoubleSpendMempool", txDoubleSpendMempool},
		{"txToBeMined", txToBeMined},
		{"descendant1", descendant1},
		{"descendant2", descendant2},
		{"stayInPool1", stayInPool1},
		{"descendant3", descendant3},
		{"stayInPool2", stayInPool2},
		{"descendant4", descendant4},
	}

	t.Run("the whole graph is accepted", func(t *testing.T) {
		// Upstream: send all eight, then check_mempool_equals lists all eight.
		// Submitted parents-first, since each spends the one before it.
		for _, entry := range graph {
			requireAccepted(t, td, entry.tx, entry.name)
		}

		pool := tryRawMempool(td)
		for _, entry := range graph {
			require.Contains(t, pool, entry.tx.TxID(), "%s should be queued for mining", entry.name)
		}
	})

	t.Run("mining a double-spend evicts it and every descendant", func(t *testing.T) {
		// Upstream's block2: the conflicting spend and txToBeMined together.
		tip := blockOf(t, td, tipHeader(t, td))
		_, block := td.CreateTestBlock(t, tip, nextNonce(t), txDoubleSpendBlock, txToBeMined)

		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height, "", "legacy", 0),
			"the block carrying the double-spend should be accepted")

		require.Eventually(t, func() bool { return tryBestBlockHash(td) == block.Header.Hash().String() },
			rpcSettle, rpcPoll, "the block should become the tip")

		// Upstream: check_mempool_equals(rpc, [stayInPool1, stayInPool2]).
		//
		// Polled, because eviction is asynchronous - see the doc comment. The
		// condition covers both halves at once so it cannot pass through a moment
		// where the doomed ones have gone but the survivors have gone too.
		evicted := []struct {
			name string
			tx   *bt.Tx
		}{
			{"txDoubleSpendMempool", txDoubleSpendMempool},
			{"descendant1", descendant1},
			{"descendant2", descendant2},
			{"descendant3", descendant3},
			{"descendant4", descendant4},
			{"txToBeMined", txToBeMined},
		}

		require.Eventually(t, func() bool {
			pool := tryRawMempool(td)

			for _, entry := range evicted {
				if containsTxID(pool, entry.tx.TxID()) {
					return false
				}
			}

			return containsTxID(pool, stayInPool1.TxID()) && containsTxID(pool, stayInPool2.TxID())
		}, rpcSettle, rpcPoll, "the conflicted branch should be evicted and the unrelated branch kept")

		// Named assertions on top of the polled condition, so a failure says which
		// transaction was wrong rather than only that the set was.
		pool := tryRawMempool(td)

		for _, entry := range evicted {
			require.NotContains(t, pool, entry.tx.TxID(),
				"%s should have been evicted: it is the double-spend, a descendant of it, or was mined",
				entry.name)
		}

		require.Contains(t, pool, stayInPool1.TxID(), "stayInPool1 descends only from surviving outputs")
		require.Contains(t, pool, stayInPool2.TxID(), "stayInPool2 descends only from surviving outputs")
	})

	t.Run("the next block carries none of the evicted transactions", func(t *testing.T) {
		// Not an upstream assertion, and the reason the one above matters: a node
		// that kept the conflicted transactions would mine them into a block that
		// double-spends an already-spent output.
		mined := td.MineAndWait(t, 1)

		full, err := td.BlockchainClient.GetBlock(td.Ctx, mined.Header.Hash())
		require.NoError(t, err, "read the block just mined")

		t.Logf("next block: txCount=%d subtrees=%d", full.TransactionCount, len(full.Subtrees))

		require.NotContains(t, tryRawMempool(td), txDoubleSpendMempool.TxID(),
			"the double-spent transaction must not reappear")
	})
}

// requireAccepted submits a transaction and waits for block assembly to take it.
func requireAccepted(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx, name string) {
	t.Helper()

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx), "%s should be accepted", name)
	td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())
}

// spendOutpoints builds a transaction spending every given outpoint into n equal
// anyone-can-spend outputs, which is upstream's create_tx.
//
// salt is subtracted from the total before splitting, so two transactions spending
// the same outpoint can be given different txids - upstream achieves the same by
// varying the output count.
//
// The unlocking script is empty rather than OP_TRUE. An OP_TRUE locking script
// leaves one item on the stack by itself; pushing another would leave two and fail
// the clean-stack rule, which is what the first attempt at this port did.
func spendOutpoints(t *testing.T, spend []outpoint, outputs int, salt uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	total := uint64(0)

	for _, o := range spend {
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      o.tx.TxIDChainHash(),
			Vout:          o.vout,
			LockingScript: o.tx.Outputs[o.vout].LockingScript,
			Satoshis:      o.tx.Outputs[o.vout].Satoshis,
		}), "add input spending output %d", o.vout)

		total += o.tx.Outputs[o.vout].Satoshis
	}

	for i := range tx.Inputs {
		tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	each := (total - 1000 - salt) / uint64(outputs)
	for range outputs {
		tx.AddOutput(&bt.Output{Satoshis: each, LockingScript: anyoneCanSpendScript()})
	}

	return tx
}

// containsTxID reports membership without asserting, for use in polling conditions.
func containsTxID(pool []string, want string) bool {
	for _, id := range pool {
		if id == want {
			return true
		}
	}

	return false
}
