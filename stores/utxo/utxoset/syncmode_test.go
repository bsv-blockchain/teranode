package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestJournalRunsBelowTheCheckpoint is the regression guard for a decision that was made
// twice, the wrong way round the first time.
//
// SetBlockHeight used to drive sync mode off model.BelowCheckpoint and switch the journal
// off for the entire initial sync. The reasoning was correct as far as it went: below the
// hardcoded checkpoint a reorg is impossible by rule, so the undo payload can never be
// replayed, and writing it is a heap insert plus an index insert per input for nothing.
//
// What that missed, at the time, was that the journal was also the prune engine: a retiring
// partition was the only record of WHICH transactions had an output spent in a window, and
// with the journal off nothing at all could be reclaimed below the checkpoint.
//
// That is no longer why it stays on. Identity reclaim is a tx_mined window drop and reads
// nothing, so the journal is undo insurance again. It stays on below the checkpoint because
// switching it off there is a SECOND spend path -- one that writes the undo row and one that
// does not -- exercised only during a sync, for a measured 354.8 bytes of WAL and 12.9
// microseconds per spend. That is about 6% of the per-block budget in the worst band, and not
// worth a divergent path on the store's hottest statement.
//
// So the journal still has no off-switch, and this test pins the height case that used to
// disable it: deep below the checkpoint, a spend must still be journalled.
func TestJournalRunsBelowTheCheckpoint(t *testing.T) {
	s, ctx := newTestStore(t)

	s.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	require.NoError(t, s.SetBlockHeight(200))

	parent := mkTx(t, 1, 555)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	_, err = spendOnly(ctx, s, child, 200)
	require.NoError(t, err)

	var journalled int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal`).Scan(&journalled))
	require.Equal(t, 1, journalled,
		"the journal has no off-switch: one spend path writes the undo row at every height, checkpoint or not")
}
