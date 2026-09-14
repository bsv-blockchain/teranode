package recoverreplayedtransactions

import (
	"context"
	"database/sql"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/stretchr/testify/require"
)

func reviewChild(t *testing.T, parent string) *bt.Tx {
	t.Helper()
	hash := mustHistoryHash(t, parent)
	tx, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(hash[:]) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	return tx
}

func TestHistoryUnconfirmedRequiresParentSpendCoverage(t *testing.T) {
	for _, covered := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-coverage", true: "covered"}[covered], func(t *testing.T) {
			ancestor := reviewChild(t, strings.Repeat("11", 32))
			parent := reviewChild(t, ancestor.TxID())
			child := reviewChild(t, parent.TxID())
			history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{ancestor, parent}, ancestor.TxID(), HistoryOptions{})
			defer history.Close()
			_, err := history.db.Exec("INSERT INTO targets VALUES(?)", child.TxID())
			require.NoError(t, err)
			if covered {
				_, err = history.db.Exec("INSERT INTO targets VALUES(?)", parent.TxID())
				require.NoError(t, err)
			}
			// Without expansion, parent is retained as ancestor's spender. Its
			// authenticated inclusion alone does not give its outputs spend coverage.
			require.NoError(t, history.Build(t.Context(), tip))
			_, err = history.Lookup(t.Context(), parent.TxID(), tip)
			require.NoError(t, err)
			history.options.Unconfirmed = func(_ context.Context, id string) (*bt.Tx, error) {
				if id == child.TxID() {
					return child, nil
				}
				return nil, nil
			}
			evidence, err := history.Check(t.Context(), child.TxID(), tip)
			require.NoError(t, err)
			if covered {
				require.Equal(t, replayrecovery.Unconfirmed, evidence.Classification, evidence.Reason)
			} else {
				require.Equal(t, replayrecovery.Unknown, evidence.Classification)
				require.Contains(t, evidence.Reason, "parent spend coverage")
			}
		})
	}
}

func TestHistoryRetainedBlockLookupUsesIndex(t *testing.T) {
	tx := reviewChild(t, strings.Repeat("11", 32))
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	defer history.Close()
	require.NoError(t, history.Build(t.Context(), tip))
	var retained int
	require.NoError(t, history.db.QueryRow("SELECT count(*) FROM transactions WHERE block=?", tip.Hash).Scan(&retained))
	require.Equal(t, 1, retained)
	rows, err := history.db.Query("EXPLAIN QUERY PLAN SELECT count(*) FROM transactions WHERE block=?", tip.Hash)
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail)
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), "SEARCH transactions", "per-block retention lookup must not scan accumulated transactions")
	require.NotContains(t, plan.String(), "SCAN transactions")
}

func TestHistoryResumeAfterCancelledInitialization(t *testing.T) {
	tx := reviewChild(t, strings.Repeat("11", 32))
	original, chain, archive, path, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	opts := original.options
	require.NoError(t, original.Close())
	path += ".interrupted"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	history, err := NewHistory(ctx, path, chain, archive, opts)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, history)
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, stat.Size())
	_, err = os.Stat(path + ".lock")
	require.NoError(t, err)
	history, err = buildJobHistory(t.Context(), path, chain, archive, opts, tip, true)
	require.NoError(t, err)
	defer history.Close()
	require.True(t, history.Coverage().Complete)
	proof, err := history.Lookup(t.Context(), tx.TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(tx.Bytes()), proof.RawTx)
}

func TestHistoryResumeRefusesForeignInitialization(t *testing.T) {
	for _, setup := range []string{"PRAGMA application_id=7", "PRAGMA user_version=7", "CREATE TABLE foreign_state(value TEXT)"} {
		t.Run(setup, func(t *testing.T) {
			tx := reviewChild(t, strings.Repeat("11", 32))
			original, chain, archive, path, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
			opts := original.options
			require.NoError(t, original.Close())
			path += ".foreign"
			require.NoError(t, os.WriteFile(path, nil, 0600))
			require.NoError(t, os.WriteFile(path+".lock", nil, 0600))
			db, err := sql.Open("sqlite", path)
			require.NoError(t, err)
			_, err = db.Exec(setup)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			counted := &historyReadCounter{HistoryChain: chain}
			history, err := buildJobHistory(t.Context(), path, counted, archive, opts, tip, true)
			require.Error(t, err)
			require.Nil(t, history)
			require.Zero(t, counted.blocks)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "refused artifacts must not be rebuilt")
		})
	}
}
