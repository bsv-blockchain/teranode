package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestConflictWALScanPolicy pins the fallback scan's policy. A socket timeout of zero would make
// the connection deadline the whole 5 minutes, so a scan that stops early could leave node reads
// running while the startup retry begins the next namespace walk.
func TestConflictWALScanPolicy(t *testing.T) {
	policy := conflictWALScanPolicy()

	require.Equal(t, time.Minute, policy.SocketTimeout)
	require.Equal(t, 5*time.Minute, policy.TotalTimeout)
	require.Equal(t, 2, policy.MaxRetries)
	require.Greater(t, policy.SocketTimeout, time.Duration(0), "a scan needs a finite socket timeout")
	require.Less(t, policy.SocketTimeout, policy.TotalTimeout)
}

// conflictWALTestRecord is a well-formed WAL root as BeginConflictIntent writes it.
func conflictWALTestRecord() aerospike.BinMap {
	blockHash := chainhash.HashH([]byte("decode-block"))
	txHash := chainhash.HashH([]byte("decode-tx"))

	return aerospike.BinMap{
		walBinKind:        string(utxo.ConflictIntentReverse),
		walBinBlockHeight: 9,
		walBinBlockHash:   blockHash[:],
		walBinTxHashes:    txHash[:],
		walBinStartedAt:   5,
		walBinKeyFormula:  walKeyFormulaVersion,
	}
}

// TestDecodeConflictWALRecord pins that a corrupt root fails the read instead of being skipped:
// a skipped root is an interrupted operation that startup would never recover.
func TestDecodeConflictWALRecord(t *testing.T) {
	t.Run("current root", func(t *testing.T) {
		intent, completed, err := decodeConflictWALRecord(&aerospike.Record{Bins: conflictWALTestRecord()})
		require.NoError(t, err)
		require.False(t, completed)
		require.Equal(t, utxo.ConflictIntentReverse, intent.Kind)
		require.Equal(t, uint32(9), intent.BlockHeight)
		require.Equal(t, int64(5), intent.StartedAt)
		require.Len(t, intent.TxHashes, 1)
		require.Equal(t, chainhash.Hash{}, intent.ID, "a root with the key formula derives its key")
	})

	t.Run("legacy root keeps its key", func(t *testing.T) {
		bins := conflictWALTestRecord()
		delete(bins, walBinKeyFormula)

		intent, _, err := decodeConflictWALRecord(&aerospike.Record{Bins: bins})
		require.NoError(t, err)
		require.Equal(t, utxo.LegacyConflictIntentID(intent.Kind, intent.BlockHash, intent.BlockHeight, intent.TxHashes), intent.ID)
	})

	t.Run("completed root", func(t *testing.T) {
		bins := conflictWALTestRecord()
		bins["recoveryDone"] = true

		_, completed, err := decodeConflictWALRecord(&aerospike.Record{Bins: bins})
		require.NoError(t, err)
		require.True(t, completed)
	})

	for _, bin := range []string{walBinKind, walBinBlockHeight, walBinBlockHash, walBinTxHashes, walBinStartedAt} {
		t.Run("missing "+bin, func(t *testing.T) {
			bins := conflictWALTestRecord()
			delete(bins, bin)

			_, _, err := decodeConflictWALRecord(&aerospike.Record{Bins: bins})
			require.ErrorContains(t, err, bin)
		})
	}

	t.Run("short block hash", func(t *testing.T) {
		bins := conflictWALTestRecord()
		bins[walBinBlockHash] = []byte{1, 2, 3}

		_, _, err := decodeConflictWALRecord(&aerospike.Record{Bins: bins})
		require.ErrorContains(t, err, "corrupt blockHash")
	})

	t.Run("truncated tx hashes", func(t *testing.T) {
		bins := conflictWALTestRecord()
		bins[walBinTxHashes] = make([]byte, chainhash.HashSize+1)

		_, _, err := decodeConflictWALRecord(&aerospike.Record{Bins: bins})
		require.ErrorContains(t, err, "corrupt intent record")
	})
}

// TestConflictWALReads_CancelledContext pins that both WAL reads stop on a cancelled context
// before they send anything.
func TestConflictWALReads_CancelledContext(t *testing.T) {
	s := &Store{client: &uaerospike.Client{}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, readable, err := s.queryConflictWALIndex(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, readable)

	_, err = s.scanConflictWAL(ctx)
	require.ErrorIs(t, err, context.Canceled)

	_, err = s.PendingConflictIntents(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestEnsureConflictWALIndex_WithoutAClient: a store without a client, as in unit tests, has no
// index to create.
func TestEnsureConflictWALIndex_WithoutAClient(t *testing.T) {
	require.NotPanics(t, (&Store{}).ensureConflictWALIndex)
	require.NotPanics(t, (&Store{client: &uaerospike.Client{}}).ensureConflictWALIndex)
}
