package netsync

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func unlockFailedErr() error {
	return errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded"))
}

func newUnlockFailureStore(t *testing.T, name string) *utxosql.Store {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	store, err := utxosql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)

	return store
}

func newUnlockSyncManager(t *testing.T, validationClient *validator.MockValidator) (*SyncManager, *peerpkg.Peer) {
	t.Helper()

	peer := &peerpkg.Peer{}

	sm := &SyncManager{
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		blockchainClient: &blockchain.Mock{},
		validationClient: validationClient,
		utxoStore:        &nullstore.NullStore{},
		orphanTxs:        expiringmap.New[chainhash.Hash, *orphanTxAndParents](10 * time.Minute),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
		requestedTxns:    expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		ctx:              t.Context(),
	}

	sm.peerStates.Set(peer, &peerSyncState{
		syncCandidate:   true,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](10),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Minute),
	})

	return sm, peer
}

func TestAcceptedMetaOnUnlockFailure(t *testing.T) {
	ctx := context.Background()
	txs := transactions.CreateTestTransactionChainWithCount(t, 3)
	tx := txs[1]

	store := newUnlockFailureStore(t, "netsync_unlock_meta")
	_, err := store.Create(ctx, tx, 1)
	require.NoError(t, err)

	sm := &SyncManager{utxoStore: store}

	t.Run("reads the accepted record back when the response carried no metadata", func(t *testing.T) {
		got, err := sm.acceptedMetaOnUnlockFailure(ctx, tx, nil, unlockFailedErr())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NotZero(t, got.SizeInBytes, "the meta must come from the stored record")
	})

	t.Run("keeps metadata the validator returned", func(t *testing.T) {
		given := &meta.Data{Fee: 7}

		got, err := sm.acceptedMetaOnUnlockFailure(ctx, tx, given, unlockFailedErr())
		require.NoError(t, err)
		require.Same(t, given, got)
	})

	t.Run("fails when the accepted record cannot be read", func(t *testing.T) {
		missing := &SyncManager{utxoStore: newUnlockFailureStore(t, "netsync_unlock_meta_empty")}

		_, err := missing.acceptedMetaOnUnlockFailure(ctx, tx, nil, unlockFailedErr())
		require.Error(t, err)
	})

	t.Run("leaves every other result unchanged", func(t *testing.T) {
		other := errors.NewTxInvalidError("bad tx")

		got, err := sm.acceptedMetaOnUnlockFailure(ctx, tx, nil, other)
		require.Nil(t, got)
		require.Equal(t, other, err)

		given := &meta.Data{Fee: 9}

		got, err = sm.acceptedMetaOnUnlockFailure(ctx, tx, given, nil)
		require.NoError(t, err)
		require.Same(t, given, got)
	})
}

// A tx the validator accepted must be announced, not rejected, when only its unlock failed.
func TestHandleTxMsg_UnlockFailureIsAccepted(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, peer := newUnlockSyncManager(t, &validator.MockValidator{Errors: []error{unlockFailedErr()}})

	notifier := &recordingNotifier{MockPeerNotifier: &MockPeerNotifier{}}
	sm.peerNotifier = notifier

	store := newUnlockFailureStore(t, "netsync_unlock_handletx")
	sm.utxoStore = store

	btTx := transactions.CreateTestTransactionChainWithCount(t, 3)[1]

	_, err := store.Create(t.Context(), btTx, 1)
	require.NoError(t, err)

	msg := wire.NewMsgTx(1)
	require.NoError(t, msg.Deserialize(bytes.NewReader(btTx.Bytes())))

	tx := bsvutil.NewTx(msg)

	sm.handleTxMsg(&txMsg{tx: tx, peer: peer})

	_, rejected := sm.rejectedTxns.Get(*tx.Hash())
	require.False(t, rejected, "an accepted tx must not be recorded as rejected")
	require.Equal(t, []chainhash.Hash{*btTx.TxIDChainHash()}, notifier.announced(), "an accepted tx must be announced to peers")
}

func TestProcessOrphanTransactions_UnlockFailureIsAccepted(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, _ := newUnlockSyncManager(t, &validator.MockValidator{Errors: []error{unlockFailedErr()}})

	store := newUnlockFailureStore(t, "netsync_unlock_orphan")
	sm.utxoStore = store

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)
	parent, orphan := txs[0], txs[1]

	_, err := store.Create(t.Context(), orphan, 1)
	require.NoError(t, err)

	parents := txmap.NewSyncedMap[chainhash.Hash, struct{}]()
	parents.Set(*parent.TxIDChainHash(), struct{}{})

	sm.orphanTxs.Set(*orphan.TxIDChainHash(), &orphanTxAndParents{tx: orphan, parents: parents, addedAt: time.Now()})

	var accepted []*TxHashAndFee

	sm.processOrphanTransactions(t.Context(), parent.TxIDChainHash(), &accepted)

	require.Len(t, accepted, 1, "an accepted orphan whose unlock failed must be announced")
	require.Equal(t, *orphan.TxIDChainHash(), accepted[0].TxHash)
}

func TestPreValidateTransactions_UnlockFailureDoesNotFailBlock(t *testing.T) {
	initPrometheusMetrics()

	cv := &countingValidator{failFirst: 5, failErr: unlockFailedErr()}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 1
	tSettings.Legacy.SpendBatcherConcurrency = 1

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
	}

	err := sm.PreValidateTransactions(context.Background(), makeTxMap(t, 5), chainhash.Hash{}, 100, 0, 0, false, false)
	require.NoError(t, err, "accepted transactions whose unlock failed must not abort legacy block sync")
	require.Equal(t, int64(5), cv.callCount.Load(), "every transaction is validated once and none is retried")
}
