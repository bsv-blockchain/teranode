package subtreevalidation

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	teranodetestutil "github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/jarcoal/httpmock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// delegatingRecorder records which transactions reached the validator, and with which
// options, and lets the real validator decide.
type delegatingRecorder struct {
	validator.Interface

	mu    sync.Mutex
	calls map[chainhash.Hash][]*validator.Options
}

func (r *delegatingRecorder) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts *validator.Options) (*meta.Data, error) {
	r.mu.Lock()
	r.calls[*tx.TxIDChainHash()] = append(r.calls[*tx.TxIDChainHash()], opts)
	r.mu.Unlock()

	return r.Interface.ValidateWithOptions(ctx, tx, blockHeight, opts)
}

// total returns how many validator calls were made, over all transactions.
func (r *delegatingRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0
	for _, calls := range r.calls {
		n += len(calls)
	}

	return n
}

func (r *delegatingRecorder) validated(h chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.calls[h]) > 0
}

// conflictingFixture is a node that accepted winner, which spends the only output of
// a mined parent. loser spends the same output, and child spends loser's output: the
// shape a peer's subtree has when the peer accepted loser instead.
type conflictingFixture struct {
	server   *Server
	store    *sql.Store
	recorder *delegatingRecorder
	settings *settings.Settings
	winner   *bt.Tx
	loser    *bt.Tx
	child    *bt.Tx
	// other spends an unrelated mined output and conflicts with nothing
	other *bt.Tx
	// parent is the mined tx winner and loser both spend
	parent     *bt.Tx
	privateKey *bec.PrivateKey
}

func newConflictingFixture(t *testing.T) *conflictingFixture {
	t.Helper()

	InitPrometheusMetrics()

	ctx := t.Context()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true
	tSettings.SubtreeValidation.SpendBatcherSize = 10

	utxoURL, err := url.Parse("sqlitememory:///conflicting_kept_aside_" + t.Name())
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)
	store.RawDB().SetMaxOpenConns(1)
	require.NoError(t, store.SetBlockHeight(100))
	require.NoError(t, store.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_KEPT_ASIDE_TEST_KEY!"))

	parent := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/conflicting kept aside test/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	_, err = store.Create(ctx, parent, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)

	winner := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 90_000, publicKey),
	)
	loser := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 80_000, publicKey),
	)
	child := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(loser, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 70_000, publicKey),
	)

	otherParent := transactions.Create(t,
		transactions.WithCoinbaseData(2, "/conflicting kept aside test other/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	_, err = store.Create(ctx, otherParent, 2, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 2, BlockHeight: 2, SubtreeIdx: 0}))
	require.NoError(t, err)

	other := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(otherParent, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 60_000, publicKey),
	)

	realValidator, err := validator.New(ctx, logger, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = realValidator.Validate(ctx, winner, 101)
	require.NoError(t, err)

	recorder := &delegatingRecorder{Interface: realValidator, calls: map[chainhash.Hash][]*validator.Options{}}

	server := &Server{
		logger:                logger,
		settings:              tSettings,
		utxoStore:             store,
		validatorClient:       recorder,
		policyRejectedTxCache: newTxPolicyRejectedCache(1024 * 1024),
	}

	return &conflictingFixture{server: server, store: store, recorder: recorder, settings: tSettings, winner: winner, loser: loser, child: child, other: other, parent: parent, privateKey: privateKey}
}

// newServer builds a full subtree validation server over the fixture's store and real
// validator, with an in-memory subtree store, the way the peer subtree handler runs.
func (f *conflictingFixture) newServer(t *testing.T) (*Server, blob.Store) {
	t.Helper()

	return f.newServerWithStore(t, blobmemory.New())
}

// newServerWithStore is newServer over the given subtree store.
func (f *conflictingFixture) newServerWithStore(t *testing.T, subtreeStore blob.Store) (*Server, blob.Store) {
	t.Helper()

	nilConsumer := &kafka.KafkaConsumerGroup{}
	blockchainClient := teranodetestutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, f.settings, t)

	server, err := New(newCancelCtx(t), ulogger.TestLogger{}, f.settings, subtreeStore, blobmemory.New(), f.store, f.recorder, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)

	if server.policyRejectedTxCache == nil {
		server.policyRejectedTxCache = newTxPolicyRejectedCache(1024 * 1024)
	}

	if server.keptAsideTxCache == nil {
		server.keptAsideTxCache = newTxPolicyRejectedCache(1024 * 1024)
	}

	return server, subtreeStore
}

// servePeerSubtree serves txs as a peer's subtree: the node list over HTTP and the
// transactions as a local subtreeData file. It returns the subtree's root hash.
func servePeerSubtree(t *testing.T, subtreeStore blob.Store, txs ...*bt.Tx) chainhash.Hash {
	t.Helper()

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(txs))
	require.NoError(t, err)

	var data []byte

	for _, tx := range txs {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
		data = append(data, tx.ExtendedBytes()...)
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	root := *subtree.RootHash()

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)
	httpmock.RegisterResponder("GET", `=~/subtree/[a-f0-9]{64}\z`, httpmock.NewBytesResponder(200, nodeBytes))

	require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeData, data))

	return root
}

// livePassFor validates root the way the peer subtree handler does.
func livePassFor(t *testing.T, server *Server, root chainhash.Hash) error {
	t.Helper()

	_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})

	return err
}

// TestValidateSubtreeInternal_PeerSubtreeWithConflictingTxs pins how a peer subtree
// with kept-aside txs is stored: never as a validated subtree, which every reader trusts
// to have all of its txs in the UTXO store, but as a subtree still to check plus the
// kept-aside txs next to it, for the block that references it.
func TestValidateSubtreeInternal_PeerSubtreeWithConflictingTxs(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

	v := ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}

	subtree, err := server.ValidateSubtreeInternal(t.Context(), v, 101, map[uint32]bool{})
	require.NoError(t, err)
	require.Nil(t, subtree, "a subtree with kept-aside txs is not a validated subtree")

	exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "never stored as validated while txs are kept aside")

	exists, err = subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, exists, "stored as a subtree still to check, so the block path does not fetch it again")

	conflictingBytes, err := subtreeStore.Get(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
	require.NoError(t, err)

	kept, err := readConflictingTxs(conflictingBytes)
	require.NoError(t, err)
	require.Len(t, kept, 2)
	require.Equal(t, *f.loser.TxIDChainHash(), *kept[0].TxIDChainHash(), "parents before children")
	require.Equal(t, *f.child.TxIDChainHash(), *kept[1].TxIDChainHash())

	f.requireNotStored(t, f.loser, f.child)
	require.NoError(t, f.store.GetMeta(t.Context(), f.other.TxIDChainHash(), &meta.Data{}), "the rest of the subtree is validated as before")

	// a second announcement of the same subtree is not validated again
	calls := f.recorder.total()

	subtree, err = server.ValidateSubtreeInternal(t.Context(), v, 101, map[uint32]bool{})
	require.NoError(t, err)
	require.Nil(t, subtree)
	require.Equal(t, calls, f.recorder.total())
}

// TestLoadSubtreeBatch_LoadsKeptAsideTxsInsteadOfSubtreeData pins that the block path's
// prewarm does not download a peer subtree's whole subtreeData when the subtree was stored
// with kept-aside txs: the rest of its txs were validated when it arrived, so only the
// kept-aside txs need loading. Validation of every tx still happens afterwards, per subtree.
func TestLoadSubtreeBatch_LoadsKeptAsideTxsInsteadOfSubtreeData(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

	_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))

	require.Equal(t, adaptivefetch.ModePessimistic, server.adaptiveFetch.Mode(), "the prewarm only loads subtreeData when pessimistic")

	// no subtree_data responder: a download fails the call
	request := &subtreevalidation_api.CheckBlockSubtreesRequest{BaseUrl: "http://peer.invalid"}

	txs, arenas, err := server.loadSubtreeBatch(t.Context(), t.Context(), request, []chainhash.Hash{root}, "peer-1", 200)
	require.NoError(t, err)

	defer func() {
		for _, arena := range arenas {
			if arena != nil {
				putSubtreeArena(arena)
			}
		}
	}()

	hashes := make([]chainhash.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = *tx.TxIDChainHash()
	}

	require.Equal(t, []chainhash.Hash{*f.loser.TxIDChainHash(), *f.child.TxIDChainHash()}, hashes)

	t.Run("a file with a tx that is not in the subtree is ignored", func(t *testing.T) {
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting))
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting, serializeConflictingTxs([]*bt.Tx{f.winner})))

		_, _, err := server.loadSubtreeBatch(t.Context(), t.Context(), request, []chainhash.Hash{root}, "peer-1", 200)
		require.Error(t, err, "the prewarm falls back to downloading subtreeData, which this peer does not serve")
		require.Contains(t, err.Error(), "subtree_data", "the failure must be the download, not the file")
	})
}

// TestValidateSubtreeInternal_KnownConflictingTxsDoNotTriggerRetryWait pins the retry
// fix: a peer subtree's txs that the validator rejected as conflicting are never in the
// txmeta cache, and waiting for them to arrive is pointless, so they do not count towards
// the fail-fast miss threshold. Other misses still do.
func TestValidateSubtreeInternal_KnownConflictingTxsDoNotTriggerRetryWait(t *testing.T) {
	f := newConflictingFixture(t)

	f.settings.Block.FailFastValidation = true
	f.settings.BlockValidation.ValidationWarmupCount = 0
	f.settings.BlockValidation.ValidationMaxRetries = 2
	f.settings.BlockValidation.RetrySleep = 50 * time.Millisecond
	f.settings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold = 1
	f.settings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 1
	f.settings.BlockValidation.SubtreeValidationAbandonThreshold = 1

	// New wraps the UTXO store in the txmeta cache, which the fail-fast probe reads
	server, subtreeStore := f.newServer(t)
	_, isCache := server.utxoStore.(*txmetacache.TxMetaCache)
	require.True(t, isCache)

	// two misses against a threshold of one: the loser, rejected as conflicting, and its child
	root := servePeerSubtree(t, subtreeStore, f.loser, f.child)

	t.Run("unexplained misses still wait", func(t *testing.T) {
		retries := testutil.ToFloat64(prometheusSubtreeValidationValidateSubtreeRetry)

		_, _ = server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
		require.Greater(t, testutil.ToFloat64(prometheusSubtreeValidationValidateSubtreeRetry), retries)

		// the kept-aside result of this run must not short-circuit the next one
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting))
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck))
	})

	t.Run("with the loser known as conflicting", func(t *testing.T) {
		server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)

		retries := testutil.ToFloat64(prometheusSubtreeValidationValidateSubtreeRetry)

		_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
		require.NoError(t, err)
		require.Equal(t, retries, testutil.ToFloat64(prometheusSubtreeValidationValidateSubtreeRetry), "known conflicting txs must not cause a retry wait")
	})
}

// blockPathOptions are the options CheckBlockSubtrees validates a referenced subtree with.
func blockPathOptions() []validator.Option {
	return []validator.Option{
		validator.WithSkipPolicyChecks(true),
		validator.WithInBlock(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
		validator.WithUnconfirmedParentsAtCandidateHeight(true),
		validator.WithAddTXToBlockAssembly(false),
	}
}

// TestValidateSubtreeInternal_BlockInsertsKeptAsideTxs pins the other half: when a block
// references a peer subtree stored with kept-aside txs, the block path creates them as
// conflicting from the file next to the subtree, without asking the peer, and only then
// stores the subtree as validated, with the kept-aside txs in its conflicting trailer.
func TestValidateSubtreeInternal_BlockInsertsKeptAsideTxs(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

	_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err)

	// the kept-aside txs must come from the file next to the subtree: no subtreeData, no
	// policy-rejected cache entry, and no peer that serves transactions
	require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))

	subtree, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true}, 101, map[uint32]bool{}, blockPathOptions()...)
	require.NoError(t, err)
	require.NotNil(t, subtree)

	exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists)

	for _, tx := range []*bt.Tx{f.loser, f.child} {
		stored := &meta.Data{}
		require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
		require.True(t, stored.Conflicting, "%s is created as conflicting behind the block", tx.TxID())
	}

	conflictingNodes := subtree.ConflictingNodes
	require.ElementsMatch(t, []chainhash.Hash{*f.loser.TxIDChainHash(), *f.child.TxIDChainHash()}, conflictingNodes,
		"block assembly finds conflicting txs through the subtree's conflicting trailer")

	// conflicting txs are never cached, neither while kept aside nor once created
	cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
	require.True(t, ok)

	for _, tx := range []*bt.Tx{f.loser, f.child} {
		_, cached := cache.GetMetaCached(t.Context(), *tx.TxIDChainHash())
		require.False(t, cached, "%s is conflicting and must not be in the txmeta cache", tx.TxID())
	}

	// a second validation of the block reads the conflicting txs back through the cache path
	_, err = server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true, TxHashes: []chainhash.Hash{*f.loser.TxIDChainHash(), *f.child.TxIDChainHash(), *f.other.TxIDChainHash()}}, 101, map[uint32]bool{}, blockPathOptions()...)
	require.NoError(t, err)

	for _, tx := range []*bt.Tx{f.loser, f.child} {
		_, cached := cache.GetMetaCached(t.Context(), *tx.TxIDChainHash())
		require.False(t, cached, "%s is conflicting and must not be cached by a store read either", tx.TxID())
	}
}

func (f *conflictingFixture) requireNotStored(t *testing.T, txs ...*bt.Tx) {
	t.Helper()

	for _, tx := range txs {
		err := f.store.GetMeta(t.Context(), tx.TxIDChainHash(), &meta.Data{})
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "%s must not be in the UTXO store: %v", tx.TxID(), err)
	}
}

// TestBlessMissingTransactions_KeepsConflictingAside pins the peer-subtree rule: a
// transaction that conflicts with one this node accepted is kept aside, and so is its
// descendant, without either reaching the UTXO store. Only a block with proof of work
// may make the node write a conflicting record.
func TestBlessMissingTransactions_KeepsConflictingAside(t *testing.T) {
	t.Run("loser and its child in one subtree are kept aside", func(t *testing.T) {
		f := newConflictingFixture(t)

		missing := []missingTx{{tx: f.loser, idx: 0}, {tx: f.child, idx: 1}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.NoError(t, err, "a subtree whose only failures are conflicting txs must not fail")

		require.True(t, txMetaSlice[0].keptAside)
		require.True(t, txMetaSlice[1].keptAside)
		require.False(t, txMetaSlice[0].isSet)
		require.False(t, txMetaSlice[1].isSet)

		require.False(t, f.recorder.validated(*f.child.TxIDChainHash()), "a child of a kept-aside tx is never validated on the peer path")
		f.requireNotStored(t, f.loser, f.child)

		for _, opts := range f.recorder.calls[*f.loser.TxIDChainHash()] {
			require.False(t, opts.CreateConflicting, "the peer path must never create conflicting records")
		}
	})

	t.Run("child whose parent is cached as conflicting is kept aside", func(t *testing.T) {
		f := newConflictingFixture(t)
		f.server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)

		missing := []missingTx{{tx: f.child, idx: 0}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.NoError(t, err)
		require.True(t, txMetaSlice[0].keptAside)
		f.requireNotStored(t, f.child)
	})

	t.Run("child of a parent stored as conflicting is kept aside", func(t *testing.T) {
		f := newConflictingFixture(t)

		// an earlier block stored the loser as conflicting
		_, err := f.recorder.Interface.ValidateWithOptions(t.Context(), f.loser, 101, validator.ProcessOptions(validator.WithCreateConflicting(true)))
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "fixture: loser must be stored as conflicting: %v", err)

		missing := []missingTx{{tx: f.child, idx: 0}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err = f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.NoError(t, err)
		require.True(t, txMetaSlice[0].keptAside)
		f.requireNotStored(t, f.child)
	})

	t.Run("a stale conflicting flag does not keep a valid child aside", func(t *testing.T) {
		f := newConflictingFixture(t)

		// the winner is accepted in the store, but cached as conflicting, as after a
		// reorg made a cached loser win
		f.server.policyRejectedTxCache.SetConflicting(*f.winner.TxIDChainHash(), f.winner)

		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_KEPT_ASIDE_TEST_KEY!"))
		winnerChild := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(f.winner, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 85_000, publicKey),
		)

		missing := []missingTx{{tx: winnerChild, idx: 0}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.NoError(t, err)
		require.False(t, txMetaSlice[0].keptAside, "a child whose parent is in the store is validated, whatever the cache says")
		require.True(t, txMetaSlice[0].isSet)
		require.NoError(t, f.store.GetMeta(t.Context(), winnerChild.TxIDChainHash(), &meta.Data{}))
	})

	t.Run("a malformed descendant is not kept aside", func(t *testing.T) {
		for _, parentCached := range []bool{false, true} {
			f := newConflictingFixture(t)

			badChild := f.child.Clone()
			dup := *badChild.Inputs[0]
			badChild.Inputs = append(badChild.Inputs, &dup)

			missing := []missingTx{{tx: badChild, idx: 0}}
			if parentCached {
				f.server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)
			} else {
				missing = []missingTx{{tx: f.loser, idx: 0}, {tx: badChild, idx: 1}}
			}

			txMetaSlice := make([]metaSliceItem, len(missing))

			err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
			require.True(t, errors.Is(err, errors.ErrTxInvalid), "parentCached=%v: a malformed descendant fails the subtree as invalid: %v", parentCached, err)
			require.False(t, txMetaSlice[len(missing)-1].keptAside)
		}
	})

	t.Run("a stale flag on a stored parent does not explain another missing parent", func(t *testing.T) {
		f := newConflictingFixture(t)

		// the winner is stored, but carries a stale conflicting flag
		f.server.policyRejectedTxCache.SetConflicting(*f.winner.TxIDChainHash(), f.winner)

		// unknown spends the winner and a parent nobody knows: the child (not stored)
		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_KEPT_ASIDE_TEST_KEY!"))
		mixed := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(f.winner, 0, privateKey),
			transactions.WithInput(f.child, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 100_000, publicKey),
		)

		missing := []missingTx{{tx: mixed, idx: 0}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.Error(t, err, "the missing parent is not known to conflict, so the subtree fails as before")
		require.False(t, txMetaSlice[0].keptAside)
	})

	t.Run("child whose parent is unknown still fails", func(t *testing.T) {
		f := newConflictingFixture(t)

		missing := []missingTx{{tx: f.child, idx: 0}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.Error(t, err, "a missing parent that is not known to conflict keeps failing the subtree")
		require.False(t, txMetaSlice[0].keptAside)
	})

	t.Run("block path creates the loser as conflicting instead", func(t *testing.T) {
		f := newConflictingFixture(t)

		missing := []missingTx{{tx: f.loser, idx: 0}, {tx: f.child, idx: 1}}
		txMetaSlice := make([]metaSliceItem, len(missing))

		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, false,
			validator.WithCreateConflicting(true))
		require.NoError(t, err)

		for i := range missing {
			require.False(t, txMetaSlice[i].keptAside, "nothing is kept aside behind proof of work")
			require.True(t, txMetaSlice[i].isSet)
			require.True(t, txMetaSlice[i].conflicting)
		}
	})
}

func TestReadConflictingTxs_TruncatedIsAnError(t *testing.T) {
	f := newConflictingFixture(t)

	b := serializeConflictingTxs([]*bt.Tx{f.loser, f.child})

	txs, err := readConflictingTxs(b)
	require.NoError(t, err)
	require.Len(t, txs, 2)

	// cutting the 4-byte locktime ends the last tx at a field boundary, where the reader
	// sees a bare io.EOF
	_, err = readConflictingTxs(b[:len(b)-4])
	require.Error(t, err, "a truncated file must not read as a shorter list")
}

// TestValidateSubtreeInternal_KeptAsideEdgeCases pins the review fixes around the
// kept-aside path: it still rejects a corrupt subtree, it does not trust a sidecar
// without its subtree to check, and a block falls back to the peer on an unreadable sidecar.
func TestValidateSubtreeInternal_KeptAsideEdgeCases(t *testing.T) {
	livePass := func(t *testing.T, server *Server, root chainhash.Hash) error {
		_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
		return err
	}

	t.Run("a duplicate tx is still rejected as corrupt", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.other, f.other)

		err := livePass(t, server, root)
		require.True(t, errors.Is(err, errors.ErrBlockCorrupt), "a duplicate stays BlockCorrupt with a kept-aside tx present: %v", err)

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("a sidecar without its subtree to check is not skipped", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)
		require.NoError(t, livePass(t, server, root))

		// as after a crash between the two writes
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck))

		calls := f.recorder.total()
		require.NoError(t, livePass(t, server, root))
		require.Greater(t, f.recorder.total(), calls, "the subtree is validated again")

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("a tx with a frozen input besides the spent one is not kept aside", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		frozenParent := transactions.Create(t,
			transactions.WithCoinbaseData(3, "/conflicting kept aside test frozen/"),
			transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
		)
		_, err := f.store.Create(t.Context(), frozenParent, 3, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 3, BlockHeight: 3, SubtreeIdx: 0}))
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(frozenParent.TxIDChainHash(), frozenParent.Outputs[0], 0)
		require.NoError(t, err)
		require.NoError(t, f.store.FreezeUTXOs(t.Context(), []*utxostore.Spend{{TxID: frozenParent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}}, f.settings))

		invalid := transactions.Create(t,
			transactions.WithPrivateKey(f.privateKey),
			transactions.WithInput(f.parent, 0, f.privateKey),
			transactions.WithInput(frozenParent, 0, f.privateKey),
			transactions.WithP2PKHOutputs(1, 150_000, f.privateKey.PubKey()),
		)

		root := servePeerSubtree(t, subtreeStore, invalid, f.other)

		require.Error(t, livePass(t, server, root), "the subtree fails as before kept-aside txs existed")

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)
		require.False(t, exists)

		known := server.knownConflicting(*invalid.TxIDChainHash())
		require.False(t, known, "an invalid tx is not cached as conflicting")
	})

	t.Run("a tx spending a conflicting tx's output and a frozen output is not kept aside", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		// the loser stored as conflicting, the way a block stores it
		_, err := f.recorder.Interface.Validate(t.Context(), f.loser, 101, validator.WithCreateConflicting(true))
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

		frozenParent := transactions.Create(t,
			transactions.WithCoinbaseData(4, "/conflicting kept aside test frozen 2/"),
			transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
		)
		_, err = f.store.Create(t.Context(), frozenParent, 4, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 4, BlockHeight: 4, SubtreeIdx: 0}))
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(frozenParent.TxIDChainHash(), frozenParent.Outputs[0], 0)
		require.NoError(t, err)
		require.NoError(t, f.store.FreezeUTXOs(t.Context(), []*utxostore.Spend{{TxID: frozenParent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}}, f.settings))

		invalid := transactions.Create(t,
			transactions.WithPrivateKey(f.privateKey),
			transactions.WithInput(f.loser, 0, f.privateKey),
			transactions.WithInput(frozenParent, 0, f.privateKey),
			transactions.WithP2PKHOutputs(1, 150_000, f.privateKey.PubKey()),
		)

		root := servePeerSubtree(t, subtreeStore, invalid, f.other)

		require.Error(t, livePass(t, server, root), "the subtree fails as before kept-aside txs existed")

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)
		require.False(t, exists)
		require.False(t, server.knownConflicting(*invalid.TxIDChainHash()))
	})

	t.Run("the block path never keeps txs aside, whatever its validator options", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

		// a block-path caller that forgot WithCreateConflicting
		_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true}, 101, map[uint32]bool{}, validator.WithSkipPolicyChecks(true))
		require.Error(t, err, "a block's conflicting tx fails the subtree instead of being kept aside")

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("a stale sidecar left by an interrupted pass is replaced", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

		// as after a pass that kept only the loser aside and stopped before the subtree to check
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting, serializeConflictingTxs([]*bt.Tx{f.loser})))

		require.NoError(t, livePass(t, server, root))

		b, err := subtreeStore.Get(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)

		kept, err := readConflictingTxs(b)
		require.NoError(t, err)
		require.Len(t, kept, 2, "the sidecar next to the subtree to check is this pass's kept-aside set")
		require.Equal(t, *f.child.TxIDChainHash(), *kept[1].TxIDChainHash())
	})

	t.Run("the block path reads the sidecar before asking the peer for subtree data", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)
		require.NoError(t, livePass(t, server, root))

		// the kept-aside txs are two thirds of the subtree, far above the full-data threshold
		server.settings.SubtreeValidation.PercentageMissingGetFullData = 20
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))

		httpmock.RegisterResponder("GET", `=~/subtree_data/[a-f0-9]{64}\z`, httpmock.NewStringResponder(404, "not found"))

		subtree, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true}, 101, map[uint32]bool{}, blockPathOptions()...)
		require.NoError(t, err)
		require.NotNil(t, subtree)

		for call, n := range httpmock.GetCallCountInfo() {
			if strings.Contains(call, "subtree_data") {
				require.Zero(t, n, "every missing tx is in the local sidecar: %s", call)
			}
		}
	})

	// The sidecar holds some of the missing txs, the rest are fetched: both must be validated.
	partialSidecar := func(t *testing.T, subtreeDataStatus int) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting, serializeConflictingTxs([]*bt.Tx{f.loser, f.child})))

		// other is a third of the subtree, above the full-data threshold
		server.settings.SubtreeValidation.PercentageMissingGetFullData = 20

		var data []byte
		for _, tx := range []*bt.Tx{f.loser, f.child, f.other} {
			data = append(data, tx.ExtendedBytes()...)
		}

		httpmock.RegisterResponder("GET", `=~/subtree_data/[a-f0-9]{64}\z`, httpmock.NewBytesResponder(subtreeDataStatus, data))
		httpmock.RegisterResponder("POST", `=~/subtree/[a-f0-9]{64}/txs\z`, httpmock.NewBytesResponder(200, f.other.ExtendedBytes()))

		subtree, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true}, 101, map[uint32]bool{}, blockPathOptions()...)
		require.NoError(t, err)
		require.NotNil(t, subtree)

		for _, tx := range []*bt.Tx{f.loser, f.child, f.other} {
			require.True(t, f.recorder.validated(*tx.TxIDChainHash()), "every missing tx is validated: %s", tx.TxIDChainHash())
		}
	}

	t.Run("a partial sidecar plus downloaded subtree data", func(t *testing.T) {
		partialSidecar(t, 200)
	})

	t.Run("a partial sidecar plus txs from the peer when it has no subtree data", func(t *testing.T) {
		partialSidecar(t, 404)
	})

	t.Run("an unreadable sidecar falls back to the peer on the block path", func(t *testing.T) {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)
		require.NoError(t, livePass(t, server, root))

		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting))
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting, []byte{0x01, 0x00, 0x00}))

		httpmock.RegisterResponder("POST", `=~/subtree/[a-f0-9]{64}/txs\z`,
			httpmock.NewBytesResponder(200, append(f.loser.ExtendedBytes(), f.child.ExtendedBytes()...)))

		subtree, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", FromBlock: true}, 101, map[uint32]bool{}, blockPathOptions()...)
		require.NoError(t, err)
		require.NotNil(t, subtree)
	})
}

// TestValidateSubtreeInternal_PublishesKeptAsideDescendants pins how a kept-aside
// descendant becomes known to every subtree validator: it is published, raw and flagged
// keptAside, on the policy-rejected topic the validators publish their conflicting
// rejections on, and another subtree validator caches it in its kept-aside budget. A tx the
// validator already reported is not published again. The producer is the one Server.New
// builds from kafka_txPolicyRejectedConfig, over the in-memory broker.
func TestValidateSubtreeInternal_PublishesKeptAsideDescendants(t *testing.T) {
	topic := "tx-policy-rejected-kept-aside-publish"

	topicURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)

	f := newConflictingFixture(t)
	f.settings.Kafka.TxPolicyRejected = topic
	f.settings.Kafka.TxPolicyRejectedConfig = topicURL

	server, subtreeStore := f.newServer(t)
	require.NotNil(t, server.conflictingTxKafkaProducer, "New builds the kept-aside producer from the topic config")

	// another subtree validator, consuming the topic
	other := &Server{
		logger:                ulogger.TestLogger{},
		policyRejectedTxCache: newTxPolicyRejectedCache(1024 * 1024),
		keptAsideTxCache:      newTxPolicyRejectedCache(1024 * 1024),
	}

	var (
		publishedMu sync.Mutex
		published   []chainhash.Hash
	)

	consumer := setupMemoryKafkaConsumer(t, topic)
	defer consumer.Close()

	handler := other.policyRejectedTxMessageHandler(t.Context())
	consumer.Start(t.Context(), func(msg *kafka.KafkaMessage) error {
		var m kafkamessage.KafkaTxPolicyRejectedTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err == nil && m.KeptAside {
			publishedMu.Lock()
			published = append(published, chainhash.Hash(m.TxHash))
			publishedMu.Unlock()
		}

		return handler(msg)
	}, kafka.WithLogErrorAndMoveOn())

	// in-memory consumer registration is async
	time.Sleep(50 * time.Millisecond)

	// the validator already published the loser
	server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

	_, err = server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return other.keptAsideTxCache.Conflicting(*f.child.TxIDChainHash())
	}, 5*time.Second, 10*time.Millisecond, "the other subtree validator learns the descendant from the topic")

	require.False(t, other.policyRejectedTxCache.Conflicting(*f.child.TxIDChainHash()), "a descendant never takes space from the validators' entries")

	require.True(t, server.keptAsideTxCache.Conflicting(*f.child.TxIDChainHash()), "this validator knows the descendant without waiting for the topic")
	require.False(t, server.policyRejectedTxCache.Conflicting(*f.child.TxIDChainHash()))

	// give a stray publish of the loser time to arrive
	time.Sleep(100 * time.Millisecond)

	publishedMu.Lock()
	defer publishedMu.Unlock()

	require.Equal(t, []chainhash.Hash{*f.child.TxIDChainHash()}, published, "only the descendant is new knowledge")
}

// TestValidateSubtreeInternal_GrandchildInLaterSubtreeIsKeptAside pins the chain across
// subtrees: the loser and its child kept aside in one subtree, the grandchild in a later
// one. The child is known as conflicting once its subtree is stored, so the grandchild's
// subtree is stored with it kept aside instead of failing.
func TestValidateSubtreeInternal_GrandchildInLaterSubtreeIsKeptAside(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_KEPT_ASIDE_TEST_KEY!"))
	grandchild := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(f.child, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 60_000, publicKey),
	)

	first := servePeerSubtree(t, subtreeStore, f.loser, f.child)
	_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: first, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err)

	second := servePeerSubtree(t, subtreeStore, grandchild, f.other)
	_, err = server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: second, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err, "the grandchild's subtree must not fail")

	b, err := subtreeStore.Get(t.Context(), second[:], fileformat.FileTypeSubtreeConflicting)
	require.NoError(t, err)

	kept, err := readConflictingTxs(b)
	require.NoError(t, err)
	require.Len(t, kept, 1)
	require.Equal(t, *grandchild.TxIDChainHash(), *kept[0].TxIDChainHash())
	f.requireNotStored(t, grandchild)
}

// TestValidateSubtreeInternal_DuplicateRejectedBeforeValidation pins that a subtree listing
// a tx twice is rejected before any of its txs is validated, on the peer path and on the
// block path, even when another of its txs is invalid.
func TestValidateSubtreeInternal_DuplicateRejectedBeforeValidation(t *testing.T) {
	for _, fromBlock := range []bool{false, true} {
		f := newConflictingFixture(t)
		server, subtreeStore := f.newServer(t)

		invalid := f.other.Clone()
		invalid.Outputs[0].Satoshis = 21_000_000*100_000_000 + 1

		root := servePeerSubtree(t, subtreeStore, invalid, f.loser, f.loser)

		v := ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: !fromBlock, FromBlock: fromBlock}

		var opts []validator.Option
		if fromBlock {
			opts = blockPathOptions()
		}

		_, err := server.ValidateSubtreeInternal(t.Context(), v, 101, map[uint32]bool{}, opts...)
		require.True(t, errors.Is(err, errors.ErrBlockCorrupt), "fromBlock=%v: the duplicate decides, not the invalid tx: %v", fromBlock, err)
		require.Zero(t, f.recorder.total(), "fromBlock=%v: nothing is validated before the duplicate is found", fromBlock)
	}
}

// sidecarExistsCounter counts Exists calls for kept-aside conflicting txs.
type sidecarExistsCounter struct {
	blob.Store
	n atomic.Int32
}

func (c *sidecarExistsCounter) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if fileType == fileformat.FileTypeSubtreeConflicting {
		c.n.Add(1)
	}

	return c.Store.Exists(ctx, key, fileType, opts...)
}

// TestValidateSubtreeInternal_PeerPathChecksForKeptAsideTxsOnce pins the store round trips a
// new peer subtree pays for kept-aside txs: one check, alongside the subtree's own, and none
// when its missing txs are fetched.
func TestValidateSubtreeInternal_PeerPathChecksForKeptAsideTxsOnce(t *testing.T) {
	f := newConflictingFixture(t)
	counter := &sidecarExistsCounter{Store: blobmemory.New()}
	server, subtreeStore := f.newServerWithStore(t, counter)

	root := servePeerSubtree(t, subtreeStore, f.other)
	require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))

	httpmock.RegisterResponder("GET", `=~/subtree_data/[a-f0-9]{64}\z`, httpmock.NewBytesResponder(200, f.other.ExtendedBytes()))

	_, err := server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{SubtreeHash: root, BaseURL: "http://peer.invalid", PeerID: "peer-1", AllowFailFast: true}, 101, map[uint32]bool{})
	require.NoError(t, err)
	require.Equal(t, int32(1), counter.n.Load())
}

// TestGetSubtreeMissingTxs_SubtreeOrderAcrossSources pins that missing txs resolved from
// different sources come back in subtree order: the level builder relies on parents
// coming before children, and here a kept-aside child's parent is fetched.
func TestGetSubtreeMissingTxs_SubtreeOrderAcrossSources(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	otherChild := transactions.Create(t,
		transactions.WithPrivateKey(f.privateKey),
		transactions.WithInput(f.other, 0, f.privateKey),
		transactions.WithP2PKHOutputs(1, 50_000, f.privateKey.PubKey()),
	)

	root := servePeerSubtree(t, subtreeStore, f.other, otherChild)
	require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))
	require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting, serializeConflictingTxs([]*bt.Tx{otherChild})))

	httpmock.RegisterResponder("POST", `=~/subtree/[a-f0-9]{64}/txs\z`, httpmock.NewBytesResponder(200, f.other.ExtendedBytes()))
	server.settings.SubtreeValidation.PercentageMissingGetFullData = 100

	allTxs := []chainhash.Hash{*f.other.TxIDChainHash(), *otherChild.TxIDChainHash()}
	missing := []utxostore.UnresolvedMetaData{{Hash: allTxs[0], Idx: 0}, {Hash: allTxs[1], Idx: 1}}

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)

	missingTxs, err := server.getSubtreeMissingTxs(t.Context(), root, subtree, missing, allTxs, "http://peer.invalid", "peer-1", true)
	require.NoError(t, err)
	require.Len(t, missingTxs, 2)
	require.Equal(t, 0, missingTxs[0].idx, "the fetched parent comes first")
	require.Equal(t, 1, missingTxs[1].idx)
}

// TestBlessMissing_RemoteValidatorConflictingCreateReadsMetaBack pins the remote-validator
// case: a gRPC error drops the response metadata, so a block's tx the validator has just
// created as conflicting arrives as (nil, TX_CONFLICTING). The record is read back instead
// of failing the block's subtree as "tx meta is nil".
func TestBlessMissing_RemoteValidatorConflictingCreateReadsMetaBack(t *testing.T) {
	f := newConflictingFixture(t)

	_, err := f.recorder.Interface.Validate(t.Context(), f.loser, 101, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

	f.server.validatorClient = &failingValidator{Interface: f.recorder.Interface, err: errors.NewTxConflictingError("tx is conflicting")}

	txMeta, err := f.server.blessMissingTransaction(t.Context(), chainhash.Hash{}, chainhash.Hash{}, f.loser, 101, map[uint32]bool{},
		validator.ProcessOptions(validator.WithCreateConflicting(true)))
	require.NoError(t, err)
	require.NotNil(t, txMeta)
	require.True(t, txMeta.Conflicting)
}

// TestValidateSubtreeInternal_KeptAsideTxsDropPeerExtendedData pins the kept-aside memory
// bound: a descendant is only context-free checked, so the previous-output data a peer
// attaches to it in extended format is never checked against anything. It is dropped
// before the tx is cached, published or written next to the subtree, and the caches count
// what an entry can retain, not just its plain size.
func TestValidateSubtreeInternal_KeptAsideTxsDropPeerExtendedData(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	padded := f.child.Clone()
	padded.Inputs[0].PreviousTxScript = bscript.NewFromBytes(make([]byte, 200_000))
	require.True(t, padded.IsExtended())

	root := servePeerSubtree(t, subtreeStore, f.loser, padded, f.other)
	require.NoError(t, livePassFor(t, server, root))

	b, err := subtreeStore.Get(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
	require.NoError(t, err)
	require.Less(t, len(b), 10_000, "the sidecar holds the kept-aside txs without the peer's extended data")

	kept, err := readConflictingTxs(b)
	require.NoError(t, err)
	require.Len(t, kept, 2)
	require.Equal(t, *f.child.TxIDChainHash(), *kept[1].TxIDChainHash())
	require.False(t, kept[1].IsExtended())

	require.Less(t, server.keptAsideTxCache.Bytes(), 10_000, "the cached descendant is stripped too")

	cached, ok := server.keptAsideTxCache.Get(*f.child.TxIDChainHash())
	require.True(t, ok)
	require.False(t, cached.IsExtended())
}

// TestTxPolicyRejectedCache_CountsExtendedData pins that an entry is budgeted by what it
// can retain: an extended tx holds every input's previous output.
func TestTxPolicyRejectedCache_CountsExtendedData(t *testing.T) {
	f := newConflictingFixture(t)

	padded := f.child.Clone()
	padded.Inputs[0].PreviousTxScript = bscript.NewFromBytes(make([]byte, 200_000))

	cache := newTxPolicyRejectedCache(1024 * 1024)
	cache.SetConflicting(*padded.TxIDChainHash(), padded)

	require.GreaterOrEqual(t, cache.Bytes(), 200_000)
}

// TestValidateSubtreeInternal_CachedKeptAsideTxsAreNotWrittenNextToTheSubtree pins the
// replay guard: a peer can announce fresh subtrees of hashes this node already holds in
// its caches, at 32 bytes a tx. Only the kept-aside txs the peer sent are written next to
// the subtree; those resolved from the caches are resolved again when a block needs them.
func TestValidateSubtreeInternal_CachedKeptAsideTxsAreNotWrittenNextToTheSubtree(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	// the validator's publish of the loser
	server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child)
	require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))

	// the peer serves no subtree data and only the child's bytes
	httpmock.RegisterResponder("GET", `=~/subtree_data/[a-f0-9]{64}\z`, httpmock.NewBytesResponder(200, nil))
	httpmock.RegisterResponder("POST", `=~/subtree/[a-f0-9]{64}/txs\z`, httpmock.NewBytesResponder(200, f.child.ExtendedBytes()))

	require.NoError(t, livePassFor(t, server, root))

	b, err := subtreeStore.Get(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
	require.NoError(t, err)

	kept, err := readConflictingTxs(b)
	require.NoError(t, err)
	require.Len(t, kept, 1, "only the tx the peer sent")
	require.Equal(t, *f.child.TxIDChainHash(), *kept[0].TxIDChainHash())

	exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, exists, "the subtree waits for its block as before")
}

// TestValidateSubtreeInternal_KeptAsideOverTheCapFailsTheSubtree pins the per-subtree cap
// on kept-aside bytes: over it, the subtree fails as it did before txs were kept aside.
func TestValidateSubtreeInternal_KeptAsideOverTheCapFailsTheSubtree(t *testing.T) {
	f := newConflictingFixture(t)
	server, subtreeStore := f.newServer(t)

	server.settings.SubtreeValidation.KeptAsideMaxBytesPerSubtree = f.loser.Size()

	root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

	require.Error(t, livePassFor(t, server, root))

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtreeConflicting, fileformat.FileTypeSubtreeToCheck} {
		exists, err := subtreeStore.Exists(t.Context(), root[:], fileType)
		require.NoError(t, err)
		require.False(t, exists, fileType)
	}
}

// TestBlessMissing_ReadBackOnlyOnTheValidatorsConflictingVerdict pins that the read-back
// never turns a rejection into success. A block's tx spending an output of a conflicting tx
// and an immature coinbase is rejected, with the store's per-input TX_CONFLICTING inside the
// error. Another candidate block, at a height where the coinbase is mature, may create the
// same tx as conflicting meanwhile; that record must not stand in for this rejection.
func TestBlessMissing_ReadBackOnlyOnTheValidatorsConflictingVerdict(t *testing.T) {
	f := newConflictingFixture(t)
	realValidator := f.recorder.Interface

	// the loser stored as conflicting, the way a block stores it
	_, err := realValidator.Validate(t.Context(), f.loser, 101, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

	// a coinbase that matures one block after 101
	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(101, "/read back immature coinbase/"),
		transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
	)
	_, err = f.store.Create(t.Context(), coinbase, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 101, BlockHeight: 101, SubtreeIdx: 0}))
	require.NoError(t, err)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(f.privateKey),
		transactions.WithInput(f.loser, 0, f.privateKey),
		transactions.WithInput(coinbase, 0, f.privateKey),
		transactions.WithP2PKHOutputs(1, 150_000, f.privateKey.PubKey()),
	)

	// at 101 the validator rejects it: the coinbase is immature
	_, rejection := realValidator.Validate(t.Context(), tx.Clone(), 101, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(rejection, errors.ErrTxCoinbaseImmature), "%v", rejection)
	require.True(t, errors.Is(rejection, errors.ErrTxConflicting), "the store's per-input TX_CONFLICTING is in the chain: %v", rejection)

	// another candidate, at a height where the coinbase is mature, creates it as conflicting
	_, created := realValidator.Validate(t.Context(), tx.Clone(), 102, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(created, errors.ErrTxConflicting), "%v", created)

	// the verdicts as a remote validator's client hands them back
	require.True(t, validatorCreatedConflicting(errors.UnwrapGRPC(errors.WrapGRPC(created))), "created as conflicting: %v", created)
	require.False(t, validatorCreatedConflicting(errors.UnwrapGRPC(errors.WrapGRPC(rejection))), "rejected: %v", rejection)

	stored := &meta.Data{}
	require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.True(t, stored.Conflicting, "precondition: the record the read-back would find")

	// this candidate's validator answer arrives after that
	f.server.validatorClient = &failingValidator{Interface: realValidator, err: rejection}

	txMeta, err := f.server.blessMissingTransaction(t.Context(), chainhash.Hash{}, chainhash.Hash{}, tx, 101, map[uint32]bool{},
		validator.ProcessOptions(validator.WithCreateConflicting(true)))
	require.Error(t, err, "the rejection stands")
	require.Nil(t, txMeta)
}

// parentReadCounter counts the UTXO store reads of one tx.
type parentReadCounter struct {
	utxostore.Store
	hash chainhash.Hash
	n    atomic.Int32
}

func (c *parentReadCounter) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if *hash == c.hash {
		c.n.Add(1)
	}

	return c.Store.GetMeta(ctx, hash, data)
}

func (c *parentReadCounter) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if *hash == c.hash {
		c.n.Add(1)
	}

	return c.Store.Get(ctx, hash, f...)
}

// TestBlessMissingTransactions_ConflictingParentReadOncePerParent pins the cost of the
// conflicting-parent check: one store read per distinct parent known as conflicting, not
// one per input spending it.
func TestBlessMissingTransactions_ConflictingParentReadOncePerParent(t *testing.T) {
	f := newConflictingFixture(t)

	// a parent known as conflicting that is nevertheless in the store, with five outputs
	parent := transactions.Create(t,
		transactions.WithCoinbaseData(5, "/conflicting parent read once/"),
		transactions.WithP2PKHOutputs(5, 10_000, f.privateKey.PubKey()),
	)
	_, err := f.store.Create(t.Context(), parent, 5, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 5, BlockHeight: 5, SubtreeIdx: 0}))
	require.NoError(t, err)

	f.server.policyRejectedTxCache.SetConflicting(*parent.TxIDChainHash(), parent)

	// an absent parent makes the validator report a missing parent
	absent := transactions.Create(t,
		transactions.WithCoinbaseData(6, "/conflicting parent read once absent/"),
		transactions.WithP2PKHOutputs(1, 10_000, f.privateKey.PubKey()),
	)

	opts := []transactions.TxOption{transactions.WithPrivateKey(f.privateKey), transactions.WithInput(absent, 0, f.privateKey)}
	for vout := uint32(0); vout < 5; vout++ {
		opts = append(opts, transactions.WithInput(parent, vout, f.privateKey))
	}

	tx := transactions.Create(t, append(opts, transactions.WithP2PKHOutputs(1, 40_000, f.privateKey.PubKey()))...)

	counter := &parentReadCounter{Store: f.store, hash: *parent.TxIDChainHash()}
	f.server.utxoStore = counter

	txMetaSlice := make([]metaSliceItem, 1)
	_ = f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, []missingTx{{tx: tx, idx: 0}}, "", "", txMetaSlice, 101, map[uint32]bool{}, true)

	require.Equal(t, int32(1), counter.n.Load())
}

// TestBlockPathValidatesKeptAsideSubtree pins the half of the design that makes keeping txs
// aside safe: every block entry point validates a peer subtree stored with kept-aside txs,
// creates them as conflicting and stores the subtree as validated. A block caller that lost
// FromBlock or WithCreateConflicting would return success here with the subtree still
// unvalidated.
func TestBlockPathValidatesKeptAsideSubtree(t *testing.T) {
	// withChild adds a child of the loser; only the paths that resolve same-block parents
	// at the candidate height can validate it
	keptAsideSubtree := func(t *testing.T, withChild bool, fetchMode string) (*conflictingFixture, *Server, blob.Store, chainhash.Hash) {
		f := newConflictingFixture(t)
		f.settings.AdaptiveFetch.BootstrapMode = fetchMode
		server, subtreeStore := f.newServer(t)

		// a node arms adaptive fetch once its FSM reaches RUNNING
		server.adaptiveFetch.Arm()
		require.Equal(t, fetchMode, server.adaptiveFetch.Mode().String())

		txs := []*bt.Tx{f.loser, f.other}
		if withChild {
			txs = []*bt.Tx{f.loser, f.child, f.other}
		}

		root := servePeerSubtree(t, subtreeStore, txs...)
		require.NoError(t, livePassFor(t, server, root))

		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeConflicting)
		require.NoError(t, err)
		require.True(t, exists, "precondition: the peer pass kept txs aside")

		// the kept-aside file is then the only local source of the kept-aside txs, as on a
		// pod that did not run the peer pass, and the peer cannot serve them either
		require.NoError(t, subtreeStore.Del(t.Context(), root[:], fileformat.FileTypeSubtreeData))
		server.policyRejectedTxCache.clearForTest()
		server.keptAsideTxCache.clearForTest()
		httpmock.RegisterResponder("GET", `=~/subtree_data/[a-f0-9]{64}\z`, httpmock.NewStringResponder(404, "not found"))
		httpmock.RegisterResponder("POST", `=~/subtree/[a-f0-9]{64}/txs\z`, httpmock.NewStringResponder(404, "not found"))

		return f, server, subtreeStore, root
	}

	requireValidated := func(t *testing.T, f *conflictingFixture, subtreeStore blob.Store, root chainhash.Hash, withChild bool) {
		exists, err := subtreeStore.Exists(t.Context(), root[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.True(t, exists, "the block path stores the subtree as validated")

		created := []*bt.Tx{f.loser}
		if withChild {
			created = append(created, f.child)
		}

		for _, tx := range created {
			data := &meta.Data{}
			require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), data), "the block path creates %s", tx.TxIDChainHash())
			require.True(t, data.Conflicting, "%s is created as conflicting", tx.TxIDChainHash())
		}
	}

	genesis := func(f *conflictingFixture) *chainhash.Hash {
		return f.settings.ChainCfgParams.GenesisHash
	}

	// the peer-facing branch does not resolve same-block parents at the candidate height, by
	// design, so its subtree holds no child; the legacy branch and CheckBlockSubtrees do
	for _, baseURL := range []string{"http://peer.invalid", "legacy"} {
		t.Run("checkSubtreeFromBlock "+baseURL, func(t *testing.T) {
			withChild := baseURL == "legacy"
			f, server, subtreeStore, root := keptAsideSubtree(t, withChild, "pessimistic")

			blockHash := chainhash.HashH([]byte("kept aside block"))

			ok, err := server.checkSubtreeFromBlock(t.Context(), &subtreevalidation_api.CheckSubtreeFromBlockRequest{
				Hash:              root[:],
				BaseUrl:           baseURL,
				BlockHeight:       101,
				BlockHash:         blockHash[:],
				PreviousBlockHash: genesis(f)[:],
			})
			require.NoError(t, err)
			require.True(t, ok)

			requireValidated(t, f, subtreeStore, root, withChild)
		})
	}

	// optimistic fetch skips the prewarm, so the kept-aside txs are created by the
	// per-subtree validation instead
	for _, fetchMode := range []string{"pessimistic", "optimistic"} {
		t.Run("CheckBlockSubtrees "+fetchMode, func(t *testing.T) {
			f, server, subtreeStore, root := keptAsideSubtree(t, true, fetchMode)

			header := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  genesis(f),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
				Bits:           model.NBit{},
			}

			block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 4, 1000, 101, 0)
			require.NoError(t, err)

			blockBytes, err := block.Bytes()
			require.NoError(t, err)

			resp, err := server.CheckBlockSubtrees(t.Context(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "http://peer.invalid"})
			require.NoError(t, err)
			require.True(t, resp.Blessed)

			requireValidated(t, f, subtreeStore, root, true)
		})
	}
}

// clearForTest empties the cache, as on a pod that never saw its entries.
func (c *txPolicyRejectedCache) clearForTest() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[chainhash.Hash]cacheEntry)
	c.curBytes = 0
}

// TestBlessMissingTransactions_KeptAsideTxsAreKnownAtOnce pins the sibling-subtree window:
// a kept-aside tx is known as conflicting on this pod as soon as it is kept aside, not once
// its subtree is stored, so a child in a sibling subtree validated at the same time is kept
// aside too. The cached copy carries no extended data.
func TestBlessMissingTransactions_KeptAsideTxsAreKnownAtOnce(t *testing.T) {
	f := newConflictingFixture(t)
	f.server.keptAsideTxCache = newTxPolicyRejectedCache(1024 * 1024)

	missing := []missingTx{{tx: f.loser, idx: 0}, {tx: f.child, idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	require.NoError(t, f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, missing, "", "", txMetaSlice, 101, map[uint32]bool{}, true))
	require.True(t, txMetaSlice[1].keptAside)

	for _, tx := range []*bt.Tx{f.loser, f.child} {
		require.True(t, f.server.knownConflicting(*tx.TxIDChainHash()), "%s is known before its subtree is stored", tx.TxIDChainHash())

		cached, ok := f.server.keptAsideTxCache.Get(*tx.TxIDChainHash())
		require.True(t, ok)
		require.False(t, cached.IsExtended())
	}
}

// faultySubtreeStore fails the calls for chosen file types.
type faultySubtreeStore struct {
	blob.Store
	existsErr map[fileformat.FileType]error
	getErr    map[fileformat.FileType]error
	setErr    map[fileformat.FileType]error
	// existsTrue reports these file types as present, whatever the store holds
	existsTrue map[fileformat.FileType]bool
}

func (s *faultySubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if err := s.existsErr[fileType]; err != nil {
		return false, err
	}

	if s.existsTrue[fileType] {
		return true, nil
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

func (s *faultySubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if err := s.getErr[fileType]; err != nil {
		return nil, err
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

func (s *faultySubtreeStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if err := s.setErr[fileType]; err != nil {
		return err
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestKeptAsideStoreErrorsHalt pins the contract every kept-aside read and write states: a
// storage error halts the operation, and only a missing or unreadable file means "none".
func TestKeptAsideStoreErrorsHalt(t *testing.T) {
	storageErr := errors.NewStorageError("disk on fire")

	t.Run("a failed kept-aside write fails the subtree and writes no subtree to check", func(t *testing.T) {
		f := newConflictingFixture(t)
		faulty := &faultySubtreeStore{Store: blobmemory.New(), setErr: map[fileformat.FileType]error{fileformat.FileTypeSubtreeConflicting: storageErr}}
		server, subtreeStore := f.newServerWithStore(t, faulty)

		root := servePeerSubtree(t, subtreeStore, f.loser, f.child, f.other)

		require.True(t, errors.Is(livePassFor(t, server, root), errors.ErrStorageError))

		exists, err := faulty.Store.Exists(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("a failed kept-aside check on the peer path fails the subtree", func(t *testing.T) {
		f := newConflictingFixture(t)
		faulty := &faultySubtreeStore{Store: blobmemory.New(), existsErr: map[fileformat.FileType]error{fileformat.FileTypeSubtreeConflicting: storageErr}}
		server, subtreeStore := f.newServerWithStore(t, faulty)

		root := servePeerSubtree(t, subtreeStore, f.other)

		require.True(t, errors.Is(livePassFor(t, server, root), errors.ErrStorageError))
	})

	t.Run("a failed kept-aside read halts both lookups", func(t *testing.T) {
		f := newConflictingFixture(t)
		faulty := &faultySubtreeStore{
			Store:      blobmemory.New(),
			getErr:     map[fileformat.FileType]error{fileformat.FileTypeSubtreeConflicting: storageErr},
			existsTrue: map[fileformat.FileType]bool{fileformat.FileTypeSubtreeConflicting: true},
		}
		server, _ := f.newServerWithStore(t, faulty)

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*f.loser.TxIDChainHash(), 0, 0))

		_, _, err = server.lookupConflictingTxs(t.Context(), *subtree.RootHash(), []utxostore.UnresolvedMetaData{{Hash: *f.loser.TxIDChainHash()}})
		require.True(t, errors.Is(err, errors.ErrStorageError), "%v", err)

		_, _, err = server.subtreeConflictingTxs(t.Context(), subtree)
		require.True(t, errors.Is(err, errors.ErrStorageError), "%v", err)
	})

	t.Run("a kept-aside file pruned between check and read is none", func(t *testing.T) {
		f := newConflictingFixture(t)
		faulty := &faultySubtreeStore{
			Store:      blobmemory.New(),
			getErr:     map[fileformat.FileType]error{fileformat.FileTypeSubtreeConflicting: errors.ErrNotFound},
			existsTrue: map[fileformat.FileType]bool{fileformat.FileTypeSubtreeConflicting: true},
		}
		server, _ := f.newServerWithStore(t, faulty)

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*f.loser.TxIDChainHash(), 0, 0))

		missing := []utxostore.UnresolvedMetaData{{Hash: *f.loser.TxIDChainHash()}}

		found, stillMissing, err := server.lookupConflictingTxs(t.Context(), *subtree.RootHash(), missing)
		require.NoError(t, err)
		require.Empty(t, found)
		require.Equal(t, missing, stillMissing)

		_, ok, err := server.subtreeConflictingTxs(t.Context(), subtree)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("a failed conflicting-parent read halts the subtree", func(t *testing.T) {
		f := newConflictingFixture(t)
		f.server.policyRejectedTxCache.SetConflicting(*f.loser.TxIDChainHash(), f.loser)
		f.server.utxoStore = &failingGetStore{Store: f.store, hash: *f.loser.TxIDChainHash(), err: storageErr}

		txMetaSlice := make([]metaSliceItem, 1)
		err := f.server.blessMissingTransactions(t.Context(), chainhash.Hash{}, []missingTx{{tx: f.child, idx: 0}}, "", "", txMetaSlice, 101, map[uint32]bool{}, true)
		require.True(t, errors.Is(err, errors.ErrStorageError), "%v", err)
		require.False(t, txMetaSlice[0].keptAside)
	})
}

// failingGetStore fails the Get of one tx.
type failingGetStore struct {
	utxostore.Store
	hash chainhash.Hash
	err  error
}

func (s *failingGetStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if *hash == s.hash {
		return nil, s.err
	}

	return s.Store.Get(ctx, hash, f...)
}
