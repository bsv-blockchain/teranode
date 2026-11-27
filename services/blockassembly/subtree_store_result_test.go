package blockassembly

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The typed store result (issue 2269, PR 2a): ErrChan says the first attempt at both writes ended and
// is answered with nil whatever happened; StoreResult says how the store ended.

// storePipeline runs the listener, its workers and the retry processor of a server, the way Init
// wires them, with a blockchain client that records the announcements.
type storePipeline struct {
	in        chan subtreeprocessor.NewSubtreeRequest
	announced chan chainhash.Hash
}

func startStorePipeline(t *testing.T, server *BlockAssembly) *storePipeline {
	t.Helper()

	p := &storePipeline{
		in:        make(chan subtreeprocessor.NewSubtreeRequest, 64),
		announced: make(chan chainhash.Hash, 64),
	}

	client := &blockchain.Mock{}
	client.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	client.On("SendNotification", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			n, _ := args.Get(1).(*blockchain.Notification)
			p.announced <- chainhash.Hash(n.Hash)
		}).
		Return(nil)
	server.blockchainClient = client
	setForTest(t, &subtreeRetryBackoffUnit, time.Millisecond)
	setForTest(t, &metaBuildBackoff, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	retryChan := make(chan *subtreeRetrySend, 100)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		server.runSubtreeRetryProcessor(ctx, retryChan)
	}()
	go func() {
		defer wg.Done()
		server.runNewSubtreeListener(ctx, p.in, retryChan)
	}()

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return p
}

// store sends a request and returns its result channel.
func (p *storePipeline) store(subtree *subtreepkg.Subtree, txMap subtreeprocessor.TxInpointsMap, skip bool) <-chan subtreeprocessor.StoreResult {
	result := make(chan subtreeprocessor.StoreResult, 1)

	p.in <- subtreeprocessor.NewSubtreeRequest{
		Subtree:          subtree,
		ParentTxMap:      txMap,
		SkipNotification: skip,
		ErrChan:          make(chan error, 1),
		Result:           result,
	}

	return result
}

func awaitResult(t *testing.T, result <-chan subtreeprocessor.StoreResult) subtreeprocessor.StoreResult {
	t.Helper()

	return func() subtreeprocessor.StoreResult {
		select {
		case r := <-result:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("the store never settled")

			return subtreeprocessor.StoreResult{}
		}
	}()
}

// requireAnnounced requires exactly one announcement of hash.
func (p *storePipeline) requireAnnounced(t *testing.T, hash *chainhash.Hash) {
	t.Helper()

	select {
	case got := <-p.announced:
		require.Equal(t, *hash, got)
	case <-time.After(5 * time.Second):
		t.Fatal("the subtree was never announced")
	}

	p.requireNoAnnouncement(t)
}

func (p *storePipeline) requireNoAnnouncement(t *testing.T) {
	t.Helper()

	select {
	case got := <-p.announced:
		t.Fatalf("unexpected announcement of %s", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// makeSubtree is a 16-leaf subtree whose transactions depend on seed, with the inpoints of each in
// the returned map, so several distinct subtrees can be stored.
func makeSubtree(t *testing.T, seed int) (*subtreepkg.Subtree, subtreeprocessor.TxInpointsMap) {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(16)
	require.NoError(t, err)

	txMap := subtreeprocessor.NewSplitTxInpointsMap(256)
	previousHash := chainhash.HashH([]byte(fmt.Sprintf("previousHash%d", seed)))

	for i := uint64(0); i < 16; i++ {
		txHash := chainhash.HashH([]byte(fmt.Sprintf("seed%d-tx%d", seed, i)))
		require.NoError(t, subtree.AddNode(txHash, i, i))

		in := &bt.Input{PreviousTxOutIndex: 0}
		require.NoError(t, in.PreviousTxIDAdd(&previousHash))

		ti, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{in})
		require.NoError(t, err)

		txMap.Set(txHash, &ti)
		previousHash = txHash
	}

	return subtree, txMap
}

func exists(t *testing.T, store interface {
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
}, subtree *subtreepkg.Subtree, fileType fileformat.FileType) bool {
	t.Helper()

	ok, err := store.Exists(t.Context(), subtree.RootHash()[:], fileType)
	require.NoError(t, err)

	return ok
}

func TestSubtreeStoreResult_SetFailsOnceThenSucceeds_StoredAndAnnouncedOnceThroughTheSender(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 1)

	fault := blobfault.New(store)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: 1})
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.NoError(t, result.Err)
	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.SubtreeStored)
	require.True(t, result.MetaStored)
	require.False(t, result.ExistedAlready)
	require.Equal(t, 2, fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtree), "one failed Set and the retry")

	p.requireAnnounced(t, subtree.RootHash())
}

func TestSubtreeStoreResult_SkipNotificationSuppressesTheLateAnnouncement(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 2)

	fault := blobfault.New(store)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: 1})
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	p.requireNoAnnouncement(t)
}

func TestSubtreeStoreResult_BothWritesFailOnce_OneResultAndOneAnnouncement(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 3)

	fault := blobfault.New(store)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: 1})
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{FailTimes: 1})
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	results := p.store(subtree, txMap, false)
	result := awaitResult(t, results)

	require.True(t, result.OK(), "%+v", result)
	require.True(t, exists(t, store, subtree, fileformat.FileTypeSubtree))
	require.True(t, exists(t, store, subtree, fileformat.FileTypeSubtreeMeta))
	require.Empty(t, results, "the result is posted once")

	p.requireAnnounced(t, subtree.RootHash())
}

func TestSubtreeStoreResult_SetAlwaysFails_FailedAfterExhaustionAndCounted(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 4)

	fault := blobfault.New(store)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 2)

	initPrometheusMetrics()

	before := testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.Error(t, result.Err)
	require.False(t, result.OK())
	require.False(t, result.SubtreeStored)
	require.False(t, result.Cancelled)
	require.Equal(t, subtreeRetryLimit+3, fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtree), "the first try and every retry")
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed))
	p.requireNoAnnouncement(t)
}

func TestSubtreeStoreResult_SubtreeExistsMetaDoesNot_ExistedAlreadyMetaNotStoredAndNotRebuilt(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 5)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	fault := blobfault.New(store)
	server.subtreeStore = fault

	p := startStorePipeline(t, server)

	errChan := make(chan error, 1)
	resultChan := make(chan subtreeprocessor.StoreResult, 1)
	p.in <- subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, ErrChan: errChan, Result: resultChan}

	require.NoError(t, <-errChan, "ErrChan is answered with nil, as it is today")

	result := awaitResult(t, resultChan)

	require.NoError(t, result.Err)
	require.True(t, result.ExistedAlready)
	require.True(t, result.SubtreeStored)
	require.False(t, result.MetaStored)
	require.False(t, result.MetaNotApplicable)
	require.False(t, result.OK(), "the meta is not there")
	require.Zero(t, fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta), "the meta is not rebuilt")
	p.requireAnnounced(t, subtree.RootHash())
}

func TestSubtreeStoreResult_ExistingSubtreeWithItsMeta_MetaStoredWithoutAWrite(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 7)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
	require.NoError(t, store.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, []byte("meta")))

	fault := blobfault.New(store)
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.ExistedAlready)
	require.True(t, result.MetaStored)
	require.Zero(t, fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta), "an existing meta file is not written again")
}

func TestSubtreeStoreResult_MetaSettingOff_MetaNotApplicable(t *testing.T) {
	server, store, _, _ := setup(t)
	server.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false

	subtree, txMap := makeSubtree(t, 8)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.SubtreeStored)
	require.True(t, result.MetaNotApplicable)
	require.False(t, result.MetaStored)
	require.False(t, exists(t, store, subtree, fileformat.FileTypeSubtreeMeta))
}

// flakyReads is a ParentTxMap whose first reads fail with an error, as a disk tx map that cannot be
// read for a moment. It hides the serialized-inpoints fast path, so the build goes through GetWithErr.
type flakyReads struct {
	subtreeprocessor.TxInpointsMap

	mu    sync.Mutex
	fails int
}

func (m *flakyReads) GetWithErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.fails > 0 {
		m.fails--

		return nil, false, errors.NewStorageError("disk tx map read failed")
	}

	inpoints, found := m.TxInpointsMap.Get(hash)

	return inpoints, found, nil
}

// noParents is a ParentTxMap that finds no parent, and counts the reads.
type noParents struct {
	subtreeprocessor.TxInpointsMap

	reads atomic.Int32
}

func (m *noParents) Get(chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	m.reads.Add(1)

	return nil, false
}

func TestSubtreeStoreResult_MetaReadFailsThenSucceeds_MetaStored(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 9)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, &flakyReads{TxInpointsMap: txMap, fails: 1}, false))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.MetaStored)
	require.True(t, exists(t, store, subtree, fileformat.FileTypeSubtreeMeta))
	p.requireAnnounced(t, subtree.RootHash())
}

func TestSubtreeStoreResult_MetaReadAlwaysFails_FailedAfterTheRetries(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 10)

	initPrometheusMetrics()

	before := testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, &flakyReads{TxInpointsMap: txMap, fails: 1 << 30}, false))

	require.Error(t, result.Err, "a missing meta is a failure when the meta setting is on")
	require.True(t, result.SubtreeStored, "the subtree itself was written")
	require.False(t, result.OK())
	require.False(t, exists(t, store, subtree, fileformat.FileTypeSubtreeMeta))
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed))
}

// alreadyPublished answers every subtree Set as the file store does when another writer published
// the name between the existence check and the write.
type alreadyPublished struct {
	*blobfault.Store
}

func (s alreadyPublished) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeSubtree {
		return errors.NewBlobAlreadyExistsError("published by another writer")
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// setForTest sets a package-level knob for the test.
func setForTest[T any](t *testing.T, knob *T, value T) {
	t.Helper()

	previous := *knob
	*knob = value

	t.Cleanup(func() { *knob = previous })
}

func TestSubtreeStoreResult_AnotherWriterPublishedTheSubtreeFirst_ExistedAlreadyNoError(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 13)

	server.subtreeStore = alreadyPublished{blobfault.New(store)}

	p := startStorePipeline(t, server)

	errChan := make(chan error, 1)
	resultChan := make(chan subtreeprocessor.StoreResult, 1)
	p.in <- subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, ErrChan: errChan, Result: resultChan}

	require.NoError(t, <-errChan)

	result := awaitResult(t, resultChan)

	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.ExistedAlready)
	p.requireAnnounced(t, subtree.RootHash())
}

// setDAHFails answers every SetDAH with an error, as a store that cannot update a delete-at-height.
type setDAHFails struct {
	*blobfault.Store
}

func (setDAHFails) SetDAH(context.Context, []byte, fileformat.FileType, uint32, ...options.FileOption) error {
	return errors.NewStorageError("set DAH failed")
}

func TestSubtreeStoreResult_ExistingSubtreeWhoseDAHCannotBeUpdated_FailsInsteadOfHanging(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 14)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	server.subtreeStore = setDAHFails{blobfault.New(store)}

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.False(t, result.Cancelled)
	require.Error(t, result.Err)
	require.False(t, result.OK())
}

func TestSubtreeStoreResult_ParentNotFound_FailsAtOnceWithoutARetry(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 15)
	parents := &noParents{TxInpointsMap: txMap}

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, parents, false))

	require.Error(t, result.Err)
	require.True(t, result.SubtreeStored)
	require.Equal(t, int32(1), parents.reads.Load(), "a parent that is not found is deterministic: no second build")
	require.False(t, exists(t, store, subtree, fileformat.FileTypeSubtreeMeta))
}

// landsThenFails writes the subtree and reports the first write as failed, as a write that
// completed after its caller was told it failed.
type landsThenFails struct {
	*blobfault.Store

	once sync.Once
}

func (s *landsThenFails) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType != fileformat.FileTypeSubtree {
		return s.Store.Set(ctx, key, fileType, value, opts...)
	}

	first := false

	s.once.Do(func() { first = true })

	err := s.Store.Set(ctx, key, fileType, value, opts...)
	if first && err == nil {
		return errors.NewStorageError("reported failed after the write landed")
	}

	return err
}

func TestSubtreeStoreResult_RetryFindsTheSubtreeAlreadyStored_ReportsExistedAndStaysSilent(t *testing.T) {
	server, store, _, _ := setup(t)
	subtree, txMap := makeSubtree(t, 16)

	server.subtreeStore = &landsThenFails{Store: blobfault.New(store)}

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, false))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, result.ExistedAlready)
	p.requireNoAnnouncement(t)
}

// Issue 2269, plan PR 3: the storage side adds a subtree's root to the stored set when its store
// ends with everything it has to write stored, and notes a store that failed for good.

func storedSetServer(t *testing.T) (*BlockAssembly, *subtreeprocessor.StoredSet) {
	t.Helper()

	server, _, _, _ := setup(t)
	server.storedSet = subtreeprocessor.NewStoredSet()

	return server, server.storedSet
}

func TestStoredSet_AStoredSubtreeAndItsMetaAddTheRoot(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 101)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()), "the root is in the set when the result is posted")
	require.False(t, set.Retrying(*subtree.RootHash()))
}

func TestStoredSet_ASubtreeStoredByARetryAddsTheRootAndTheRetryIsNoLongerInFlight(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 102)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: 1})
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()))
	require.False(t, set.Retrying(*subtree.RootHash()), "the retry settled")
}

func TestStoredSet_ARetryThatIsExhaustedLeavesTheRootOutAndNotesTheFailure(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 103)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 1)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.Error(t, result.Err)
	require.False(t, set.Has(*subtree.RootHash()))
	require.True(t, set.HasFailed(*subtree.RootHash()), "the processor sends it again from memory")
	require.False(t, set.Retrying(*subtree.RootHash()))
}

// A subtree whose file landed but whose meta did not: the second request for it finds the file and
// must build the meta, or the root never reaches the set and the prefix stops there for good.
func TestStoredSet_AnExistingSubtreeWithoutItsMetaGetsTheMetaBuiltAndIsStored(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 104)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	p := startStorePipeline(t, server)

	// no Result channel: the processor's re-enqueue does not ask for one
	errChan := make(chan error, 1)
	p.in <- subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, SkipNotification: true, ErrChan: errChan}
	require.NoError(t, <-errChan)

	require.True(t, set.Has(*subtree.RootHash()), "the meta was built")
	require.True(t, exists(t, server.subtreeStore, subtree, fileformat.FileTypeSubtreeMeta))
}

func TestStoredSet_AMetaThatFailedForGoodIsStoredByTheSecondRequest(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 105)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 1)

	p := startStorePipeline(t, server)

	first := awaitResult(t, p.store(subtree, txMap, true))
	require.Error(t, first.Err)
	require.False(t, set.Has(*subtree.RootHash()), "the meta is missing")
	require.True(t, set.HasFailed(*subtree.RootHash()))

	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{})

	second := awaitResult(t, p.store(subtree, txMap, true))
	require.True(t, second.OK(), "%+v", second)
	require.True(t, set.Has(*subtree.RootHash()))
	require.False(t, set.HasFailed(*subtree.RootHash()), "stored since")
}

func TestStoredSet_WithoutAMetaOnlyTheSubtreeIsNeeded(t *testing.T) {
	server, set := storedSetServer(t)
	server.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	subtree, txMap := makeSubtree(t, 106)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()))
}

// The processor's stall rule leaves a root alone while its write is in the retry processor, and
// only the store side can tell it so: the count must be up while the retried write is held.
func TestStoredSet_ARootIsRetryingWhileItsRetriedWriteIsHeld(t *testing.T) {
	for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeMeta} {
		t.Run(fileType.String(), func(t *testing.T) {
			server, set := storedSetServer(t)
			subtree, txMap := makeSubtree(t, 107)
			root := *subtree.RootHash()

			// the first write is held, then fails; the retry meets a second rule that holds it too
			fail, failEntered := make(chan struct{}), make(chan struct{}, 1)
			fault := blobfault.New(server.subtreeStore)
			fault.OnSet(root[:], fileType, blobfault.Rule{FailTimes: 1, Block: fail, Entered: failEntered})
			server.subtreeStore = fault

			p := startStorePipeline(t, server)
			result := p.store(subtree, txMap, true)

			select {
			case <-failEntered:
			case <-time.After(10 * time.Second):
				t.Fatal("the first write never reached the store")
			}

			release, entered := make(chan struct{}), make(chan struct{}, 1)
			fault.OnSet(root[:], fileType, blobfault.Rule{Block: release, Entered: entered})
			close(fail)

			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock) // a failed assertion must not leave the write held

			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the retry never reached the store")
			}

			require.True(t, set.Retrying(root), "the write is in the retry processor")

			unblock()

			require.True(t, awaitResult(t, result).OK())
			require.False(t, set.Retrying(root), "the retry settled")
		})
	}
}

// Every way a request settles must leave the retry count where it was, or the progress rule,
// which waits for no retry in flight, would stay off for good.
func TestStoredSet_EveryWayARequestSettlesBalancesTheRetryCount(t *testing.T) {
	initPrometheusMetrics() // fail counts the request; the counter is nil until initialised

	root := chainhash.HashH([]byte("outcome"))
	boom := errors.NewStorageError("boom")

	for name, settle := range map[string]func(o *subtreeStoreOutcome){
		"stored":         func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.metaStored() },
		"existed":        func(o *subtreeStoreOutcome) { o.subtreeStored(true); o.metaAbsent() },
		"retry stored":   func(o *subtreeStoreOutcome) { o.toRetry(); o.subtreeStored(false); o.metaStored() },
		"retry failed":   func(o *subtreeStoreOutcome) { o.toRetry(); o.fail(boom); o.metaStored() },
		"cancelled":      func(o *subtreeStoreOutcome) { o.toRetry(); o.cancel(boom); o.cancel(boom) },
		"aborted":        func(o *subtreeStoreOutcome) { o.abort(boom) },
		"both retrying":  func(o *subtreeStoreOutcome) { o.toRetry(); o.toRetry(); o.subtreeStored(false); o.metaStored() },
		"settled twice":  func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.metaStored(); o.fail(boom) },
		"meta not built": func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.fail(boom) },
		"retry too late": func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.metaStored(); o.toRetry() },
	} {
		t.Run(name, func(t *testing.T) {
			set := subtreeprocessor.NewStoredSet()
			o := newSubtreeStoreOutcome(subtreeprocessor.NewSubtreeRequest{}, true, nil)
			o.track(set, root)

			settle(o)

			require.False(t, set.Retrying(root))
		})
	}
}
