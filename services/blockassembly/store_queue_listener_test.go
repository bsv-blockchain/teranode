package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 4b: with blockassembly_storedPrefixTemplate on, the listener's workers take
// store requests from the store queue, which enforces its byte limit, takes the bytes of a write that
// is retried or abandoned into account, and gives sequence numbers at pickup.

type queuedRequest struct {
	subtree *subtreepkg.Subtree
	result  chan subtreeprocessor.StoreResult
}

// startQueueListener is a server whose Init, with blockassembly_storedPrefixTemplate on, made the
// store queue and started its listener. The queue limit is 0 MB: one request is admitted at a time
// (a request larger than the limit is admitted when nothing is held), so Held is at most one
// request's bytes.
func startQueueListener(t *testing.T) (*BlockAssembly, *subtreeprocessor.StoreQueue, *storePipeline) {
	t.Helper()

	server, _ := setupServerWithSettings(t, func(s *settings.Settings) {
		s.BlockAssembly.StoredPrefixTemplate = true
		s.BlockAssembly.StoreQueueMaxMB = 0
	})
	require.NotNil(t, server.storeQueue, "Init makes the queue when the flag is on")

	p := &storePipeline{announced: make(chan chainhash.Hash, 64)}

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

	return server, server.storeQueue, p
}

// pushChain sends a chain request for a new subtree, and returns it with its result.
func pushChain(t *testing.T, q *subtreeprocessor.StoreQueue, seed int, skip bool) queuedRequest {
	t.Helper()

	subtree, txMap := makeSubtree(t, seed)
	result := make(chan subtreeprocessor.StoreResult, 1)

	q.Push(subtreeprocessor.NewSubtreeRequest{
		Subtree:          subtree,
		ParentTxMap:      txMap,
		SkipNotification: skip,
		ErrChan:          make(chan error, 1),
		Result:           result,
	})

	return queuedRequest{subtree: subtree, result: result}
}

func TestStoreQueueListener_HeldBytesStayUnderTheLimitAcrossGenerationsDuringAnOutage(t *testing.T) {
	server, q, _ := startQueueListener(t)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
	fault.OnSetAny(fileformat.FileTypeSubtreeMeta, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 100000)

	subtree, txMap := makeSubtree(t, 1)
	limit := subtreeprocessor.StoreRequestBytes(subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap})

	var previous, all []queuedRequest

	seed := 10

	for generation := 0; generation < 5; generation++ {
		var current []queuedRequest

		for range 8 {
			seed++

			r := pushChain(t, q, seed, true)
			current = append(current, r)
			require.LessOrEqual(t, q.Held(), limit)
		}

		// a rechain: the previous generation has left the chain
		for _, r := range previous {
			q.Stale(r.subtree)
		}

		time.Sleep(50 * time.Millisecond)
		require.LessOrEqual(t, q.Held(), limit, "queued, retried and in-flight bytes stay under the limit")
		require.Positive(t, q.Held(), "the outage holds bytes in retries")

		all = append(all, current...)
		previous = current
	}

	// the outage ends: what is still wanted is stored, what left the chain was dropped
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{})
	fault.OnSetAny(fileformat.FileTypeSubtreeMeta, blobfault.Rule{})

	require.Eventually(t, func() bool { return q.Held() == 0 && q.Len() == 0 }, 20*time.Second, 20*time.Millisecond,
		"every settled or dropped request gave its bytes back")

	last := all[len(all)-8:]
	for _, r := range last {
		require.True(t, awaitResult(t, r.result).OK(), "the newest generation is stored")
	}

	dropped := 0

	for _, r := range all[:len(all)-8] {
		select {
		case res := <-r.result:
			if res.Cancelled {
				dropped++
			}
		default:
		}
	}

	require.Positive(t, dropped, "requests of generations that left the chain were dropped at the limit")
}

func TestStoreQueueListener_ABlockedWriteKeepsItsBytesUntilItReturns(t *testing.T) {
	server, q, _ := startQueueListener(t)

	fault := blobfault.New(server.subtreeStore)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: release, Entered: entered})
	server.subtreeStore = fault

	subtree, txMap := makeSubtree(t, 1)
	one := subtreeprocessor.StoreRequestBytes(subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	first := pushChain(t, q, 2, true)
	<-entered

	second := pushChain(t, q, 3, true)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, one, q.Held(), "the blocked write is the only request admitted")

	select {
	case <-second.result:
		t.Fatal("the second request ran while the first held the whole limit")
	default:
	}

	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{})
	close(release)

	require.True(t, awaitResult(t, first.result).OK())
	require.True(t, awaitResult(t, second.result).OK())
	require.Eventually(t, func() bool { return q.Held() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// The in-order sender waits for each sequence number. A request the queue drops before pickup has
// none, so the requests after it are still announced.
func TestStoreQueueListener_ADroppedRequestLeavesNoGapInTheNotifications(t *testing.T) {
	server, q, p := startQueueListener(t)

	fault := blobfault.New(server.subtreeStore)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: release, Entered: entered})
	server.subtreeStore = fault
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blocker := pushChain(t, q, 2, false)
	<-entered

	dropped1, dropped2 := pushChain(t, q, 3, false), pushChain(t, q, 4, false)
	q.Stale(dropped1.subtree, dropped2.subtree)

	kept := pushChain(t, q, 5, false) // at the limit with stale requests queued: they are dropped

	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{})
	close(release)

	require.True(t, awaitResult(t, blocker.result).OK())
	require.True(t, awaitResult(t, kept.result).OK())
	require.True(t, awaitResult(t, dropped1.result).Cancelled)
	require.True(t, awaitResult(t, dropped2.result).Cancelled)

	announced := map[chainhash.Hash]bool{}

	for range 2 {
		select {
		case h := <-p.announced:
			announced[h] = true
		case <-time.After(5 * time.Second):
			t.Fatal("a subtree after a dropped request was never announced: the sender waits for a number nobody has")
		}
	}

	require.True(t, announced[*blocker.subtree.RootHash()])
	require.True(t, announced[*kept.subtree.RootHash()])
}

// With room in the queue nothing is dropped at the limit, so a stale request is still queued when
// a worker comes to it: the worker cancels it, never writes it, and the in-order sender announces the
// request after it.
func TestStoreQueueListener_AStaleRequestIsCancelledAtTakeAndTheNextOneIsAnnounced(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) {
		s.BlockAssembly.StoredPrefixTemplate = true
		s.BlockAssembly.StoreQueueMaxMB = 1024
		s.BlockAssembly.SubtreeStorageWorkers = 1
	})
	q := server.storeQueue
	require.NotNil(t, q)

	p := &storePipeline{announced: make(chan chainhash.Hash, 64)}
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

	fault := blobfault.New(server.subtreeStore)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: release, Entered: entered})
	server.subtreeStore = fault
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	first := pushChain(t, q, 2, false)
	<-entered // the only worker is writing it

	stale, last := pushChain(t, q, 3, false), pushChain(t, q, 4, false)
	q.Stale(stale.subtree)

	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{})
	close(release)

	require.True(t, awaitResult(t, first.result).OK())
	require.True(t, awaitResult(t, last.result).OK())
	require.True(t, awaitResult(t, stale.result).Cancelled)

	announced := map[chainhash.Hash]bool{}

	for range 2 {
		select {
		case h := <-p.announced:
			announced[h] = true
		case <-time.After(5 * time.Second):
			t.Fatal("a subtree after a cancelled request was never announced")
		}
	}

	require.True(t, announced[*first.subtree.RootHash()])
	require.True(t, announced[*last.subtree.RootHash()])
	require.False(t, announced[*stale.subtree.RootHash()], "a cancelled request is not announced")
	require.Zero(t, fault.SetCalls(stale.subtree.RootHash()[:], fileformat.FileTypeSubtree), "and its subtree is never written")
	require.Eventually(t, func() bool { return q.Held() == 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestStoreQueueListener_AnAnnounceOnlyRequestAnnouncesWithoutStoring(t *testing.T) {
	server, q, p := startQueueListener(t)

	fault := blobfault.New(server.subtreeStore)
	server.subtreeStore = fault

	stored := pushChain(t, q, 2, true)
	require.True(t, awaitResult(t, stored.result).OK())
	p.requireNoAnnouncement(t)

	q.Push(subtreeprocessor.NewSubtreeRequest{Subtree: stored.subtree, AnnounceOnly: true})
	p.requireAnnounced(t, stored.subtree.RootHash())

	require.Equal(t, 1, fault.SetCalls(stored.subtree.RootHash()[:], fileformat.FileTypeSubtree), "announced, not stored again")
}

// With the flag off no request carries a settled hook: nothing holds the request for a retry's
// whole backoff on account of a queue that is not there.
func TestStoreQueueListener_WithoutAQueueTheOutcomeHoldsNoSettledHook(t *testing.T) {
	require.Nil(t, newSubtreeStoreOutcome(subtreeprocessor.NewSubtreeRequest{}, true, nil).settled)
}

// A fork reorg's forward steps send silently. A subtree that is not stored when the reorg asks for its
// announcement is announced once, when it settles stored, whichever path stored it.
func TestStoreQueueListener_ASilentRequestIsAnnouncedOnceWhenItSettlesAfterTheReorgAsked(t *testing.T) {
	for name, run := range map[string]func(t *testing.T, server *BlockAssembly, fault *blobfault.Store, subtree *subtreepkg.Subtree, ask func()){
		// the subtree is written, the meta is not: the meta retry is silent
		"meta retry": func(t *testing.T, server *BlockAssembly, fault *blobfault.Store, subtree *subtreepkg.Subtree, ask func()) {
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{FailTimes: blobfault.Always})
			require.Eventually(t, func() bool { return exists(t, server.subtreeStore, subtree, fileformat.FileTypeSubtree) }, 5*time.Second, 5*time.Millisecond)
			ask()
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{})
		},
		// the write failed, then the subtree turned up in the store: the retry finds it stored and is silent
		"retry finds it stored": func(t *testing.T, server *BlockAssembly, fault *blobfault.Store, subtree *subtreepkg.Subtree, ask func()) {
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
			require.Eventually(t, func() bool { return fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtree) > 0 }, 5*time.Second, 5*time.Millisecond)

			bytes, err := subtree.Serialize()
			require.NoError(t, err)
			require.NoError(t, fault.Store.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, bytes))

			ask()
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{})
		},
		"retry": func(t *testing.T, server *BlockAssembly, fault *blobfault.Store, subtree *subtreepkg.Subtree, ask func()) {
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
			require.Eventually(t, func() bool { return fault.SetCalls(subtree.RootHash()[:], fileformat.FileTypeSubtree) > 0 }, 5*time.Second, 5*time.Millisecond)
			ask()
			fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			server, q, p := startQueueListener(t)

			fault := blobfault.New(server.subtreeStore)
			server.subtreeStore = fault
			setForTest(t, &subtreeRetryLimit, 100000)

			subtree, txMap := makeSubtree(t, 2)
			subtree.RootHash() // cached before the workers read it
			result := make(chan subtreeprocessor.StoreResult, 1)

			q.Push(subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, SkipNotification: true, ErrChan: make(chan error, 1), Result: result})

			run(t, server, fault, subtree, func() { require.True(t, q.AnnounceOnSuccess(subtree), "the request is outstanding") })

			require.True(t, awaitResult(t, result).OK())
			p.requireAnnounced(t, subtree.RootHash())
		})
	}
}
