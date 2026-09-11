package blockassembly

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// This fixture runs the real authority and native subscription client. Reserving
// the listener before Start avoids selecting and releasing a supposedly free port.
func newUnminedRecoveryTestAssembler(t *testing.T, state blockchain.FSMStateType, configure ...func(*settings.Settings)) (*BlockAssembler, *blockchain.Blockchain) {
	t.Helper()
	initPrometheusMetrics()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings(t)
	tSettings.Context = t.Name()
	tSettings.BlockAssembly.OnRestartValidateParentChain = false
	tSettings.BlockAssembly.UnminedTxDiskSortEnabled = false
	tSettings.BlockAssembly.DoubleSpendWindow = 0
	tSettings.BlockChain.HTTPListenAddress = "127.0.0.1:0"
	tSettings.BlockChain.PeerRegistryStore = nil
	tSettings.Kafka.BlocksFinalConfig = &url.URL{Scheme: "memory", Host: "recovery-test"}
	for _, apply := range configure {
		apply(tSettings)
	}
	listener, _, _, err := util.GetListener(tSettings.Context, "blockchain", "", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { util.RemoveListener(tSettings.Context, "blockchain", "") })
	tSettings.BlockChain.GRPCListenAddress = listener.Addr().String()
	tSettings.BlockChain.GRPCAddress = listener.Addr().String()
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := blockchainstore.NewStore(logger, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, tSettings.Kafka.BlocksFinalConfig, &tSettings.Kafka)
	require.NoError(t, err)
	server, err := blockchain.New(ctx, logger, tSettings, store, producer, state.String())
	require.NoError(t, err)
	require.NoError(t, server.Init(ctx))
	ready := make(chan struct{})
	finished := make(chan error, 1)
	go func() { finished <- server.Start(ctx, ready) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, server.Stop(context.Background()))
		select {
		case err := <-finished:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("blockchain server did not stop")
		}
	})
	select {
	case <-ready:
	case err := <-finished:
		t.Fatalf("blockchain server failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("blockchain server did not become ready")
	}
	client, err := blockchain.NewClient(ctx, logger, tSettings, "unmined-recovery-test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.(*blockchain.Client).Close()) })
	require.Eventually(t, func() bool {
		actual, readErr := client.ReadFSMState(ctx)
		return readErr == nil && actual == state
	}, time.Second, time.Millisecond)
	utxoStore, err := utxosql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, utxoStore.Close(context.Background())) })
	require.NoError(t, utxoStore.SetBlockHeight(1))
	subtreeStore := memory.New()
	newSubtrees := make(chan subtreeprocessor.NewSubtreeRequest, 100)
	assembler, err := NewBlockAssembler(ctx, logger, tSettings, gocore.NewStat("unmined-recovery-test"), utxoStore, subtreeStore, client, newSubtrees)
	require.NoError(t, err)
	header, meta, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assembler.setBestBlockHeader(header, meta.Height)
	assembler.subtreeProcessor.InitCurrentBlockHeader(header)
	assembler.setCurrentRunningState(StateRunning)
	assembler.subtreeProcessor.Start(ctx)
	t.Cleanup(func() { assembler.subtreeProcessor.Stop(context.Background()) })
	// Mining an incomplete subtree requires actual blob storage acknowledgement.
	// Run the production storage listener instead of faking that acknowledgement.
	storage := &BlockAssembly{
		logger: logger, settings: tSettings, stats: gocore.NewStat("unmined-recovery-storage"),
		blockAssembler: assembler, utxoStore: utxoStore, subtreeStore: subtreeStore, blockchainClient: client,
	}
	storageCtx, stopStorage := context.WithCancel(ctx)
	storageDone := make(chan struct{})
	go func() {
		defer close(storageDone)
		storage.runNewSubtreeListener(storageCtx, newSubtrees, make(chan *subtreeRetrySend, 100))
	}()
	t.Cleanup(func() { stopStorage(); <-storageDone })
	return assembler, server
}

func storeSuppressedRecoveryTransaction(t *testing.T, assembler *BlockAssembler) chainhash.Hash {
	t.Helper()
	return storeRecoverySelectionChain(t, assembler, 1)[0]
}

func recoveryCandidateHashes(t *testing.T, assembler *BlockAssembler) []chainhash.Hash {
	t.Helper()
	_, trees, lease, err := assembler.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	defer lease.Release()
	var hashes []chainhash.Hash
	for _, tree := range trees {
		for _, node := range tree.Nodes {
			hashes = append(hashes, node.Hash)
		}
	}
	return hashes
}

// A timer pass can be rebuilding between polls. Observe usable mining work
// before stopping the fixture; a transaction count alone can be partial state.
func recoveryCandidateContains(ctx context.Context, assembler *BlockAssembler, txID chainhash.Hash) bool {
	_, trees, lease, err := assembler.GetMiningCandidate(ctx)
	defer lease.Release()
	if err != nil {
		return false
	}
	for _, tree := range trees {
		for _, node := range tree.Nodes {
			if node.Hash == txID {
				return true
			}
		}
	}
	return false
}

func TestRecoverUnminedTransactionsWithoutNewBlock(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	beforeHeader, beforeHeight := assembler.CurrentBlock()
	require.NotContains(t, recoveryCandidateHashes(t, assembler), txID, "stored transactions have no automatic assembly admission before recovery")
	for range 2 {
		recovered, err := assembler.recoverUnminedTransactions(t.Context())
		require.NoError(t, err)
		require.True(t, recovered)
		hashes := recoveryCandidateHashes(t, assembler)
		count := 0
		for _, hash := range hashes {
			if hash == txID {
				count++
			}
		}
		require.Equal(t, 1, count, "repeated recovery must produce exactly one mining-template entry")
		afterHeader, afterHeight := assembler.CurrentBlock()
		require.Equal(t, beforeHeader.Hash(), afterHeader.Hash())
		require.Equal(t, beforeHeight, afterHeight)
	}
}

func TestRecoverUnminedTransactionsWaitsForRunningAuthority(t *testing.T) {
	for _, state := range []blockchain.FSMStateType{blockchain.FSMStateIDLE, blockchain.FSMStateCATCHINGBLOCKS} {
		t.Run(state.String(), func(t *testing.T) {
			assembler, _ := newUnminedRecoveryTestAssembler(t, state)
			txID := storeSuppressedRecoveryTransaction(t, assembler)
			recovered, err := assembler.recoverUnminedTransactions(t.Context())
			require.NoError(t, err)
			require.False(t, recovered)
			require.NotContains(t, recoveryCandidateHashes(t, assembler), txID)
		})
	}
}

func TestRecoverUnminedTransactionsRetriesUnavailableAuthority(t *testing.T) {
	assembler, server := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	server.SetSubscriptionManagerReadyForTesting(false)
	recovered, err := assembler.recoverUnminedTransactions(t.Context())
	require.Error(t, err)
	require.False(t, recovered)
	require.NotContains(t, recoveryCandidateHashes(t, assembler), txID)
	server.SetSubscriptionManagerReadyForTesting(true)
	recovered, err = assembler.recoverUnminedTransactions(t.Context())
	require.NoError(t, err)
	require.True(t, recovered)
	require.Contains(t, recoveryCandidateHashes(t, assembler), txID)
}

func TestRecoverUnminedTransactionsDefersTipMismatch(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	assembler.setBestBlockHeader(blockHeader1, 1)
	assembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)
	recovered, err := assembler.recoverUnminedTransactions(t.Context())
	require.Error(t, err)
	require.False(t, recovered)
	header, height := assembler.CurrentBlock()
	require.Equal(t, blockHeader1.Hash(), header.Hash(), "recovery must leave tip changes to normal chain reconciliation")
	require.Equal(t, uint32(1), height)
	_, found := assembler.subtreeProcessor.GetCurrentTxMap().Get(txID)
	require.False(t, found)
}

func TestRecoverUnminedTransactionsCancelled(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	recovered, err := assembler.recoverUnminedTransactions(ctx)
	require.Error(t, err)
	require.False(t, recovered)
	require.NotContains(t, recoveryCandidateHashes(t, assembler), txID)
}

func TestUnminedRecoveryRepairsFailureBeforeReconcilingNewTip(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	header, height := assembler.CurrentBlock()
	processor := assembler.subtreeProcessor
	processor.SetCurrentItemsPerFile(1) // Coinbase fills the tree; the first transaction fails after clearing.
	recovered, err := assembler.recoverUnminedTransactions(t.Context())
	require.Error(t, err)
	require.False(t, recovered)
	require.True(t, processor.RecoveryPending())
	_, _, lease, err := assembler.GetMiningCandidate(t.Context())
	lease.Release()
	require.Error(t, err, "partially rebuilt assembly must not issue mining work")

	next := &model.BlockHeader{
		Version: header.Version, HashPrevBlock: header.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: header.Timestamp + 1, Bits: header.Bits, Nonce: 932,
	}
	require.NoError(t, assembler.blockchainClient.AddBlock(t.Context(), &model.Block{
		Header: next, CoinbaseTx: coinbaseTxForHeader(t, next), TransactionCount: 1, Subtrees: []*chainhash.Hash{},
	}, "", options.WithMinedSet(true)))
	assembler.processNewBlockAnnouncement(t.Context())
	current, _ := assembler.CurrentBlock()
	require.Equal(t, header.Hash(), current.Hash(), "chain movement must wait for a complete assembly")
	require.Error(t, assembler.reset(t.Context()), "legacy reset must not bypass read-only repair")

	processor.SetCurrentItemsPerFile(32)
	recovered, err = assembler.recoverUnminedTransactions(t.Context())
	require.NoError(t, err)
	require.True(t, recovered, "repair at the anchored old tip must work after the chain advances")
	require.False(t, processor.RecoveryPending())
	_, _, lease, err = assembler.GetMiningCandidate(t.Context())
	lease.Release()
	require.Error(t, err, "repaired old-tip work stays blocked until chain reconciliation")
	require.Equal(t, time.Minute, assembler.nextUnminedRecoveryDelay(recovered), "an outstanding mining gate must retain short retry cadence")
	assembler.processNewBlockAnnouncement(t.Context())
	current, currentHeight := assembler.CurrentBlock()
	require.Equal(t, next.Hash(), current.Hash())
	require.Equal(t, height+1, currentHeight)
	require.Contains(t, recoveryCandidateHashes(t, assembler), txID)
}

func TestUnminedRecoveryTimerWorksWithoutNewBlock(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) {
		s.BlockAssembly.UnminedRecoveryInterval = 25 * time.Millisecond
	})
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	beforeHeader, beforeHeight := assembler.CurrentBlock()
	require.NotContains(t, recoveryCandidateHashes(t, assembler), txID)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	require.Eventually(t, func() bool {
		return recoveryCandidateContains(ctx, assembler, txID)
	}, 5*time.Second, 10*time.Millisecond, "listener timer must repair a suppressed transaction without a new block")
	cancel()
	assembler.wg.Wait()
	afterHeader, afterHeight := assembler.CurrentBlock()
	require.Equal(t, beforeHeader.Hash(), afterHeader.Hash())
	require.Equal(t, beforeHeight, afterHeight)
}

// All storage remains SQLite; only the first index-open result is fault-injected.
type recoveryIteratorFailureStore struct {
	utxo.Store
	attempts atomic.Int64
}

func (s *recoveryIteratorFailureStore) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	if s.attempts.Add(1) == 1 {
		return nil, errors.NewProcessingError("injected transient unmined index failure")
	}
	return s.Store.GetUnminedTxIterator()
}

func TestUnminedRecoveryTimerRetriesFailedReload(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) {
		s.BlockAssembly.UnminedRecoveryInterval = 25 * time.Millisecond
	})
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	faultStore := &recoveryIteratorFailureStore{Store: assembler.utxoStore}
	assembler.utxoStore = faultStore
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	require.Eventually(t, func() bool {
		return faultStore.attempts.Load() >= 2 && recoveryCandidateContains(ctx, assembler, txID)
	}, 5*time.Second, 10*time.Millisecond, "failed reload must remain eligible for an automatic retry")
	cancel()
	assembler.wg.Wait()
}

type recoveryDelayedIteratorStore struct {
	utxo.Store
	attempts      atomic.Int64
	firstStarted  chan struct{}
	releaseFirst  chan struct{}
	secondStarted chan time.Time
}

func (s *recoveryDelayedIteratorStore) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	switch s.attempts.Add(1) {
	case 1:
		close(s.firstStarted)
		<-s.releaseFirst
		return nil, errors.NewProcessingError("injected slow unmined index failure")
	case 2:
		s.secondStarted <- time.Now()
	}
	return s.Store.GetUnminedTxIterator()
}

func TestUnminedRecoveryTimerWaitsAfterSlowFailure(t *testing.T) {
	const interval = 100 * time.Millisecond
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) {
		s.BlockAssembly.UnminedRecoveryInterval = interval
	})
	faultStore := &recoveryDelayedIteratorStore{
		Store: assembler.utxoStore, firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}), secondStarted: make(chan time.Time, 1),
	}
	assembler.utxoStore = faultStore
	ctx, cancel := context.WithCancel(t.Context())
	release := sync.OnceFunc(func() { close(faultStore.releaseFirst) })
	t.Cleanup(func() { release(); cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	select {
	case <-faultStore.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first reload did not start")
	}
	// A fixed-rate ticker would retain an expired tick while this pass is
	// blocked and immediately start another expensive reload on completion.
	time.Sleep(3 * interval)
	released := time.Now()
	release()
	select {
	case secondStarted := <-faultStore.secondStarted:
		require.GreaterOrEqual(t, secondStarted.Sub(released), interval, "retry delay must begin after the preceding pass completes")
	case <-time.After(5 * time.Second):
		t.Fatal("failed reload was not retried")
	}
	cancel()
	assembler.wg.Wait()
}

func TestUnminedRecoveryChecksStateBeforePendingBlocks(t *testing.T) {
	for _, state := range []blockchain.FSMStateType{blockchain.FSMStateIDLE, blockchain.FSMStateCATCHINGBLOCKS, blockchain.FSMStateRUNNING} {
		t.Run(state.String(), func(t *testing.T) {
			assembler, _ := newUnminedRecoveryTestAssembler(t, state)
			header, _ := assembler.CurrentBlock()
			next := &model.BlockHeader{Version: header.Version, HashPrevBlock: header.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: header.Timestamp + 1, Bits: header.Bits, Nonce: 933}
			require.NoError(t, assembler.blockchainClient.AddBlock(t.Context(), &model.Block{Header: next, CoinbaseTx: coinbaseTxForHeader(t, next), TransactionCount: 1, Subtrees: []*chainhash.Hash{}}, "", options.WithMinedSet(false), options.WithSubtreesSet(true)))
			pending, err := assembler.blockchainClient.GetBlocksMinedNotSet(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, pending)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			recovered, err := assembler.recoverUnminedTransactions(ctx)
			require.False(t, recovered)
			if state == blockchain.FSMStateRUNNING {
				require.Error(t, err, "RUNNING must still wait for pending mined flags")
			} else {
				require.NoError(t, err, "IDLE and catchup must defer without waiting for mined flags")
				require.NoError(t, ctx.Err())
			}
		})
	}
}

func TestUnminedRecoveryDisabledDoesNotScan(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) { s.BlockAssembly.UnminedRecoveryInterval = -time.Second })
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	iterator := &recoveryIteratorFailureStore{Store: assembler.utxoStore}
	assembler.utxoStore = iterator
	recovered, err := assembler.recoverUnminedTransactions(t.Context())
	require.NoError(t, err)
	require.False(t, recovered)
	require.Zero(t, iterator.attempts.Load())
	require.Zero(t, assembler.nextUnminedRecoveryDelay(false), "disabled recovery must have no timer")
	require.NotContains(t, recoveryCandidateHashes(t, assembler), txID)
}

func TestUnminedRecoveryDisabledKeepsPendingRepair(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	txID := storeSuppressedRecoveryTransaction(t, assembler)
	processor := assembler.subtreeProcessor
	processor.SetCurrentItemsPerFile(1)
	recovered, err := assembler.recoverUnminedTransactions(t.Context())
	require.Error(t, err)
	require.False(t, recovered)
	require.True(t, processor.RecoveryPending())
	assembler.settings.BlockAssembly.UnminedRecoveryInterval = -time.Second
	require.Equal(t, time.Minute, assembler.nextUnminedRecoveryDelay(false), "disabling new scans must retain already-started repair responsibility")
	processor.SetCurrentItemsPerFile(32)
	recovered, err = assembler.recoverUnminedTransactions(t.Context())
	require.NoError(t, err)
	require.True(t, recovered)
	require.False(t, processor.RecoveryPending())
	require.Contains(t, recoveryCandidateHashes(t, assembler), txID)
	require.Zero(t, assembler.nextUnminedRecoveryDelay(true))
}
