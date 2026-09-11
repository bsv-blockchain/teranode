package netsync

import (
	"container/list"
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Use the real SQL blockchain client so provenance tests exercise inventory
// lookups and block delivery without mocked chain state.
func newHeaderProvenanceManager(t *testing.T) (*SyncManager, *peer.Peer, *peerSyncState) {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	params := tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 33_333, Hash: &chainhash.Hash{0x7f}}}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})
	state := &peerSyncState{requestedBlocks: expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Hour)}
	t.Cleanup(state.requestedBlocks.Stop)
	sm := &SyncManager{
		ctx: context.Background(), logger: ulogger.TestLogger{}, settings: tSettings,
		chainParams: params, blockchainClient: client,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Minute),
		headerList:      list.New(), blockSizeTracker: newBlockSizeTracker(10),
	}
	t.Cleanup(sm.requestedBlocks.Stop)
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})
	sm.headersFirstMode.Store(true)
	return sm, p, state
}

func TestHeaderProvenance_CheckpointAdvanceDoesNotVerifyTail(t *testing.T) {
	sm, p, state := newHeaderProvenanceManager(t)
	anchor := hashFrom(0x41)
	checkpointHeader := wire.NewBlockHeader(1, &anchor, &chainhash.Hash{}, 0x207fffff, 0)
	checkpointHash := checkpointHeader.BlockHash()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 11_111, Hash: &checkpointHash}
	sm.headerList.PushBack(&headerNode{height: 11_110, hash: &anchor})
	headers := wire.NewMsgHeaders()
	require.NoError(t, headers.AddBlockHeader(checkpointHeader))
	sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
	require.True(t, sm.blockOrigin(state, checkpointHash).headerProven,
		"matching the pinned hash must preserve the IBD fast path")

	// Block delivery advances the pending checkpoint independently of header
	// verification. No header in the next interval has matched a pinned hash.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 33_333, Hash: &chainhash.Hash{0x7f}}
	tailHeader := wire.NewBlockHeader(1, &checkpointHash, &chainhash.Hash{}, 0x207fffff, 1)
	tailHash := tailHeader.BlockHash()
	tail := wire.NewMsgHeaders()
	require.NoError(t, tail.AddBlockHeader(tailHeader))
	sm.handleHeadersMsg(&headersMsg{headers: tail, peer: p})
	sm.fetchHeaderBlocks() // also triggered by a delayed block from the previous run
	origin, requested := state.requestedBlocks.Get(tailHash)
	require.True(t, requested, "exercise the production request-provenance write")
	require.False(t, origin.headerProven, "advancing the pending checkpoint must not bless the unverified tail")
	require.False(t, sm.quickValidationAllowed(origin, 11_112))
}

func TestHeaderProvenance_UnmatchedCheckpointDeniesRequests(t *testing.T) {
	sm, p, state := newHeaderProvenanceManager(t)
	anchor := hashFrom(0x42)
	header := wire.NewBlockHeader(1, &anchor, &chainhash.Hash{}, 0x207fffff, 0)
	hash := header.BlockHash()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 11_111, Hash: &chainhash.Hash{0x7f}}
	sm.headerList.PushBack(&headerNode{height: 11_110, hash: &anchor})
	headers := wire.NewMsgHeaders()
	require.NoError(t, headers.AddBlockHeader(header))
	sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
	// A failed checkpoint comparison currently leaves the node in the list.
	// Even if another block delivery requests it, it must never acquire trust.
	sm.fetchHeaderBlocks()
	require.False(t, sm.blockOrigin(state, hash).headerProven)
}

func TestHandleBlockDirect_RejectsPoWBeforeAssemblyWait(t *testing.T) {
	sm, p, _ := newHeaderProvenanceManager(t)
	sm.chainParams = &chaincfg.MainNetParams
	// Any call to this mock is unexpected: both PoW rejections must precede it.
	sm.blockAssembly = blockassembly.NewMock()
	for _, bits := range []uint32{0x207fffff, 0} {
		block := makeDuplicateTxidBlock(1).MsgBlock()
		block.Header.PrevBlock = *sm.settings.ChainCfgParams.GenesisHash
		block.Header.Bits = bits
		if bits != 0 {
			require.True(t, solveBlock(&block.Header, chaincfg.RegressionNetParams.PowLimit))
		}
		initPrometheusMetrics()
		err := sm.HandleBlockDirect(sm.ctx, p, block.Header.BlockHash(), block, blockRequestOrigin{})
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "%v", err)
		if bits != 0 {
			require.ErrorContains(t, err, "block declares a target easier than the network proof-of-work limit")
		} else {
			require.ErrorContains(t, err, "invalid block header")
			require.ErrorContains(t, err, "block header does not meet target")
			require.NotContains(t, err.Error(), "%!")
		}
	}
}

func TestHeaderProvenance_CheckpointBlockDelivery(t *testing.T) {
	for _, final := range []bool{false, true} {
		name := "next checkpoint"
		if final {
			name = "final checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			sm, p, state := newHeaderProvenanceManager(t)
			block := makeDuplicateTxidBlock(1).MsgBlock()
			block.Header.PrevBlock = *sm.chainParams.GenesisHash
			block.Header.Timestamp = time.Unix(1231006505, 0)
			// An already-stored block exercises successful delivery bookkeeping
			// without coupling the checkpoint transition test to UTXO validation.
			stored, err := model.NewBlockFromMsgBlock(block, nil)
			require.NoError(t, err)
			require.NoError(t, sm.blockchainClient.AddBlock(sm.ctx, stored, "test"))
			hash := block.Header.BlockHash()
			sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: &hash}}
			if !final {
				sm.chainParams.Checkpoints = append(sm.chainParams.Checkpoints, chaincfg.Checkpoint{Height: 3, Hash: &chainhash.Hash{0x7f}})
			}
			sm.nextCheckpoint = &sm.chainParams.Checkpoints[0]
			sm.verifiedCheckpointHeight = 1
			sm.headerList.PushBack(&headerNode{height: 1, hash: &hash})
			sm.startHeader = sm.headerList.PushBack(&headerNode{height: 2, hash: &chainhash.Hash{0x44}})
			sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}]()
			state.requestedBlocks.Set(hash, blockRequestOrigin{headerProven: true})
			sm.requestedBlocks.Set(hash, blockRequestOrigin{headerProven: true})

			require.NoError(t, sm.handleBlockMsg(&blockQueueMsg{block: block, blockHash: hash, blockHeight: 1, peer: p}))
			if final {
				require.Nil(t, sm.nextCheckpoint)
				require.Zero(t, sm.verifiedCheckpointHeight)
				require.Zero(t, sm.headerList.Len())
				require.Nil(t, sm.startHeader)
				require.False(t, sm.headersFirstMode.Load())
			} else {
				require.Equal(t, int32(3), sm.nextCheckpoint.Height)
				require.Equal(t, int32(1), sm.verifiedCheckpointHeight)
				require.False(t, sm.headerNodeProven(sm.startHeader.Value.(*headerNode)))
			}
		})
	}
}

// Drive the actual asynchronous header handler against a shared anchor. A
// sibling of an interior header must never inherit the matched run's proof.
func TestHeaderProvenance_ConcurrentHeaders(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			sm, p, state := newHeaderProvenanceManager(t)
			anchor := hashFrom(0x51)
			sm.headerList.PushBack(&headerNode{height: 100, hash: &anchor})
			headers := wire.NewMsgHeaders()
			prev := anchor
			var sibling *wire.BlockHeader
			for i := 0; i < 64; i++ {
				header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{}, 0x207fffff, uint32(i))
				require.NoError(t, headers.AddBlockHeader(header))
				if i == 32 {
					sibling = wire.NewBlockHeader(1, &prev, &chainhash.Hash{1}, 0x207fffff, 99)
				}
				prev = header.BlockHash()
			}
			sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 164, Hash: &prev}
			forgedHash := sibling.BlockHash()
			forged := wire.NewMsgHeaders()
			require.NoError(t, forged.AddBlockHeader(sibling))
			start := make(chan struct{})
			var wg sync.WaitGroup
			for _, msg := range []*wire.MsgHeaders{headers, forged} {
				wg.Add(1)
				go func(msg *wire.MsgHeaders) {
					defer wg.Done()
					<-start
					sm.handleHeadersMsg(&headersMsg{headers: msg, peer: p})
				}(msg)
			}
			close(start)
			wg.Wait()
			// Drain the genuine run so a bad sibling cannot hide beyond the first
			// request batch. Preserve the request records for the final assertion.
			for sm.startHeader != nil {
				// The global and peer request maps are only dedupe/admission state here.
				// Remove genuine requests to make room for the remaining header entries.
				for _, header := range headers.Headers {
					state.requestedBlocks.Delete(header.BlockHash())
				}
				sm.fetchHeaderBlocks()
			}
			require.False(t, sm.blockOrigin(state, forgedHash).headerProven)
			require.Equal(t, int32(164), sm.verifiedCheckpointHeight)
		})
	}
}

func TestHeaderProvenance_ConcurrentReset(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			sm, p, _ := newHeaderProvenanceManager(t)
			anchor := hashFrom(0x52)
			header := wire.NewBlockHeader(1, &anchor, &chainhash.Hash{}, 0x207fffff, 1)
			hash := header.BlockHash()
			sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 101, Hash: &hash}
			sm.headerList.PushBack(&headerNode{height: 100, hash: &anchor})
			headers := wire.NewMsgHeaders()
			require.NoError(t, headers.AddBlockHeader(header))
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(3)
			go func() { defer wg.Done(); <-start; sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p}) }()
			go func() { defer wg.Done(); <-start; sm.resetHeaderState(&anchor, 100) }()
			go func() { defer wg.Done(); <-start; sm.fetchHeaderBlocks() }()
			close(start)
			wg.Wait()
			require.Zero(t, sm.verifiedCheckpointHeight)
			require.Nil(t, sm.startHeader)
			require.Equal(t, 1, sm.headerList.Len())
		})
	}
}
