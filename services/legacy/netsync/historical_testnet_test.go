//go:build longtest

package netsync

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestLegacyHistoricalTestnetSync replays the first 547 testnet blocks through
// the legacy pipeline against the real service stack (legacyValidationStack): a
// headers message proves the run through testnet's first checkpoint at 546, each
// block streams through pipelineBlockSink into the file store as a converted
// record, and commitParkedBlock commits it through the blockvalidation server
// over gRPC.
//
// Height 547 is the one the two routes treat differently. It sits above the
// checkpoint the run matched and below testnet's last pinned checkpoint, so
// unifiedRoute(547) is true while the 546-header run proves nothing above 546.
// With the route flags off it takes full validation, unproven, and no
// checkpoint proof travels on that RPC: the historical difficulty calculator
// must work with release/v0.15's stricter proof gate too. On the unified route
// HandleConvertedBlock refuses to commit it, the retry-later shape that keeps
// the record parked until the header walk reaches the next checkpoint; in
// production Wantable would not have asked for it before then, and this test
// hands it over unasked to pin that refusal.
//
// Two subtests, the shipped default first, each over its own stack:
//
//   - "default-settings": both route flags false, the configuration an unmodified
//     node runs. Every block takes ValidateBlockWithOptions, which is the route PR
//     1732 wrote this test for: the expected-nBits rule it added lives there and
//     nowhere on the quick route, so this subtest is the one that exercises it
//     over real testnet headers. Ends at height 547.
//   - "unified-below-checkpoint": both true, the configuration the Hetzner nodes
//     soak. Heights 1 to 546 take quickValidateBlock on real testnet shapes for
//     the first time in a test: 380 coinbase-only bodies through the zero-subtree
//     AssignBlockID branch, then 112 blocks of two to forty-nine
//     transactions from height 381 on, through bindSubtreeBodyToHeader over
//     sink-written .subtreeToCheck files, spending coinbases and each other.
//     Height 547 is refused, kept parked and stamped; the chain ends at 546.
//
// Each committed height asserts which route block validation took, by its
// outpoint-only blocks counter (outpointOnlyBlocks): one more on the unified
// subtest, none on the default one. Without it both subtests passed with the
// server's unified route switched off, because a block that commits looks the
// same in the chain on either route.
//
// The download ledger is bypassed on purpose: the test calls the sink and the
// commit directly, never handleBlockOnDiskMsg, so the wanted-range pass that the
// headers fill and each commit run never has its owners released and goes quiet
// after perPeerDepth blocks. That is expected and asserts nothing.
//
// Two steps other services own are taken by the test after each commit. Block
// assembly normally creates the accepted block's coinbase outputs; neither route
// creates them (extendBatch skips the coinbase slot; full validation never did),
// so the test creates them, and a route that started doing so would fail that
// create with ErrTxExists, a finding rather than something to tolerate. Then the
// blockchain server's BlockSubtreesSet notification, which the LocalClient does
// not send, is sent so block validation's own setMined worker stamps the block's
// transactions and sets mined_set; see requireBlockMinedByValidation. The next
// block's parent-mined wait on the default route reads that flag, and its spends
// of this block's transactions need the stamps.
func TestLegacyHistoricalTestnetSync(t *testing.T) {
	t.Run("default-settings", func(t *testing.T) { runHistoricalTestnetSync(t, false) })
	t.Run("unified-below-checkpoint", func(t *testing.T) { runHistoricalTestnetSync(t, true) })
}

func runHistoricalTestnetSync(t *testing.T, unified bool) {
	t.Helper()

	started := time.Now()

	s := test.CreateBaseTestSettings(t)
	params := chaincfg.TestNetParams
	s.ChainCfgParams = &params
	s.BlockValidation.OptimisticMining = false
	s.BlockValidation.IsParentMinedRetryMaxRetry = 1
	s.BlockAssembly.Disabled = true
	s.GlobalBlockHeightRetention = 1000
	s.BlockValidation.OutpointOnlyBelowCheckpoint = unified
	s.BlockValidation.LegacyUnifiedBelowCheckpoint = unified

	stack := newLegacyValidationStack(t, s)
	ctx := stack.ctx
	sm := newLegacySyncManager(t, stack, s, &params)

	// The route netsync's own gates will take below the checkpoint; the server
	// reads the same settings and store, so this pins which subtest proves what.
	require.Equal(t, unified, sm.unifiedRoute(1), "the route flags must select the route this subtest names")

	// Outbound and asked: below the last checkpoint only a peer this node sent
	// a getheaders to may fill the header cache, and only outbound peers are
	// sent one. The peer is not connected, so the getheaders itself is dropped.
	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, s, &peer.Config{}, "10.0.0.1:18333")
	require.NoError(t, err)
	registerRacePeer(sm, p)
	sm.storeSyncPeer(p, &syncPeerState{})
	sm.headersFirstMode.Store(true)
	askForHeaders(t, sm, p)

	data, err := os.ReadFile("testdata/testnet_blocks_1_547.gz")
	require.NoError(t, err)
	reader, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	blocks := make([]*wire.MsgBlock, 547)
	headers := wire.NewMsgHeaders()

	for i := range blocks {
		var size uint32
		require.NoError(t, binary.Read(reader, binary.LittleEndian, &size))
		require.LessOrEqual(t, size, uint32(100_000))

		data := make([]byte, size)
		_, err := io.ReadFull(reader, data)
		require.NoError(t, err)

		blocks[i] = &wire.MsgBlock{}
		require.NoError(t, blocks[i].Deserialize(bytes.NewReader(data)))

		if i < 546 {
			require.NoError(t, headers.AddBlockHeader(&blocks[i].Header))
		}
	}

	// The production handler: the run links to the committed genesis and ends at
	// the pinned hash, so the cache proves every height through 546.
	sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
	require.Equal(t, int32(546), sm.headerCache.ProvenTo(), "the 546-header run must prove through the first testnet checkpoint")

	for i, block := range blocks {
		height := uint32(i + 1) //nolint:gosec // 547 blocks
		hash := block.Header.BlockHash()

		origin := sm.blockOrigin(hash)
		if height <= 546 {
			require.True(t, origin.headerProven, "height %d must come from the verified header run", height)
		} else {
			require.False(t, origin.headerProven, "height %d is above the checkpoint the run matched and the headers run never named it", height)
		}

		if height == 149 {
			require.Equal(t, "00000000291d8e6f5d0d2a59de8f0f206917f3e00ff53edc8f6dcaddd61f3fe9", hash.String())

			exists, err := stack.chain.GetBlockExists(ctx, params.Checkpoints[0].Hash)
			require.NoError(t, err)
			require.False(t, exists, "checkpoint evidence is only in netsync, not blockchain storage")
		}

		// The wire layer hands the sink the body after the header, with the
		// message's declared payload length.
		body := blockBodyBytes(t, bsvutil.NewBlock(block))

		converted, err := sm.pipelineBlockSink(hash, &block.Header, bytes.NewReader(body), sinkPayloadLen(body))
		require.NoError(t, err, "height %d must convert", height)
		require.True(t, converted, "height %d must convert", height)

		// Through the park, as the on-disk handler and the drain do: adopt the
		// record, then take it. A commit's keep row puts back only a block that
		// was taken from the park (blockPark.Restore).
		require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hash, prevBlock: block.Header.PrevBlock, peer: p}), "height %d must be adopted", height)

		entry, ok := sm.blockPark.Take(hash)
		require.True(t, ok, "height %d must be taken", height)

		if unified && height == 547 {
			// Below testnet's last pinned checkpoint and unproven, so the
			// unified route applies and HandleConvertedBlock refuses the commit.
			// The disposition is retry-later: the record is kept, the entry goes
			// back into the park stamped so the drain does not spin on it, and
			// nothing reaches the chain.
			require.True(t, sm.unifiedRoute(height), "height %d is below testnet's last pinned checkpoint, so the unified route applies to it", height)
			require.False(t, sm.commitParkedBlock(entry), "height %d must be refused: on the unified route and not proven by the header walk", height)
			require.True(t, sm.blockPark.Has(hash), "height %d must stay parked for the header walk to prove", height)

			parked, ok := sm.blockPark.entries[hash]
			require.True(t, ok)
			require.False(t, parked.awaitingProofAt.IsZero(), "height %d must be stamped so the drain leaves it alone within the floor", height)

			exists, err := stack.chain.GetBlockExists(ctx, &hash)
			require.NoError(t, err)
			require.False(t, exists, "height %d must not reach the chain unproven", height)

			break
		}

		// The commit runs the disposition that deletes the record, so the park
		// does not accumulate 547 entries over the run.
		quickBefore := outpointOnlyBlocks(t)

		require.True(t, sm.commitParkedBlock(entry), "height %d must commit", height)

		// Which route block validation took for this height. The chain looks the
		// same after either, so this is the one assertion that tells them apart.
		if unified {
			require.Equal(t, quickBefore+1, outpointOnlyBlocks(t), "height %d must take quickValidateBlock on the unified route", height)
		} else {
			require.Equal(t, quickBefore, outpointOnlyBlocks(t), "height %d must not take quickValidateBlock on the default route", height)
		}

		_, meta, err := stack.chain.GetBlockHeader(ctx, &hash)
		require.NoError(t, err, "height %d must be in the chain", height)
		require.Equal(t, height, meta.Height)
		require.False(t, meta.Invalid)

		accepted, err := model.NewBlockFromMsgBlock(block, s)
		require.NoError(t, err)

		_, _, err = stack.utxos.SpendAndCreate(ctx, accepted.CoinbaseTx, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: height}))
		require.NoError(t, err, "height %d: the coinbase must not already be in the store", height)

		requireBlockMinedByValidation(t, stack, hash)
	}

	wantBest := uint32(547)
	if unified {
		wantBest = 546
	}

	_, best, err := stack.chain.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, wantBest, best.Height)

	t.Logf("replayed 547 testnet blocks, committed %d (unified=%v) in %s", wantBest, unified, time.Since(started).Round(time.Second))
}
