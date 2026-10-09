package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// A second copy of a block is kept in a side file and may take over the copy converting now. Its
// merkle root is checked first: a block can have two owners, and an owner's corrupt copy used to
// stop an honest conversion and then fail its own root check, losing both.

// The streamed root agrees with the header for odd and even counts at every level.
func TestStreamedMerkleRootMatchesTheHeader(t *testing.T) {
	for _, txs := range []int{1, 2, 3, 4, 5, 7, 8, 9, 40} {
		blk := wireBlockWithTxs(t, txs, false)
		body := blockBodyBytes(t, blk)

		root, mutated, err := streamedMerkleRoot(bytes.NewReader(body), sinkPayloadLen(body))
		require.NoError(t, err)
		require.Equal(t, blk.MsgBlock().Header.MerkleRoot, *root, "%d transactions", txs)
		require.False(t, mutated, "%d distinct transactions", txs)
	}
}

// A body that repeats the last transaction of an odd count keeps the honest root (CVE-2012-2459)
// and is flagged, as SV Node flags it (consensus/merkle.cpp:86).
func TestStreamedMerkleRootFlagsARepeatedLastTransaction(t *testing.T) {
	for _, txs := range []int{3, 5, 41} {
		blk := wireBlockWithTxs(t, txs, false)
		mutatedBody := blockBodyWithLastRepeated(t, blk)

		root, mutated, err := streamedMerkleRoot(bytes.NewReader(mutatedBody), sinkPayloadLen(mutatedBody))
		require.NoError(t, err)
		require.Equal(t, blk.MsgBlock().Header.MerkleRoot, *root, "the repeat keeps the root for %d transactions", txs)
		require.True(t, mutated, "%d transactions with the last one repeated", txs)
	}
}

// A second copy that repeats the last transaction keeps the header's root. It used to pass the
// root check, stop the honest conversion, and then fail on the duplicate in the sink: both copies
// were lost.
func TestARepeatedTransactionSecondCopyNeverTakesOverAnHonestConversion(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()

	blk := wireBlockWithTxs(t, 41, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)

	owner := owingPeer(t, sm, hash, 247)

	mutatedBody := blockBodyWithLastRepeated(t, blk)

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, slowR, n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:len(body)/2])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(mutatedBody), owner), sinkPayloadLen(mutatedBody), sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	// A copy that took over waits for the first copy's next bytes, so a takeover shows as no answer.
	var second sinkResult
	select {
	case second = <-secondDone:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the repeated-transaction copy took over the honest conversion", "yielding: %v", first.yielding())
	}

	require.Error(t, second.err)
	require.True(t, errors.Is(second.err, errors.ErrBlockBodyMismatch), "the copy is refused as the sink refuses a duplicate: %v", second.err)
	require.Contains(t, second.err.Error(), "repeats a transaction")
	require.False(t, second.converted)
	require.False(t, first.yielding(), "the honest conversion was never asked to stop")

	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the honest copy converts")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees)
	requireNoSideFiles(t, sm.blockPark.dir)
}

// blockBodyWithLastRepeated is blockBodyBytes with the last transaction sent twice and the count
// raised by one: for an odd count, the CVE-2012-2459 shape that keeps the merkle root.
func blockBodyWithLastRepeated(t *testing.T, blk *bsvutil.Block) []byte {
	t.Helper()

	txs := blk.Transactions()

	var buf bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, uint64(len(txs)+1)))

	for _, tx := range txs {
		require.NoError(t, tx.MsgTx().Serialize(&buf))
	}

	require.NoError(t, txs[len(txs)-1].MsgTx().Serialize(&buf))

	return buf.Bytes()
}

func TestACorruptSecondCopyNeverTakesOverAnHonestConversion(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)

	owner := owingPeer(t, sm, hash, 248)

	// The last byte is the last transaction's lock time: the copy parses, and its root differs.
	corrupt := append([]byte(nil), body...)
	corrupt[len(corrupt)-1] ^= 0x01

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, slowR, n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:len(body)/2])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(corrupt), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	// A copy that took over waits for the first copy to stop, which needs the first copy's next
	// bytes, so a takeover shows as no answer here.
	var second sinkResult
	select {
	case second = <-secondDone:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the corrupt copy took over the honest conversion", "yielding: %v", first.yielding())
	}

	require.Error(t, second.err)
	require.True(t, errors.Is(second.err, errors.ErrBlockBodyMismatch), "the corrupt copy is refused as the sink refuses a wrong root: %v", second.err)
	require.False(t, second.converted)
	require.False(t, first.yielding(), "the honest conversion was never asked to stop")

	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the honest copy converts")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees)
	requireNoSideFiles(t, sm.blockPark.dir)
}
