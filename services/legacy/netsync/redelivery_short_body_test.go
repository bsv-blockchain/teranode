package netsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// TestAShortRedeliveryThroughTheWireKeepsTheParkedBlock drives both deliveries through the real
// streaming path: go-wire's ReadMessageWithEncodingN, the registered block handler, and the
// sink, gate and delete this manager installs.
//
// The first delivery is the honest block at exactly its declared length, which must convert and
// park. The second replays the same body under a message header that declares more payload than
// it sends, and the connection then ends after the last transaction. The stream sees a clean
// io.EOF there, because an io.LimitedReader passes the underlying EOF through while bytes are
// still owed. Before the sink held the body to its declared length, the redelivery converted,
// overwrote the parked record with an identical one, reported converted, and was then refused by
// readBlockMessage's own short-body check, whose cleanup (pipelineBlockDelete) read that record
// back and deleted every subtree file it names. Those files are keyed by root, so they were the
// parked copy's files too.
func TestAShortRedeliveryThroughTheWireKeepsTheParkedBlock(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	msgBlock := wireBlockWithTxs(t, 40, false).MsgBlock()
	blk := bsvutil.NewBlock(msgBlock)
	blk.SetHeight(500)
	pipelineHeaderFixture(t, sm, blk)

	// The gate checks proof of work against the chain's floor, so give the header regtest's
	// target and find a nonce that meets it. The fixture's header hash is never used before this.
	msgBlock.Header.Bits = chaincfg.RegressionNetParams.PowLimitBits

	header := &msgBlock.Header
	for nonce := uint32(0); ; nonce++ {
		require.Less(t, nonce, uint32(1000), "regtest's target is met about every other nonce")

		header.Nonce = nonce
		hash := header.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, hash))

		if sm.streamingBlockGate(hash, header) == nil {
			break
		}
	}

	blk = bsvutil.NewBlock(msgBlock)
	blk.SetHeight(500)
	hash := *blk.Hash()
	proveBlockOrigin(t, sm, blk)

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	var framed bytes.Buffer
	_, err := wire.WriteMessageN(&framed, msgBlock, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	message := framed.Bytes()

	// An exactly sized delivery still converts.
	_, msg, _, err := wire.ReadMessageWithEncodingN(bytes.NewReader(message), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err, "a body of exactly its declared length must be accepted")

	onDisk, ok := msg.(*peerpkg.MsgBlockOnDisk)
	require.True(t, ok, "the streaming path hands back the body on disk, got %T", msg)
	require.True(t, onDisk.Converted, "the honest delivery converted")
	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hash, prevBlock: header.PrevBlock, wireSize: onDisk.Size}),
		"what handleBlockOnDiskMsg does with a converted delivery")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees)

	// The redelivery: the same bytes under a header declaring 4 KiB more than it sends. The 24-byte
	// message header carries the payload length at offset 16; the checksum is not checked on the
	// streaming path, which is the point of streaming.
	short := append([]byte(nil), message...)
	binary.LittleEndian.PutUint32(short[16:20], binary.LittleEndian.Uint32(short[16:20])+4096)

	_, _, _, err = wire.ReadMessageWithEncodingN(bytes.NewReader(short), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.Error(t, err, "a body shorter than declared must be refused")
	require.True(t, errors.IsBlockCorrupt(err), "refused inside the sink as corrupt, before its record is written: %v", err)

	require.True(t, sm.blockPark.Has(hash), "the parked block is still parked")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err, "the parked block's record still reads back")

	for _, root := range record.Subtrees {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, existsErr := store.Exists(ctx, root[:], ft)
			require.NoError(t, existsErr)
			require.True(t, exists, "the refused redelivery removed the parked block's %s file for subtree %s", ft, root)
		}
	}
}
