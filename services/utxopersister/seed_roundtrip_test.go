package utxopersister

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/muhash"
	"github.com/bsv-blockchain/teranode/pkg/seedpack"
	"github.com/bsv-blockchain/teranode/pkg/utxoseed"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestProductionSeedIsConsumerReadable is the producer half of the full
// production-to-production round trip. It drives the *real* producer pipeline
// end to end — consolidator + CreateUTXOSet (which writes the on-disk set file
// and folds c.acc), persistSetHash, BuildSeedPackage, BuildSignedCheckpoint —
// then reads the producer's own bytes back through StreamSeedPackage and
// re-parses them exactly the way cmd/seedimport's streamLoad does.
//
// This closes the framing-drift gap: previously the consumer tests hand-rolled
// the utxo-set body framing (block-hash header + footer) rather than exercising
// the bytes CreateUTXOSet actually writes. Here the framing, the chunk
// reassembly, and the MuHash fold are all driven from real producer output, and
// the recomputed digest is checked against the *signed* checkpoint — the same
// integrity gate the consumer applies.
//
// seedimport.Run itself cannot be invoked here: package seedimport imports this
// package, so importing it back would create a cycle. The consumer half of the
// round trip (the real Run) lives in
// cmd/seedimport/seedimport.TestRunConsumesProductionBuiltSeed, which ingests a
// seed produced by these same BuildSeedPackage / BuildSignedCheckpoint
// functions. The two tests meet at the serialized on-disk artifacts a real
// deployment transfers.
func TestProductionSeedIsConsumerReadable(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	prevHash := chainhash.HashH([]byte("rt-prev-block"))
	currentHash := chainhash.HashH([]byte("rt-current-block"))
	grandparentHash := chainhash.HashH([]byte("rt-grandparent-block"))

	txA := chainhash.HashH([]byte("rt-tx-a"))
	txB := chainhash.HashH([]byte("rt-tx-b"))

	wrappers := []*UTXOWrapper{
		{TxID: txA, Height: 100, Coinbase: true, UTXOs: []*UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}}},
		{TxID: txB, Height: 101, Coinbase: false, UTXOs: []*UTXO{
			{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9}},
			{Index: 1, Value: 2000, Script: []byte{0x6a}},
		}},
	}

	const seedHeight = uint32(102)

	// --- PRODUCER: materialize the set via the real consolidator. ---
	writePreviousSet(t, store, prevHash, grandparentHash, 99, wrappers)

	c := NewConsolidator(logger, tSettings, nil, nil, store, &prevHash)
	c.lastBlockHash = &currentHash
	c.lastBlockHeight = seedHeight
	c.previousBlockHash = &prevHash

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &currentHash)
	require.NoError(t, err)

	require.NoError(t, us.CreateUTXOSet(ctx, c))

	setHash := c.acc.Digest()
	require.NoError(t, persistSetHash(ctx, store, &currentHash, setHash))

	// Read the body the producer actually wrote (Get strips the format magic),
	// and assert the framing the consumer relies on: a 68-byte header of
	// blockHash(32) | height(4 LE) | prevHash(32) before the first UTXOWrapper.
	bodyReader, err := store.GetIoReader(ctx, currentHash[:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)

	body, err := io.ReadAll(bodyReader)
	require.NoError(t, err)
	require.NoError(t, bodyReader.Close())

	require.GreaterOrEqual(t, len(body), 68, "body must carry the per-file header")
	require.Equal(t, currentHash[:], body[0:32], "header must lead with the current block hash")
	require.Equal(t, seedHeight, binary.LittleEndian.Uint32(body[32:36]), "header height must match")
	require.Equal(t, prevHash[:], body[36:68], "header must carry the previous block hash")

	// --- PRODUCER: package + sign over that exact body. ---
	cfg := seedpack.Config{Min: 16, Max: 256, Mask: (1 << 6) - 1}
	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(body), seedHeight, currentHash, setHash, cfg))

	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := BuildSignedCheckpoint(ctx, store, currentHash, seedHeight, priv, testNetMagic)
	require.NoError(t, err)
	require.Equal(t, setHash, sc.Checkpoint.SetHash, "signed set hash must be the producer's own commitment")
	require.NoError(t, sc.VerifyWithKey(priv.PubKey().Compressed(), testNetMagic))

	// --- CONSUMER: reassemble from chunks and verify integrity. ---
	var reassembled bytes.Buffer
	require.NoError(t, StreamSeedPackage(ctx, store, currentHash, &reassembled))
	require.Equal(t, body, reassembled.Bytes(), "streamed seed must reproduce the producer's body byte-for-byte")

	// --- CONSUMER: re-parse exactly as cmd/seedimport's streamLoad does and
	// fold the surviving UTXOs back into a MuHash, then check it equals the
	// SIGNED set hash. This is the consumer's integrity gate, run over real
	// producer bytes. ---
	r := bytes.NewReader(reassembled.Bytes())

	var hdr [68]byte
	_, err = io.ReadFull(r, hdr[:])
	require.NoError(t, err)

	var streamedBlockHash chainhash.Hash
	copy(streamedBlockHash[:], hdr[0:32])
	require.Equal(t, currentHash, streamedBlockHash, "streamed body must bind to the seed block")

	acc := muhash.New()

	// The producer wrote exactly len(wrappers) records, so read that many
	// directly — no need to infer end-of-stream from a read error.
	for range wrappers {
		w, err := NewUTXOWrapperFromReader(ctx, r)
		require.NoError(t, err)

		for _, u := range w.UTXOs {
			acc.Add(utxoseed.Element(w.TxID, u.Index, w.Height, w.Coinbase, u.Value, u.Script))
		}
	}
	require.Equal(t, sc.Checkpoint.SetHash, acc.Digest(),
		"MuHash folded over the producer's streamed UTXOs must equal the signed set hash")
	require.Equal(t, independentDigest(wrappers), acc.Digest(),
		"and must equal an independent fold over the surviving UTXOs")
}
