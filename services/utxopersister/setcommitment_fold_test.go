package utxopersister

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/muhash"
	"github.com/bsv-blockchain/teranode/pkg/utxoseed"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func writePreviousSet(t *testing.T, store *memory.Memory, prevHash, grandparentHash chainhash.Hash, height uint32, wrappers []*UTXOWrapper) {
	t.Helper()

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], height)

	body := make([]byte, 0)
	body = append(body, prevHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)

	var utxoCount uint64
	for _, w := range wrappers {
		body = append(body, w.Bytes()...)
		utxoCount += uint64(len(w.UTXOs))
	}

	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(len(wrappers)))
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	body = append(body, footer[:]...)

	require.NoError(t, store.Set(context.Background(), prevHash[:], fileformat.FileTypeUtxoSet, body))
}

func independentDigest(wrappers []*UTXOWrapper) [32]byte {
	m := muhash.New()

	for _, w := range wrappers {
		for _, u := range w.UTXOs {
			m.Add(utxoseed.Element(w.TxID, u.Index, w.Height, w.Coinbase, u.Value, u.Script))
		}
	}

	return m.Digest()
}

func TestCreateUTXOSetComputesSetHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	prevHash := chainhash.HashH([]byte("prev-block"))
	currentHash := chainhash.HashH([]byte("current-block"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block"))

	txA := chainhash.HashH([]byte("tx-a"))
	txB := chainhash.HashH([]byte("tx-b"))

	wrappers := []*UTXOWrapper{
		{TxID: txA, Height: 100, Coinbase: true, UTXOs: []*UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}}},
		{TxID: txB, Height: 101, Coinbase: false, UTXOs: []*UTXO{
			{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9}},
			{Index: 1, Value: 2000, Script: []byte{0x6a}},
		}},
	}

	writePreviousSet(t, store, prevHash, grandparentHash, 99, wrappers)

	c := NewConsolidator(logger, tSettings, nil, nil, store, &prevHash)
	c.lastBlockHash = &currentHash
	c.lastBlockHeight = 100
	c.previousBlockHash = &prevHash

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &currentHash)
	require.NoError(t, err)

	require.NoError(t, us.CreateUTXOSet(ctx, c))

	require.Equal(t, independentDigest(wrappers), c.acc.Digest(),
		"set hash from CreateUTXOSet must equal an independent fold over the surviving UTXOs")
}

func TestSetHashOrderIndependent(t *testing.T) {
	txA := chainhash.HashH([]byte("tx-a"))
	txB := chainhash.HashH([]byte("tx-b"))

	forward := []*UTXOWrapper{
		{TxID: txA, Height: 100, Coinbase: true, UTXOs: []*UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}}},
		{TxID: txB, Height: 101, Coinbase: false, UTXOs: []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9}}}},
	}

	reverse := []*UTXOWrapper{forward[1], forward[0]}

	require.Equal(t, independentDigest(forward), independentDigest(reverse))
}
