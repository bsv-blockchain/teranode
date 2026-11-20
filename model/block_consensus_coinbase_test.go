package model

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newLooseCoinbaseTx returns a transaction go-bt calls a coinbase and consensus does
// not: a null prevout HASH with index 0, carried by a 0xFFFFFFFF sequence number.
// go-bt's Tx.IsCoinbase accepts the index OR the sequence; COutPoint::IsNull requires
// the null hash AND index 0xFFFFFFFF, so svnode rejects it.
func newLooseCoinbaseTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
	tx.Inputs[0].SequenceNumber = 0xFFFFFFFF
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))

	require.True(t, tx.IsCoinbase(), "precondition: go-bt accepts this shape")
	require.False(t, IsConsensusCoinbase(tx), "precondition: consensus does not")

	return tx
}

// TestBlockValid_InvalidWhenCoinbaseIsOnlyLooselyACoinbase pins Block.Valid step 4a to
// the consensus predicate.
//
// Valid is where the verdict is actually decided on the two paths that matter. Above
// the checkpoint it is the only check that runs, and the miner chooses the coinbase.
// On catch-up, a quick-route rejection that is neither corrupt nor incomplete sends
// the block back through full validation, so Valid gets the final say there too. A
// looser test here therefore overrules the strict one on the quick route.
func TestBlockValid_InvalidWhenCoinbaseIsOnlyLooselyACoinbase(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	looseCoinbase := newLooseCoinbaseTx(t)

	nBits, err := NewNBitFromString("2000ffff")
	require.NoError(t, err)

	// Merkle root IS the coinbase txid, so the coinbase-only binding succeeds and the
	// body is bound: whatever rejects this block is deciding on the miner's own body.
	blockHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: looseCoinbase.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	met := false

	for nonce := uint32(0); nonce < 1_000_000; nonce++ {
		blockHeader.Nonce = nonce
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			met = true
			break
		}
	}

	require.True(t, met, "test fixture must find a header meeting the easy target")

	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       looseCoinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{},
		Height:           0,
	}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, blobmemory.New(), createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err, "a shape svnode rejects must not be accepted here — that is a chain split")
	require.Contains(t, err.Error(), "not a valid coinbase")
}
