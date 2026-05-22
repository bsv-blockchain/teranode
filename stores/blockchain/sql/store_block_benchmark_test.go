package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

// makeBenchBlock builds a minimal block at the given height with a unique hash.
func makeBenchBlock(b *testing.B, height uint32, prevHash *chainhash.Hash, timestamp uint32) *model.Block {
	b.Helper()

	cb := bt.NewTx()
	input := &bt.Input{
		PreviousTxOutIndex: 0xFFFFFFFF,
		SequenceNumber:     0xFFFFFFFF,
	}
	if err := input.PreviousTxIDAdd(&chainhash.Hash{}); err != nil {
		b.Fatal(err)
	}
	script := []byte{3, byte(height), byte(height >> 8), byte(height >> 16)}
	input.UnlockingScript = bscript.NewFromBytes(script)
	cb.Inputs = append(cb.Inputs, input)

	outScript := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0x88, 0xac})
	cb.AddOutput(&bt.Output{Satoshis: 5000000000, LockingScript: outScript})

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        2,
			Timestamp:      timestamp,
			Nonce:          height,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: cb.TxIDChainHash(),
			Bits:           *bits,
		},
		Height:           height,
		CoinbaseTx:       cb,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
}

// BenchmarkStoreBlock_Sequential measures the throughput of sequential block
// storage. Two sub-benchmarks isolate the cost below and above CSVHeight so
// that MTP-calculation overhead is immediately visible.
//
// The "AboveCSVHeight" sub-benchmark is the one that regresses without the MTP
// cache — before the cache it would fire a SQL query per block, roughly 45x
// slower than the "BelowCSVHeight" baseline.
func BenchmarkStoreBlock_Sequential(b *testing.B) {
	b.Run("BelowCSVHeight", func(b *testing.B) {
		benchStoreBlocks(b, 576) // regtest CSVHeight=576; blocks 1..N all below
	})
	b.Run("AboveCSVHeight", func(b *testing.B) {
		benchStoreBlocks(b, 0) // CSVHeight=0 so MTP is active from block 11
	})
}

func benchStoreBlocks(b *testing.B, csvHeight uint32) {
	b.Helper()

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.ChainCfgParams.CSVHeight = csvHeight

	storeURL, err := url.Parse("sqlitememory:///")
	if err != nil {
		b.Fatal(err)
	}

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	genesisHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		b.Fatal(err)
	}
	prevHash := genesisHeader.Hash()

	// Pre-store 11 blocks so MTP has enough history for the above-CSV case.
	const warmup = 11
	for h := uint32(1); h <= warmup; h++ {
		block := makeBenchBlock(b, h, prevHash, 1600000000+h)
		if _, _, err := s.StoreBlock(ctx, block, "bench"); err != nil {
			b.Fatal(err)
		}
		prevHash = block.Hash()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		h := uint32(warmup + 1 + i)
		block := makeBenchBlock(b, h, prevHash, 1600000000+h)
		if _, _, err := s.StoreBlock(ctx, block, "bench"); err != nil {
			b.Fatal(err)
		}
		prevHash = block.Hash()
	}
}
