package chainoracle

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fixture is a three-block chain written through the production stores and
// writers, so the oracle is checked against the same bytes a node would hold.
//
//	block 1: coinbase cb1, seed (no inputs, outputs 0 and 1)
//	block 2: coinbase cb2, tx2 (spends seed:0, outputs 0 and 1)
//	block 3: coinbase cb3, tx3a (spends tx2:0), tx3b (spends seed:1)
//	mempool: mem (spends tx2:1), unmined
//
// seed is fully spent on the chain (prunable); tx2:1 is unspent on the chain
// but spent in the store by an unmined tx (a legitimate mempool spend).
type fixture struct {
	t        *testing.T
	ctx      context.Context
	settings *settings.Settings
	chain    blockchainstore.Store
	utxo     utxo.Store
	subtrees blob.Store
	blocks   blob.Store

	blockList []*fixtureBlock

	seed, tx2, tx3a, tx3b, mem *bt.Tx
}

type fixtureBlock struct {
	block   *model.Block
	height  uint32
	id      uint32
	txs     []*bt.Tx // coinbase first
	subtree *subtreepkg.Subtree
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	chainURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	chain, err := blockchainstore.NewStore(ulogger.TestLogger{}, chainURL, tSettings)
	require.NoError(t, err)

	utxoURL, err := url.Parse("sqlitememory:///chainoracle")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, utxoURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = utxoStore.Close(context.Background()) })

	f := &fixture{
		t:        t,
		ctx:      ctx,
		settings: tSettings,
		chain:    chain,
		utxo:     utxoStore,
		subtrees: memory.New(),
		blocks:   memory.New(),
	}

	f.seed = newTx(nil, 1000, 2000)
	f.tx2 = newTx([]spendOf{{f.seed, 0}}, 600, 300)
	f.tx3a = newTx([]spendOf{{f.tx2, 0}}, 500)
	f.tx3b = newTx([]spendOf{{f.seed, 1}}, 1900)
	f.mem = newTx([]spendOf{{f.tx2, 1}}, 250)

	f.addBlock(f.seed)
	f.addBlock(f.tx2)
	f.addBlock(f.tx3a, f.tx3b)

	_, _, err = f.utxo.SpendAndCreate(ctx, f.mem, 3)
	require.NoError(t, err)

	return f
}

type spendOf struct {
	parent *bt.Tx
	vout   uint32
}

// newTx builds a tx spending the given outputs, with one P2PKH output per
// value. Txs are distinguished by their inputs and values; no signatures are
// needed because the stores do not run scripts.
func newTx(spends []spendOf, values ...uint64) *bt.Tx {
	tx := bt.NewTx()

	for _, s := range spends {
		out := s.parent.Outputs[s.vout]
		in := &bt.Input{
			PreviousTxOutIndex: s.vout,
			PreviousTxSatoshis: out.Satoshis,
			PreviousTxScript:   out.LockingScript,
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
		}
		_ = in.PreviousTxIDAdd(s.parent.TxIDChainHash())
		tx.Inputs = append(tx.Inputs, in)
	}

	for i, v := range values {
		tx.AddOutput(&bt.Output{Satoshis: v, LockingScript: p2pkh(byte(len(values)*16 + i))})
	}

	return tx
}

func p2pkh(seed byte) *bscript.Script {
	b := make([]byte, 25)
	b[0], b[1], b[2] = bscript.OpDUP, bscript.OpHASH160, 0x14

	for i := 3; i < 23; i++ {
		b[i] = seed
	}

	b[23], b[24] = bscript.OpEQUALVERIFY, bscript.OpCHECKSIG
	s := bscript.Script(b)

	return &s
}

// newCoinbase builds a coinbase with a spendable output and an OP_FALSE
// OP_RETURN output, which the UTXO store and the additions file both omit.
func newCoinbase(height uint32) *bt.Tx {
	tx := bt.NewTx()
	unlocking := bscript.Script([]byte{0x03, byte(height), byte(height >> 8), byte(height >> 16)})
	in := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff, UnlockingScript: &unlocking}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{})
	tx.Inputs = append(tx.Inputs, in)

	tx.AddOutput(&bt.Output{Satoshis: 5000000000, LockingScript: p2pkh(byte(height))})

	opReturn := bscript.Script([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, byte(height)})
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &opReturn})

	return tx
}

// addBlock mines the txs into a new block on top of the fixture's tip and writes
// it everywhere a node would: blockchain store, subtree store (subtree and
// subtreeData), block store (block, utxo-additions, utxo-deletions) and the
// UTXO store.
func (f *fixture) addBlock(txs ...*bt.Tx) {
	t := f.t
	t.Helper()

	tip, tipMeta, err := f.chain.GetBestBlockHeader(f.ctx)
	require.NoError(t, err)

	height := tipMeta.Height + 1
	coinbase := newCoinbase(height)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size()))) //nolint:gosec // tiny test tx
	}

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  tip.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()) + height, //nolint:gosec // test clock
			Bits:           *bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(len(txs) + 1),
		SizeInBytes:      80,
		Subtrees:         []*chainhash.Hash{st.RootHash()},
	}

	id, storedHeight, err := f.chain.StoreBlock(f.ctx, block, "")
	require.NoError(t, err)
	require.Equal(t, height, storedHeight)

	fb := &fixtureBlock{block: block, height: height, id: uint32(id), txs: append([]*bt.Tx{coinbase}, txs...), subtree: st} //nolint:gosec // sqlite ids are small
	f.blockList = append(f.blockList, fb)

	f.writeSubtreeFiles(fb)
	f.writeBlockFile(fb, fb.block)
	f.writeDeltas(fb, fb.txs...)

	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: fb.id, BlockHeight: height, OnLongestChain: true})

	_, _, err = f.utxo.SpendAndCreate(f.ctx, coinbase, height, utxo.WithCreateOnly(), mined)
	require.NoError(t, err)

	for _, tx := range txs {
		if len(tx.Inputs) == 0 {
			_, _, err = f.utxo.SpendAndCreate(f.ctx, tx, height, utxo.WithCreateOnly(), mined)
		} else {
			_, _, err = f.utxo.SpendAndCreate(f.ctx, tx, height, mined)
		}

		require.NoError(t, err)
	}
}

func (f *fixture) writeSubtreeFiles(fb *fixtureBlock) {
	b, err := fb.subtree.Serialize()
	require.NoError(f.t, err)

	key := fb.block.Subtrees[0][:]
	require.NoError(f.t, f.subtrees.Set(f.ctx, key, fileformat.FileTypeSubtree, b))

	var data bytes.Buffer
	for _, tx := range fb.txs {
		data.Write(tx.Bytes())
	}

	require.NoError(f.t, f.subtrees.Set(f.ctx, key, fileformat.FileTypeSubtreeData, data.Bytes()))
}

func (f *fixture) writeBlockFile(fb *fixtureBlock, content *model.Block) {
	b, err := content.Bytes()
	require.NoError(f.t, err)
	require.NoError(f.t, f.blocks.Set(f.ctx, fb.block.Hash()[:], fileformat.FileTypeBlock, b))
}

func (f *fixture) writeDeltas(fb *fixtureBlock, txs ...*bt.Tx) {
	us, err := utxopersister.NewUTXOSet(f.ctx, ulogger.TestLogger{}, f.settings, f.blocks, fb.block.Hash(), fb.height)
	require.NoError(f.t, err)

	for _, tx := range txs {
		require.NoError(f.t, us.ProcessTx(tx))
	}

	require.NoError(f.t, us.Close())
}

func (f *fixture) stores() Stores {
	return Stores{
		Chain:        StoreChain(f.chain),
		UTXO:         f.utxo,
		SubtreeStore: f.subtrees,
		BlockStore:   f.blocks,
		Settings:     f.settings,
	}
}

func (f *fixture) check() *Report {
	f.t.Helper()

	report, err := Check(f.ctx, f.stores(), Options{})
	require.NoError(f.t, err)

	f.t.Log(report.String())

	return report
}

// spend describes the spend of parent:vout by input vin of spender, as Unspend needs it.
func (f *fixture) spend(parent *bt.Tx, vout uint32, spender *bt.Tx, vin int) *utxo.Spend {
	hash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
	require.NoError(f.t, err)

	return &utxo.Spend{
		TxID:         parent.TxIDChainHash(),
		Vout:         vout,
		UTXOHash:     hash,
		SpendingData: spend.NewSpendingData(spender.TxIDChainHash(), vin),
	}
}

// requireOnly asserts the report holds exactly the given discrepancy kinds (as a
// multiset) and returns the first discrepancy.
func requireOnly(t *testing.T, report *Report, kinds ...Kind) Discrepancy {
	t.Helper()

	got := make([]Kind, 0, len(report.Discrepancies))
	for _, d := range report.Discrepancies {
		got = append(got, d.Kind)
	}

	require.ElementsMatch(t, kinds, got, report.String())
	require.NotEmpty(t, report.Discrepancies)

	return report.Discrepancies[0]
}

func TestCheck_ConsistentStores(t *testing.T) {
	f := newFixture(t)

	report := f.check()

	require.True(t, report.OK(), report.String())
	require.Equal(t, uint32(3), report.TipHeight)
	require.Equal(t, *f.blockList[2].block.Hash(), report.Tip)
	require.Equal(t, 3, report.Blocks)
	require.Equal(t, 7, report.Txs)
	// 3 coinbases x 1 storable output + seed 2 + tx2 2 + tx3a 1 + tx3b 1
	require.Equal(t, 9, report.Outputs)
	require.Equal(t, 1, report.MempoolSpends)
	require.Zero(t, report.PrunedTxs)
	require.Zero(t, report.ExternalInputs)
	require.True(t, report.BlockFilesChecked)
	require.True(t, report.UTXODeltasChecked)
	require.True(t, report.ReplayComplete)
}

func TestCheck_WithoutBlockStoreSkipsPersistedFiles(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.blocks.Del(f.ctx, f.blockList[1].block.Hash()[:], fileformat.FileTypeBlock))
	require.NoError(t, f.subtrees.Del(f.ctx, f.blockList[1].block.Subtrees[0][:], fileformat.FileTypeSubtreeData))

	stores := f.stores()
	stores.BlockStore = nil

	report, err := Check(f.ctx, stores, Options{})
	require.NoError(t, err)
	require.True(t, report.OK(), report.String())
	require.False(t, report.BlockFilesChecked)
	require.False(t, report.UTXODeltasChecked)
}

func TestCheck_FromHeightCountsExternalInputs(t *testing.T) {
	f := newFixture(t)

	report, err := Check(f.ctx, f.stores(), Options{FromHeight: 2})
	require.NoError(t, err)
	require.True(t, report.OK(), report.String())
	require.Equal(t, 2, report.Blocks)
	// tx2 spends seed:0 and tx3b spends seed:1, both created at height 1
	require.Equal(t, 2, report.ExternalInputs)
}

// The UTXO-store cases each break one record the way a crash or a half-applied
// write could, and assert the oracle names exactly that record.

func TestCheck_UTXOTxMissing(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.utxo.Delete(f.ctx, f.tx3a.TxIDChainHash()))

	d := requireOnly(t, f.check(), KindUTXOTxMissing)
	require.Equal(t, *f.tx3a.TxIDChainHash(), d.TxID)
	require.Equal(t, uint32(3), d.Height)
}

func TestCheck_FullySpentTxMayBePruned(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.utxo.Delete(f.ctx, f.seed.TxIDChainHash()))

	report := f.check()
	require.True(t, report.OK(), report.String())
	require.Equal(t, 1, report.PrunedTxs)
}

func TestCheck_UTXOUnspentShouldBeSpent(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.utxo.Unspend(f.ctx, []*utxo.Spend{f.spend(f.tx2, 0, f.tx3a, 0)}))

	d := requireOnly(t, f.check(), KindUTXOUnspentShouldBeSpent)
	require.Equal(t, *f.tx2.TxIDChainHash(), d.TxID)
	require.Equal(t, 0, d.Vout)
}

func TestCheck_UTXOWrongSpender(t *testing.T) {
	f := newFixture(t)

	// put tx2:0 back and let an off-chain tx take it: the chain says tx3a spent it
	require.NoError(t, f.utxo.Unspend(f.ctx, []*utxo.Spend{f.spend(f.tx2, 0, f.tx3a, 0)}))

	rival := newTx([]spendOf{{f.tx2, 0}}, 1)
	_, _, err := f.utxo.SpendAndCreate(f.ctx, rival, 3)
	require.NoError(t, err)

	d := requireOnly(t, f.check(), KindUTXOWrongSpender)
	require.Equal(t, *f.tx2.TxIDChainHash(), d.TxID)
	require.Contains(t, d.Detail, rival.TxIDChainHash().String())
}

func TestCheck_UTXOTxNotMined(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.utxo.MarkTransactionsOnLongestChain(f.ctx, []chainhash.Hash{*f.tx3b.TxIDChainHash()}, false))

	d := requireOnly(t, f.check(), KindUTXOTxNotMined)
	require.Equal(t, *f.tx3b.TxIDChainHash(), d.TxID)
}

func TestCheck_UTXOSpentByMinedOffChainTx(t *testing.T) {
	f := newFixture(t)

	// the mempool spender of tx2:1 claims to be mined, in a block not on the chain
	_, err := f.utxo.SetMinedMulti(f.ctx, []*chainhash.Hash{f.mem.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 999, BlockHeight: 3, OnLongestChain: true})
	require.NoError(t, err)

	d := requireOnly(t, f.check(), KindUTXOSpentShouldBeUnspent)
	require.Equal(t, *f.tx2.TxIDChainHash(), d.TxID)
	require.Equal(t, 1, d.Vout)
}

func TestCheck_UTXOSpentByMissingTx(t *testing.T) {
	f := newFixture(t)

	// the spender's record is gone but its spend remains: a parent naming a
	// record-less spender
	require.NoError(t, f.utxo.Delete(f.ctx, f.mem.TxIDChainHash()))

	d := requireOnly(t, f.check(), KindUTXOSpentShouldBeUnspent)
	require.Contains(t, d.Detail, "no UTXO record")
}

// The blob-store cases each remove or replace one persisted file.

func TestCheck_BlockFileMissing(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.blocks.Del(f.ctx, f.blockList[1].block.Hash()[:], fileformat.FileTypeBlock))

	d := requireOnly(t, f.check(), KindBlobMissing)
	require.Equal(t, fileformat.FileTypeBlock, d.FileType)
	require.Equal(t, uint32(2), d.Height)
}

func TestCheck_BlockFileWrongContent(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.blocks.Del(f.ctx, f.blockList[1].block.Hash()[:], fileformat.FileTypeBlock))
	f.writeBlockFile(f.blockList[1], f.blockList[2].block)

	d := requireOnly(t, f.check(), KindBlobMismatch)
	require.Equal(t, fileformat.FileTypeBlock, d.FileType)
	require.Contains(t, d.Detail, "hashes to")
}

func TestCheck_SubtreeDataMissingFallsBackToUTXOStore(t *testing.T) {
	f := newFixture(t)

	subtree := *f.blockList[2].block.Subtrees[0]
	require.NoError(t, f.subtrees.Del(f.ctx, subtree[:], fileformat.FileTypeSubtreeData))

	report := f.check()
	d := requireOnly(t, report, KindBlobMissing)
	require.Equal(t, fileformat.FileTypeSubtreeData, d.FileType)
	require.Equal(t, subtree, d.Subtree)
	// the txs came back from the UTXO store, so every coin was still checked
	require.True(t, report.ReplayComplete)
	require.Equal(t, 9, report.Outputs)
}

func TestCheck_SubtreeDataWrongContent(t *testing.T) {
	f := newFixture(t)

	fb := f.blockList[2]
	require.NoError(t, f.subtrees.Del(f.ctx, fb.block.Subtrees[0][:], fileformat.FileTypeSubtreeData))

	var data bytes.Buffer
	data.Write(fb.txs[0].Bytes())
	data.Write(fb.txs[2].Bytes()) // swapped order
	data.Write(fb.txs[1].Bytes())
	require.NoError(t, f.subtrees.Set(f.ctx, fb.block.Subtrees[0][:], fileformat.FileTypeSubtreeData, data.Bytes()))

	d := requireOnly(t, f.check(), KindBlobMismatch)
	require.Equal(t, fileformat.FileTypeSubtreeData, d.FileType)
}

func TestCheck_SubtreeMissingLeavesReplayIncomplete(t *testing.T) {
	f := newFixture(t)

	subtree := *f.blockList[2].block.Subtrees[0]
	require.NoError(t, f.subtrees.Del(f.ctx, subtree[:], fileformat.FileTypeSubtree))

	report := f.check()
	requireOnly(t, report, KindBlobMissing, KindReplayIncomplete)
	require.False(t, report.ReplayComplete)
}

func TestCheck_UTXOAdditionsWrongContent(t *testing.T) {
	f := newFixture(t)

	fb := f.blockList[2]
	require.NoError(t, f.blocks.Del(f.ctx, fb.block.Hash()[:], fileformat.FileTypeUtxoAdditions))
	require.NoError(t, f.blocks.Del(f.ctx, fb.block.Hash()[:], fileformat.FileTypeUtxoDeletions))
	f.writeDeltas(fb, fb.txs[0], fb.txs[1]) // tx3b's coin changes never written

	report := f.check()
	// one additions record and one deletions record are missing
	requireOnly(t, report, KindBlobMismatch, KindBlobMismatch)
	require.Equal(t, 1, countFileType(report, fileformat.FileTypeUtxoAdditions))
	require.Equal(t, 1, countFileType(report, fileformat.FileTypeUtxoDeletions))
}

func TestCheck_UTXODeletionsMissing(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.blocks.Del(f.ctx, f.blockList[1].block.Hash()[:], fileformat.FileTypeUtxoDeletions))

	d := requireOnly(t, f.check(), KindBlobMissing)
	require.Equal(t, fileformat.FileTypeUtxoDeletions, d.FileType)
}

func TestCheck_SkipUTXODeltas(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.blocks.Del(f.ctx, f.blockList[1].block.Hash()[:], fileformat.FileTypeUtxoDeletions))

	report, err := Check(f.ctx, f.stores(), Options{SkipUTXODeltas: true})
	require.NoError(t, err)
	require.True(t, report.OK(), report.String())
	require.False(t, report.UTXODeltasChecked)
}

func countFileType(report *Report, fileType fileformat.FileType) int {
	n := 0

	for _, d := range report.Discrepancies {
		if d.FileType == fileType {
			n++
		}
	}

	return n
}
