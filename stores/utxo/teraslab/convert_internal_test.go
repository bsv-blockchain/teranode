package teraslab

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	teraslab "github.com/icellan/teraslab/client/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// internalTestTxHex is the extended-format tx reused across pure conversion
// tests (1 input carrying prev satoshis/script, 6 p2pkh outputs, fee 215).
const internalTestTxHex = "010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000"

const internalCoinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000"

func mustTx(t *testing.T, hexStr string) *bt.Tx {
	t.Helper()
	tx, err := bt.NewTxFromString(hexStr)
	require.NoError(t, err)
	return tx
}

func TestHashConversions(t *testing.T) {
	t.Run("hashToTxID nil yields zero value", func(t *testing.T) {
		assert.Equal(t, teraslab.TxID{}, hashToTxID(nil))
	})
	t.Run("hashToUtxoHash nil yields zero value", func(t *testing.T) {
		assert.Equal(t, teraslab.UtxoHash{}, hashToUtxoHash(nil))
	})
	t.Run("hashToTxID copies bytes", func(t *testing.T) {
		h := &chainhash.Hash{}
		h[0], h[31] = 0xAB, 0xCD
		got := hashToTxID(h)
		assert.Equal(t, byte(0xAB), got[0])
		assert.Equal(t, byte(0xCD), got[31])
	})
	t.Run("hashToUtxoHash copies bytes", func(t *testing.T) {
		h := &chainhash.Hash{}
		h[5] = 0x77
		got := hashToUtxoHash(h)
		assert.Equal(t, byte(0x77), got[5])
	})
}

func TestSpendingDataWireConversions(t *testing.T) {
	t.Run("spendingDataToWire nil yields zero wire", func(t *testing.T) {
		assert.Equal(t, teraslab.SpendingData{}, spendingDataToWire(nil))
	})
	t.Run("spendingDataToWire nil TxID yields zero wire", func(t *testing.T) {
		assert.Equal(t, teraslab.SpendingData{}, spendingDataToWire(&spend.SpendingData{}))
	})
	t.Run("wireToSpendingData zero yields nil", func(t *testing.T) {
		assert.Nil(t, wireToSpendingData(teraslab.SpendingData{}))
	})
	t.Run("round trip preserves txid and vin", func(t *testing.T) {
		txid := &chainhash.Hash{}
		txid[0], txid[1] = 0x11, 0x22
		sd := spend.NewSpendingData(txid, 7)

		wire := spendingDataToWire(sd)
		assert.Equal(t, uint32(7), binary.LittleEndian.Uint32(wire[32:36]))

		back := wireToSpendingData(wire)
		require.NotNil(t, back)
		assert.Equal(t, txid.String(), back.TxID.String())
		assert.Equal(t, 7, back.Vin)
	})
}

func TestBuildFieldMask(t *testing.T) {
	t.Run("empty request uses default Get mask", func(t *testing.T) {
		assert.Equal(t, defaultGetMask, buildFieldMask(nil))
	})
	t.Run("known field maps to its bits", func(t *testing.T) {
		assert.Equal(t, teraslab.FieldFee, buildFieldMask([]fields.FieldName{fields.Fee}))
	})
	t.Run("multiple known fields OR their bits", func(t *testing.T) {
		got := buildFieldMask([]fields.FieldName{fields.Fee, fields.SizeInBytes})
		assert.Equal(t, teraslab.FieldFee|teraslab.FieldSizeInBytes, got)
	})
	t.Run("field that maps to zero falls back to FieldAll", func(t *testing.T) {
		// fields.TxID maps to 0 (always available), so a mask of only TxID is 0
		// and must fall back to FieldAll rather than fetching nothing.
		assert.Equal(t, teraslab.FieldAll, buildFieldMask([]fields.FieldName{fields.TxID}))
	})
	t.Run("unknown field is ignored, falling back to FieldAll", func(t *testing.T) {
		// A FieldName not present in the lookup table contributes no bits.
		assert.Equal(t, teraslab.FieldAll, buildFieldMask([]fields.FieldName{fields.FieldName("not-a-real-field")}))
	})
}

func TestSerializeDeserializeInputs(t *testing.T) {
	tx := mustTx(t, internalTestTxHex)

	t.Run("round trip preserves inputs", func(t *testing.T) {
		blob := serializeInputs(tx.Inputs)
		got, err := deserializeInputs(blob)
		require.NoError(t, err)
		require.Len(t, got, len(tx.Inputs))
		assert.Equal(t, tx.Inputs[0].PreviousTxOutIndex, got[0].PreviousTxOutIndex)
		assert.Equal(t, tx.Inputs[0].PreviousTxSatoshis, got[0].PreviousTxSatoshis)
	})

	t.Run("zero-length data yields nil (no stored inputs)", func(t *testing.T) {
		got, err := deserializeInputs(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("truncated count header is an error", func(t *testing.T) {
		// A non-empty blob too short to even hold the 4-byte count is corrupt and
		// must surface an error, not be silently treated as empty.
		_, err := deserializeInputs([]byte{0x01, 0x02})
		require.Error(t, err)
	})

	t.Run("declared count exceeding buffer is an error", func(t *testing.T) {
		// count=1 but the entry length/body is missing — a truncated blob must
		// error rather than silently returning fewer inputs than declared.
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, 1)
		_, err := deserializeInputs(buf)
		require.Error(t, err)
	})

	t.Run("entry length larger than buffer is an error", func(t *testing.T) {
		// count=1, declared entry length 9999 but body absent.
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint32(buf[0:4], 1)
		binary.LittleEndian.PutUint32(buf[4:8], 9999)
		_, err := deserializeInputs(buf)
		require.Error(t, err)
	})

	t.Run("malformed entry body surfaces a parse error", func(t *testing.T) {
		// count=1, entry length 2, body 0x00 0x00 — too short for an extended input.
		buf := make([]byte, 10)
		binary.LittleEndian.PutUint32(buf[0:4], 1)
		binary.LittleEndian.PutUint32(buf[4:8], 2)
		_, err := deserializeInputs(buf)
		require.Error(t, err)
	})
}

func TestSerializeDeserializeOutputs(t *testing.T) {
	tx := mustTx(t, internalTestTxHex)

	t.Run("round trip preserves outputs", func(t *testing.T) {
		blob := serializeOutputs(tx.Outputs)
		got, err := deserializeOutputs(blob)
		require.NoError(t, err)
		require.Len(t, got, len(tx.Outputs))
		assert.Equal(t, tx.Outputs[0].Satoshis, got[0].Satoshis)
	})

	t.Run("nil output round-trips as nil", func(t *testing.T) {
		outs := []*bt.Output{tx.Outputs[0], nil, tx.Outputs[1]}
		blob := serializeOutputs(outs)
		got, err := deserializeOutputs(blob)
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.NotNil(t, got[0])
		assert.Nil(t, got[1])
		assert.NotNil(t, got[2])
	})

	t.Run("zero-length data yields nil (no stored outputs)", func(t *testing.T) {
		got, err := deserializeOutputs(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("truncated count header is an error", func(t *testing.T) {
		_, err := deserializeOutputs([]byte{0x00})
		require.Error(t, err)
	})

	t.Run("entry length larger than buffer is an error", func(t *testing.T) {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint32(buf[0:4], 1)
		binary.LittleEndian.PutUint32(buf[4:8], 9999)
		_, err := deserializeOutputs(buf)
		require.Error(t, err)
	})

	t.Run("malformed entry body surfaces a parse error", func(t *testing.T) {
		// count=1, entry length 1, body 0xFF — not a valid output.
		buf := make([]byte, 9)
		binary.LittleEndian.PutUint32(buf[0:4], 1)
		binary.LittleEndian.PutUint32(buf[4:8], 1)
		buf[8] = 0xFF
		_, err := deserializeOutputs(buf)
		require.Error(t, err)
	})
}

func TestTxToCreateItem(t *testing.T) {
	t.Run("standard tx computes fee and utxo hashes", func(t *testing.T) {
		tx := mustTx(t, internalTestTxHex)
		item, err := txToCreateItem(tx, 10, 100, utxo.CreateOptions{})
		require.NoError(t, err)
		assert.Equal(t, uint64(215), item.Fee)
		assert.False(t, item.IsCoinbase)
		assert.Equal(t, uint32(0), item.SpendingHeight, "non-coinbase has no spending height")
		assert.Len(t, item.UtxoHashes, len(tx.Outputs))
		assert.Equal(t, hashToTxID(tx.TxIDChainHash()), item.TxID)
		assert.NotEmpty(t, item.TxData.Inputs)
		assert.NotEmpty(t, item.TxData.Outputs)
	})

	t.Run("coinbase zeroes fee and sets maturity spending height", func(t *testing.T) {
		tx := mustTx(t, internalCoinbaseHex)
		item, err := txToCreateItem(tx, 42, 100, utxo.CreateOptions{})
		require.NoError(t, err)
		assert.True(t, item.IsCoinbase)
		assert.Equal(t, uint64(0), item.Fee)
		// Coinbase outputs mature at blockHeight + CoinbaseMaturity, matching the
		// Aerospike backend (create.go: blockHeight + CoinbaseMaturity). Storing
		// just blockHeight would make the coinbase spendable at its creation height.
		assert.Equal(t, uint32(142), item.SpendingHeight)
	})

	t.Run("conflicting populates deduped parent txids", func(t *testing.T) {
		tx := mustTx(t, internalTestTxHex)
		item, err := txToCreateItem(tx, 0, 100, utxo.CreateOptions{Conflicting: true})
		require.NoError(t, err)
		require.Len(t, item.ParentTxIDs, 1)
		assert.Equal(t, hashToTxID(tx.Inputs[0].PreviousTxIDChainHash()), item.ParentTxIDs[0])
		assert.Equal(t, uint8(0x02), item.Flags) // conflicting bit
	})

	t.Run("flags reflect locked and frozen options", func(t *testing.T) {
		tx := mustTx(t, internalTestTxHex)
		item, err := txToCreateItem(tx, 0, 100, utxo.CreateOptions{Locked: true, Frozen: true})
		require.NoError(t, err)
		assert.Equal(t, uint8(0x01|0x04), item.Flags)
	})

	t.Run("OP_RETURN output gets zero utxo hash", func(t *testing.T) {
		dataScript := &bscript.Script{}
		require.NoError(t, dataScript.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
		require.NoError(t, dataScript.AppendPushData([]byte("teraslab")))

		tx := bt.NewTx()
		tx.Outputs = []*bt.Output{
			{Satoshis: 1000, LockingScript: tx0LockingScript(t)},
			{Satoshis: 0, LockingScript: dataScript},
		}
		item, err := txToCreateItem(tx, 0, 100, utxo.CreateOptions{})
		require.NoError(t, err)
		require.Len(t, item.UtxoHashes, 2)
		assert.NotEqual(t, teraslab.UtxoHash{}, item.UtxoHashes[0])
		assert.Equal(t, teraslab.UtxoHash{}, item.UtxoHashes[1], "data output must hash to zero")
	})

	t.Run("explicit TxID and IsCoinbase overrides are honored", func(t *testing.T) {
		tx := mustTx(t, internalTestTxHex)
		override := &chainhash.Hash{}
		override[0] = 0x99
		cb := true
		item, err := txToCreateItem(tx, 7, 100, utxo.CreateOptions{TxID: override, IsCoinbase: &cb})
		require.NoError(t, err)
		assert.Equal(t, hashToTxID(override), item.TxID)
		assert.True(t, item.IsCoinbase)
		assert.Equal(t, uint32(107), item.SpendingHeight, "coinbase spending height = blockHeight + maturity")
	})

	t.Run("mined block info populates pointers", func(t *testing.T) {
		tx := mustTx(t, internalTestTxHex)
		item, err := txToCreateItem(tx, 0, 100, utxo.CreateOptions{
			MinedBlockInfos: []utxo.MinedBlockInfo{{BlockID: 5, BlockHeight: 6, SubtreeIdx: 7}},
		})
		require.NoError(t, err)
		require.NotNil(t, item.MinedBlockID)
		assert.Equal(t, uint32(5), *item.MinedBlockID)
		assert.Equal(t, uint32(6), *item.MinedBlockHeight)
		assert.Equal(t, uint32(7), *item.MinedSubtreeIdx)
	})
}

// tx0LockingScript returns a real p2pkh locking script (output 0 of the test tx).
func tx0LockingScript(t *testing.T) *bscript.Script {
	t.Helper()
	return mustTx(t, internalTestTxHex).Outputs[0].LockingScript
}

func TestRecordToMetaData(t *testing.T) {
	t.Run("not found yields nil", func(t *testing.T) {
		data, err := recordToMetaData(teraslab.TxRecord{Found: false})
		require.NoError(t, err)
		assert.Nil(t, data)
	})

	t.Run("found-but-empty yields default tx", func(t *testing.T) {
		data, err := recordToMetaData(teraslab.TxRecord{Found: true})
		require.NoError(t, err)
		require.NotNil(t, data)
		require.NotNil(t, data.Tx, "Tx must never be nil for a found record")
	})

	t.Run("metadata flags decode coinbase/conflicting/locked", func(t *testing.T) {
		data, err := recordToMetaData(teraslab.TxRecord{
			Found: true,
			Metadata: &teraslab.TxMetadata{
				Fee:          99,
				SizeInBytes:  123,
				Locktime:     5,
				UnminedSince: 1000,
				Flags:        0x01 | 0x02 | 0x04,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(99), data.Fee)
		assert.Equal(t, uint64(123), data.SizeInBytes)
		assert.Equal(t, uint32(5), data.LockTime)
		assert.Equal(t, uint32(1000), data.UnminedSince)
		assert.True(t, data.IsCoinbase)
		assert.True(t, data.Conflicting)
		assert.True(t, data.Locked)
	})

	t.Run("slots decode spent spending data and frozen flag", func(t *testing.T) {
		spendTxid := &chainhash.Hash{}
		spendTxid[0] = 0x42
		var spent teraslab.SpendingData
		copy(spent[:32], spendTxid[:])
		binary.LittleEndian.PutUint32(spent[32:36], 3)

		data, err := recordToMetaData(teraslab.TxRecord{
			Found: true,
			Slots: []teraslab.UtxoSlot{
				{Status: teraslab.SlotSpent, SpendingData: spent},
				{Status: teraslab.SlotFrozen},
				{Status: teraslab.SlotUnspent},
			},
		})
		require.NoError(t, err)
		require.Len(t, data.SpendingDatas, 3)
		require.NotNil(t, data.SpendingDatas[0])
		assert.Equal(t, spendTxid.String(), data.SpendingDatas[0].TxID.String())
		// A frozen slot surfaces the FrozenBytesTxHash sentinel as its spending
		// data so the conflict-resolution helpers detect frozen UTXOs.
		require.NotNil(t, data.SpendingDatas[1])
		assert.Equal(t, subtree.FrozenBytesTxHash, *data.SpendingDatas[1].TxID)
		assert.True(t, data.Frozen)
		assert.Nil(t, data.SpendingDatas[2])
	})

	t.Run("cold data reconstructs tx inputs/outputs/inpoints", func(t *testing.T) {
		src := mustTx(t, internalTestTxHex)
		inpoints, err := subtree.NewTxInpointsFromTx(src)
		require.NoError(t, err)
		inpointsBlob, err := inpoints.Serialize()
		require.NoError(t, err)

		rec := teraslab.TxRecord{
			Found:    true,
			Metadata: &teraslab.TxMetadata{TxVersion: src.Version, Locktime: src.LockTime},
			TxData: &teraslab.TxData{
				Inputs:   serializeInputs(src.Inputs),
				Outputs:  serializeOutputs(src.Outputs),
				Inpoints: inpointsBlob,
			},
		}
		data, err := recordToMetaData(rec)
		require.NoError(t, err)
		require.NotNil(t, data.Tx)
		assert.Equal(t, src.Version, data.Tx.Version)
		assert.Len(t, data.Tx.Inputs, len(src.Inputs))
		assert.Len(t, data.Tx.Outputs, len(src.Outputs))
	})

	t.Run("corrupt cold data propagates an error instead of a partial tx", func(t *testing.T) {
		// A stored inputs blob that declares one entry but is truncated must abort
		// recordToMetaData with an error — a store read error must never be
		// swallowed and surfaced as a silently-incomplete tx.
		corrupt := make([]byte, 8)
		binary.LittleEndian.PutUint32(corrupt[0:4], 1)    // declares 1 input
		binary.LittleEndian.PutUint32(corrupt[4:8], 9999) // body absent
		_, err := recordToMetaData(teraslab.TxRecord{
			Found:  true,
			TxData: &teraslab.TxData{Inputs: corrupt},
		})
		require.Error(t, err)
	})

	t.Run("corrupt stored outputs propagate an error", func(t *testing.T) {
		corrupt := make([]byte, 9)
		binary.LittleEndian.PutUint32(corrupt[0:4], 1)
		binary.LittleEndian.PutUint32(corrupt[4:8], 1)
		corrupt[8] = 0xFF // not a valid output body
		_, err := recordToMetaData(teraslab.TxRecord{
			Found:  true,
			TxData: &teraslab.TxData{Outputs: corrupt},
		})
		require.Error(t, err)
	})

	t.Run("truncated block entries surface an error, not a partial set", func(t *testing.T) {
		// TeraSlab returns at most MaxInlineBlockEntries (3) block entries inline and
		// flags the rest as truncated. Silently returning a capped BlockIDs set would
		// corrupt reorg/rewind decisions (Aerospike stores the full set), so a
		// truncated record must error rather than hand back an incomplete list.
		_, err := recordToMetaData(teraslab.TxRecord{
			Found: true,
			BlockEntries: []teraslab.BlockEntry{
				{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0},
				{BlockID: 2, BlockHeight: 2, SubtreeIdx: 0},
				{BlockID: 3, BlockHeight: 3, SubtreeIdx: 0},
			},
			BlockEntriesTruncated: true,
		})
		require.Error(t, err)
	})

	t.Run("block entries and conflicting children decode", func(t *testing.T) {
		child := teraslab.TxID{}
		child[0] = 0xCC
		data, err := recordToMetaData(teraslab.TxRecord{
			Found: true,
			BlockEntries: []teraslab.BlockEntry{
				{BlockID: 1, BlockHeight: 2, SubtreeIdx: 3},
			},
			ConflictingChildren: []teraslab.TxID{child},
		})
		require.NoError(t, err)
		require.Len(t, data.BlockIDs, 1)
		assert.Equal(t, uint32(1), data.BlockIDs[0])
		assert.Equal(t, uint32(2), data.BlockHeights[0])
		assert.Equal(t, 3, data.SubtreeIdxs[0])
		require.Len(t, data.ConflictingChildren, 1)
		assert.Equal(t, chainhash.Hash(child), data.ConflictingChildren[0])
	})
}
