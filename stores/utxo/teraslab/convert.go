package teraslab

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// fieldNameToMask maps a Teranode FieldName to the TeraSlab field mask bits
// needed to satisfy it. Some Teranode fields map to multiple TeraSlab bits
// (e.g. Tx requires cold data + version + locktime to reconstruct).
var fieldNameToMask = map[fields.FieldName]uint32{
	fields.Tx:                  teraslab.FieldColdData | teraslab.FieldTxVersion | teraslab.FieldLocktime,
	fields.TxID:                0, // txid is always available (it's the key)
	fields.Inputs:              teraslab.FieldColdData | teraslab.FieldTxVersion | teraslab.FieldLocktime,
	fields.Outputs:             teraslab.FieldColdData | teraslab.FieldTxVersion | teraslab.FieldLocktime,
	fields.LockTime:            teraslab.FieldLocktime,
	fields.Version:             teraslab.FieldTxVersion,
	fields.Fee:                 teraslab.FieldFee,
	fields.SizeInBytes:         teraslab.FieldSizeInBytes,
	fields.ExtendedSize:        teraslab.FieldExtendedSize,
	fields.TxInpoints:          teraslab.FieldColdData,
	fields.IsCoinbase:          teraslab.FieldFlags,
	fields.Conflicting:         teraslab.FieldFlags,
	fields.ConflictingChildren: teraslab.FieldConflictingChildren,
	fields.Locked:              teraslab.FieldFlags,
	fields.SpendingHeight:      teraslab.FieldSpendingHeight,
	fields.Utxos:               teraslab.FieldUtxoSlots,
	fields.SpentUtxos:          teraslab.FieldSpentUtxos,
	fields.BlockIDs:            teraslab.FieldBlockEntries,
	fields.BlockHeights:        teraslab.FieldBlockEntries,
	fields.SubtreeIdxs:         teraslab.FieldBlockEntries,
	fields.DeleteAtHeight:      teraslab.FieldDeleteAtHeight,
	fields.CreatedAt:           teraslab.FieldCreatedAt,
	fields.UnminedSince:        teraslab.FieldUnminedSince,
	fields.PreserveUntil:       teraslab.FieldPreserveUntil,
}

// defaultGetMask is the field mask used when Get() is called with no specific
// fields. Matches utxo.MetaFieldsWithTx: metadata + transaction data.
var defaultGetMask = buildFieldMaskFrom(utxo.MetaFieldsWithTx)

// defaultGetMetaMask is the field mask used by GetMeta(). Matches
// utxo.MetaFields: metadata without transaction data.
var defaultGetMetaMask = buildFieldMaskFrom(utxo.MetaFields)

// buildFieldMask converts Teranode field names to a TeraSlab field mask.
// If no fields are specified, uses the default Get mask (MetaFieldsWithTx).
func buildFieldMask(requestedFields []fields.FieldName) uint32 {
	if len(requestedFields) == 0 {
		return defaultGetMask
	}
	return buildFieldMaskFrom(requestedFields)
}

// buildFieldMaskFrom converts a list of Teranode field names to a TeraSlab field mask.
func buildFieldMaskFrom(requestedFields []fields.FieldName) uint32 {
	var mask uint32
	for _, f := range requestedFields {
		if bits, ok := fieldNameToMask[f]; ok {
			mask |= bits
		}
	}
	if mask == 0 {
		return teraslab.FieldAll
	}
	return mask
}

// hashToTxID converts a chainhash.Hash to a teraslab.TxID.
func hashToTxID(h *chainhash.Hash) teraslab.TxID {
	var txid teraslab.TxID
	if h != nil {
		copy(txid[:], h[:])
	}
	return txid
}

// hashToUtxoHash converts a chainhash.Hash to a teraslab.UtxoHash.
func hashToUtxoHash(h *chainhash.Hash) teraslab.UtxoHash {
	var uh teraslab.UtxoHash
	if h != nil {
		copy(uh[:], h[:])
	}
	return uh
}

// wireToSpendingData converts wire format SpendingData to Teranode SpendingData.
func wireToSpendingData(wire teraslab.SpendingData) *spend.SpendingData {
	empty := true
	for _, b := range wire {
		if b != 0 {
			empty = false
			break
		}
	}
	if empty {
		return nil
	}

	txID, _ := chainhash.NewHash(wire[:32])
	vin := binary.LittleEndian.Uint32(wire[32:36])
	return spend.NewSpendingData(txID, int(vin))
}

// spendingDataToWire converts Teranode SpendingData to the wire format
// [txid:32][vin:4 LE] expected by the server. A nil/empty SpendingData maps to
// the zero value (which the server treats as "no expected spending data").
func spendingDataToWire(sd *spend.SpendingData) teraslab.SpendingData {
	var wire teraslab.SpendingData
	if sd == nil || sd.TxID == nil {
		return wire
	}

	copy(wire[:32], sd.TxID[:])
	binary.LittleEndian.PutUint32(wire[32:36], uint32(sd.Vin)) //nolint:gosec // input index fits u32

	return wire
}

// ---------------------------------------------------------------------------
// Serialize: bt.Tx → TxData for storage
// ---------------------------------------------------------------------------

// serializeInputs serializes transaction inputs into a length-prefixed byte blob.
//
// Format: [count:4 LE] then for each input [len:4 LE][extended-input-bytes]
//
// Extended input bytes = standard input bytes + PreviousTxSatoshis (8 LE) +
// varint(len(PreviousTxScript)) + PreviousTxScript.
// Extended input bytes include PreviousTxSatoshis and PreviousTxScript so ReadFromExtended
// deserializer can be used on the read path.
func serializeInputs(inputs []*bt.Input) []byte {
	var buf bytes.Buffer
	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, uint32(len(inputs))) //nolint:gosec
	buf.Write(countBytes)

	for _, input := range inputs {
		// Standard input serialization
		b := input.Bytes(false)

		// Append PreviousTxSatoshis (8 bytes LE) — extended field
		sat := make([]byte, 8)
		binary.LittleEndian.PutUint64(sat, input.PreviousTxSatoshis)
		b = append(b, sat...)

		// Append PreviousTxScript (varint-prefixed) — extended field
		if input.PreviousTxScript == nil {
			b = append(b, bt.VarInt(0).Bytes()...)
		} else {
			l := uint64(len(*input.PreviousTxScript))
			b = append(b, bt.VarInt(l).Bytes()...)
			b = append(b, *input.PreviousTxScript...)
		}

		// Length-prefix the whole entry
		lenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBytes, uint32(len(b))) //nolint:gosec
		buf.Write(lenBytes)
		buf.Write(b)
	}

	return buf.Bytes()
}

// serializeOutputs serializes transaction outputs into a length-prefixed byte blob.
//
// Format: [count:4 LE] then for each output [len:4 LE][output-bytes]
func serializeOutputs(outputs []*bt.Output) []byte {
	var buf bytes.Buffer
	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, uint32(len(outputs))) //nolint:gosec
	buf.Write(countBytes)

	for _, output := range outputs {
		if output == nil {
			// Zero-length entry for nil outputs
			buf.Write([]byte{0, 0, 0, 0})
			continue
		}
		b := output.Bytes()
		lenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBytes, uint32(len(b))) //nolint:gosec
		buf.Write(lenBytes)
		buf.Write(b)
	}

	return buf.Bytes()
}

// deserializeInputs reads back inputs from the format written by serializeInputs.
func deserializeInputs(data []byte) ([]*bt.Input, error) {
	if len(data) < 4 {
		return nil, nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	inputs := make([]*bt.Input, 0, count)

	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			break
		}
		entryLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+entryLen > len(data) {
			break
		}
		input := &bt.Input{}
		if _, err := input.ReadFromExtended(bytes.NewReader(data[pos : pos+entryLen])); err != nil {
			return inputs, err
		}
		inputs = append(inputs, input)
		pos += entryLen
	}

	return inputs, nil
}

// deserializeOutputs reads back outputs from the format written by serializeOutputs.
func deserializeOutputs(data []byte) ([]*bt.Output, error) {
	if len(data) < 4 {
		return nil, nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	outputs := make([]*bt.Output, 0, count)

	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			break
		}
		entryLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if entryLen == 0 {
			outputs = append(outputs, nil)
			continue
		}
		if pos+entryLen > len(data) {
			break
		}
		output := &bt.Output{}
		if _, err := output.ReadFrom(bytes.NewReader(data[pos : pos+entryLen])); err != nil {
			return outputs, err
		}
		outputs = append(outputs, output)
		pos += entryLen
	}

	return outputs, nil
}

// ---------------------------------------------------------------------------
// txToCreateItem: bt.Tx → teraslab.CreateItem
// ---------------------------------------------------------------------------

func txToCreateItem(tx *bt.Tx, blockHeight uint32, opts utxo.CreateOptions) (teraslab.CreateItem, error) {
	txHash := tx.TxIDChainHash()

	var txid teraslab.TxID
	if opts.TxID != nil {
		txid = hashToTxID(opts.TxID)
	} else {
		txid = hashToTxID(txHash)
	}

	isCoinbase := tx.IsCoinbase()
	if opts.IsCoinbase != nil {
		isCoinbase = *opts.IsCoinbase
	}

	// Compute UTXO hashes for each non-OP_RETURN output
	utxoHashes := make([]teraslab.UtxoHash, 0, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output.LockingScript.IsData() {
			utxoHashes = append(utxoHashes, teraslab.UtxoHash{})
			continue
		}
		hash, err := util.UTXOHashFromOutput(txHash, output, uint32(i)) //nolint:gosec
		if err != nil {
			return teraslab.CreateItem{}, err
		}
		utxoHashes = append(utxoHashes, hashToUtxoHash(hash))
	}

	// Calculate fee
	fee := tx.TotalOutputSatoshis()
	if !isCoinbase && tx.TotalInputSatoshis() > fee {
		fee = tx.TotalInputSatoshis() - tx.TotalOutputSatoshis()
	} else {
		fee = 0
	}

	// Build flags for the CreateBatch wire protocol.
	// The dispatch handler reads: bit 0 = locked, bit 1 = conflicting, bit 2 = frozen.
	// IsCoinbase is sent via the separate IsCoinbase bool field, not in flags.
	var flags uint8
	if opts.Locked {
		flags |= 0x01
	}
	if opts.Conflicting {
		flags |= 0x02
	}
	if opts.Frozen {
		flags |= 0x04
	}

	var spendingHeight uint32
	if isCoinbase {
		spendingHeight = blockHeight
	}

	// Serialize tx data for cold storage
	inputsBlob := serializeInputs(tx.Inputs)
	outputsBlob := serializeOutputs(tx.Outputs)

	var inpointsBlob []byte
	txInpoints, err := subtree.NewTxInpointsFromTx(tx)
	if err == nil {
		inpointsBlob, _ = txInpoints.Serialize()
	}

	item := teraslab.CreateItem{
		TxID:           txid,
		TxVersion:      tx.Version,
		Locktime:       tx.LockTime,
		Fee:            fee,
		SizeInBytes:    uint64(len(tx.Bytes())),
		ExtendedSize:   uint64(tx.Size()),
		IsCoinbase:     isCoinbase,
		SpendingHeight: spendingHeight,
		CreatedAt:      uint64(time.Now().UnixMilli()),
		Flags:          flags,
		BlockHeight:    blockHeight,
		UtxoHashes:     utxoHashes,
		TxData: teraslab.TxData{
			Inputs:   inputsBlob,
			Outputs:  outputsBlob,
			Inpoints: inpointsBlob,
		},
	}

	// Populate parent txids for conflicting tx creation
	if opts.Conflicting {
		seen := make(map[teraslab.TxID]bool)
		for _, input := range tx.Inputs {
			prevTxID := input.PreviousTxIDChainHash()
			if prevTxID != nil {
				tid := hashToTxID(prevTxID)
				if !seen[tid] {
					item.ParentTxIDs = append(item.ParentTxIDs, tid)
					seen[tid] = true
				}
			}
		}
	}

	// Set mined block info if provided
	if len(opts.MinedBlockInfos) > 0 {
		mbi := opts.MinedBlockInfos[0]
		blockID := mbi.BlockID
		blockHeightVal := mbi.BlockHeight
		subtreeIdx := uint32(mbi.SubtreeIdx) //nolint:gosec
		item.MinedBlockID = &blockID
		item.MinedBlockHeight = &blockHeightVal
		item.MinedSubtreeIdx = &subtreeIdx
	}

	return item, nil
}

// ---------------------------------------------------------------------------
// recordToMetaData: teraslab.TxRecord → meta.Data
// ---------------------------------------------------------------------------

func recordToMetaData(rec teraslab.TxRecord) (*meta.Data, error) {
	if !rec.Found {
		return nil, nil
	}

	data := &meta.Data{}

	if rec.Metadata != nil {
		md := rec.Metadata
		data.Fee = md.Fee
		data.SizeInBytes = md.SizeInBytes
		data.LockTime = md.Locktime
		data.UnminedSince = md.UnminedSince
		// TeraSlab TxFlags: bit 0=IS_COINBASE, bit 1=CONFLICTING, bit 2=LOCKED
		data.IsCoinbase = md.Flags&0x01 != 0
		data.Conflicting = md.Flags&0x02 != 0
		data.Locked = md.Flags&0x04 != 0
	}

	if len(rec.Slots) > 0 {
		data.SpendingDatas = make([]*spend.SpendingData, len(rec.Slots))
		for i, slot := range rec.Slots {
			if slot.Status == teraslab.SlotSpent {
				data.SpendingDatas[i] = wireToSpendingData(slot.SpendingData)
			}
			if slot.Status == teraslab.SlotFrozen {
				data.Frozen = true
				// Surface a frozen UTXO as the FrozenBytesTxHash sentinel in its
				// spending data, mirroring GetSpend and the Aerospike backend.
				// The shared conflict-resolution helpers (GetConflictingChildren /
				// GetCounterConflictingTxHashes) treat this sentinel as a frozen
				// child and reject a reorg/blessing that would re-enable a tx whose
				// counter-party is frozen. Without it, conflict resolution silently
				// ignores frozen UTXOs and lets such blocks through.
				data.SpendingDatas[i] = spend.NewSpendingData(&subtree.FrozenBytesTxHash, i)
			}
		}
	}

	// Reconstruct bt.Tx from metadata (version, locktime) + stored inputs/outputs
	if rec.TxData != nil {
		tx := &bt.Tx{}
		if rec.Metadata != nil {
			tx.Version = rec.Metadata.TxVersion
			tx.LockTime = rec.Metadata.Locktime
		}
		if len(rec.TxData.Inputs) > 0 {
			inputs, err := deserializeInputs(rec.TxData.Inputs)
			if err == nil {
				tx.Inputs = inputs
			}
		}
		if len(rec.TxData.Outputs) > 0 {
			outputs, err := deserializeOutputs(rec.TxData.Outputs)
			if err == nil {
				tx.Outputs = outputs
			}
		}
		if len(rec.TxData.Inpoints) > 0 {
			txInpoints, err := subtree.NewTxInpointsFromBytes(rec.TxData.Inpoints)
			if err == nil {
				data.TxInpoints = txInpoints
			}
		}
		data.Tx = tx
	}

	if data.Tx == nil {
		data.Tx = &bt.Tx{}
	}

	if len(rec.BlockEntries) > 0 {
		data.BlockIDs = make([]uint32, len(rec.BlockEntries))
		data.BlockHeights = make([]uint32, len(rec.BlockEntries))
		data.SubtreeIdxs = make([]int, len(rec.BlockEntries))
		for i, entry := range rec.BlockEntries {
			data.BlockIDs[i] = entry.BlockID
			data.BlockHeights[i] = entry.BlockHeight
			data.SubtreeIdxs[i] = int(entry.SubtreeIdx)
		}
	}

	if len(rec.ConflictingChildren) > 0 {
		data.ConflictingChildren = make([]chainhash.Hash, len(rec.ConflictingChildren))
		for i, txid := range rec.ConflictingChildren {
			data.ConflictingChildren[i] = chainhash.Hash(txid)
		}
	}

	return data, nil
}
