package teraslab

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	teraslab "github.com/icellan/teraslab/client/go"
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
// fields. The Aerospike backend returns the full metadata set plus transaction
// data by default, so this requests all metadata bits (including unmined_since,
// created_at and spending_height — which utxo.MetaFieldsWithTx omits) plus the
// cold data needed to reconstruct the tx and the block entries.
var defaultGetMask = teraslab.FieldAllMetadata | teraslab.FieldColdData | teraslab.FieldBlockEntries

// defaultGetMetaMask is the field mask used by GetMeta(). Same metadata coverage
// as the default Get mask; TxInpoints reconstruction already requires the cold
// data, so it is included.
var defaultGetMetaMask = teraslab.FieldAllMetadata | teraslab.FieldColdData | teraslab.FieldBlockEntries

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
//
// A zero-length blob means "no inputs stored" and yields (nil, nil). Any other
// malformation — a truncated count header, a declared count the buffer cannot
// hold, an entry length that overruns, or an unparseable entry body — is a
// corrupt store read and returns an error rather than a silently-partial slice.
func deserializeInputs(data []byte) ([]*bt.Input, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.NewProcessingError("teraslab: inputs blob truncated (%d bytes, need >=4 for count header)", len(data))
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	// Each entry consumes at least a 4-byte length prefix, so a declared count
	// larger than the remaining bytes can hold is corrupt. This also bounds the
	// preallocation so a bogus count cannot force a huge up-front allocation.
	if count > uint32((len(data)-4)/4) { //nolint:gosec
		return nil, errors.NewProcessingError("teraslab: inputs blob declares %d entries but only %d bytes remain", count, len(data)-4)
	}
	inputs := make([]*bt.Input, 0, count)

	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return nil, errors.NewProcessingError("teraslab: inputs blob truncated reading entry %d length prefix", i)
		}
		entryLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+entryLen > len(data) {
			return nil, errors.NewProcessingError("teraslab: inputs blob entry %d length %d overruns buffer", i, entryLen)
		}
		input := &bt.Input{}
		if _, err := input.ReadFromExtended(bytes.NewReader(data[pos : pos+entryLen])); err != nil {
			return nil, errors.NewProcessingError("teraslab: inputs blob entry %d malformed", i, err)
		}
		inputs = append(inputs, input)
		pos += entryLen
	}

	return inputs, nil
}

// deserializeOutputs reads back outputs from the format written by serializeOutputs.
//
// A zero-length blob means "no outputs stored" and yields (nil, nil). Any other
// malformation is treated as a corrupt store read and returns an error rather
// than a silently-partial slice. A zero-length entry is a legitimately nil
// output (preserved by serializeOutputs).
func deserializeOutputs(data []byte) ([]*bt.Output, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.NewProcessingError("teraslab: outputs blob truncated (%d bytes, need >=4 for count header)", len(data))
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	// Each entry consumes at least a 4-byte length prefix; reject a declared
	// count the buffer cannot hold and bound the preallocation in one check.
	if count > uint32((len(data)-4)/4) { //nolint:gosec
		return nil, errors.NewProcessingError("teraslab: outputs blob declares %d entries but only %d bytes remain", count, len(data)-4)
	}
	outputs := make([]*bt.Output, 0, count)

	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return nil, errors.NewProcessingError("teraslab: outputs blob truncated reading entry %d length prefix", i)
		}
		entryLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if entryLen == 0 {
			outputs = append(outputs, nil)
			continue
		}
		if pos+entryLen > len(data) {
			return nil, errors.NewProcessingError("teraslab: outputs blob entry %d length %d overruns buffer", i, entryLen)
		}
		output := &bt.Output{}
		if _, err := output.ReadFrom(bytes.NewReader(data[pos : pos+entryLen])); err != nil {
			return nil, errors.NewProcessingError("teraslab: outputs blob entry %d malformed", i, err)
		}
		outputs = append(outputs, output)
		pos += entryLen
	}

	return outputs, nil
}

// extendedTxSize returns the length of the extended (prev-output-carrying)
// serialization of tx without allocating it — equivalent to
// len(tx.ExtendedBytes()) but allocation-free, mirroring the Aerospike backend
// (aerospike/create.go). Avoids a second full serialization on the create hot
// path (tx.Size() already gives the standard length for SizeInBytes).
func extendedTxSize(tx *bt.Tx) int {
	size := tx.Size() + 6
	for _, in := range tx.Inputs {
		if in.PreviousTxIDChainHash() == nil {
			size -= 32
		}
		size += 8
		if in.PreviousTxScript == nil {
			size++
		} else {
			l := len(*in.PreviousTxScript)
			size += bt.VarInt(uint64(l)).Length() + l
		}
	}
	return size
}

// ---------------------------------------------------------------------------
// txToCreateItem: bt.Tx → teraslab.CreateItem
// ---------------------------------------------------------------------------

func txToCreateItem(tx *bt.Tx, blockHeight uint32, coinbaseMaturity uint32, genesisActivationHeight uint32, opts utxo.CreateOptions) (teraslab.CreateItem, error) {
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

	// Compute the fee and per-output UTXO hashes through the shared utxo helpers
	// so the rules stay identical to the Aerospike/SQL backends — parity is the
	// whole contract for this store. GetFeesAndUtxoHashes hard-errors on a
	// non-extended (undecorated) non-coinbase tx instead of silently recording a
	// zero fee (which would corrupt block-assembly fee ordering / mining totals);
	// the inputless branch mirrors aerospike create.go (fee 0, hashes only).
	var (
		fee          uint64
		utxoHashPtrs []*chainhash.Hash
		err          error
	)
	if len(tx.Inputs) == 0 {
		utxoHashPtrs, err = utxo.GetUtxoHashes(tx, txHash)
	} else {
		fee, utxoHashPtrs, err = utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	}
	if err != nil {
		return teraslab.CreateItem{}, err
	}

	// Store a real UTXO hash only for outputs that should join the UTXO set
	// (era-aware, value-agnostic; matches utxo.ShouldStoreOutputAsUTXO used by
	// Aerospike create.go and SQL sql.go). Provably-unspendable outputs keep a
	// zero UtxoHash{}, which the server treats as "not a spendable UTXO". Using
	// bt.Script.IsData() here would fork the UTXO set: it is era-agnostic, so it
	// drops post-Genesis bare OP_RETURN (which must be retained) and keeps
	// pre-Genesis oversized scripts (which must be dropped).
	utxoHashes := make([]teraslab.UtxoHash, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output != nil && utxo.ShouldStoreOutputAsUTXO(output, blockHeight, genesisActivationHeight) {
			utxoHashes[i] = hashToUtxoHash(utxoHashPtrs[i])
		}
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

	// Coinbase outputs are only spendable after the maturity window. Store the
	// absolute spendable-at height (blockHeight + CoinbaseMaturity) as the server
	// records the SpendingHeight verbatim, matching the Aerospike backend
	// (create.go: blockHeight + CoinbaseMaturity). Sending just blockHeight would
	// make the coinbase spendable at its creation height.
	var spendingHeight uint32
	if isCoinbase {
		spendingHeight = blockHeight + coinbaseMaturity
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
		SizeInBytes:    uint64(tx.Size()),
		ExtendedSize:   uint64(extendedTxSize(tx)),
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

// recordToMetaData converts a TeraSlab record into meta.Data, fully
// reconstructing the transaction body (inputs/outputs). This is what most
// callers need; the metadata-only GetMeta path uses recordToMetaDataMasked with
// includeTx=false to skip the input/output decode it would immediately discard.
func recordToMetaData(rec teraslab.TxRecord) (*meta.Data, error) {
	return recordToMetaDataMasked(rec, true)
}

// recordToMetaDataMasked converts a TeraSlab record into meta.Data. When
// includeTx is false the stored inputs/outputs are NOT decoded into data.Tx;
// only the cheap fields (metadata, slots, inpoints, block entries) are filled.
// TxInpoints — which GetMeta needs — is always decoded regardless of includeTx.
func recordToMetaDataMasked(rec teraslab.TxRecord, includeTx bool) (*meta.Data, error) {
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
		if includeTx && len(rec.TxData.Inputs) > 0 {
			inputs, err := deserializeInputs(rec.TxData.Inputs)
			if err != nil {
				return nil, errors.NewTxInvalidError("teraslab: could not read stored inputs", err)
			}
			tx.Inputs = inputs
		}
		if includeTx && len(rec.TxData.Outputs) > 0 {
			outputs, err := deserializeOutputs(rec.TxData.Outputs)
			if err != nil {
				return nil, errors.NewTxInvalidError("teraslab: could not read stored outputs", err)
			}
			tx.Outputs = outputs
		}
		if len(rec.TxData.Inpoints) > 0 {
			txInpoints, err := subtree.NewTxInpointsFromBytes(rec.TxData.Inpoints)
			if err != nil {
				return nil, errors.NewTxInvalidError("teraslab: could not read stored inpoints", err)
			}
			data.TxInpoints = txInpoints
		}
		data.Tx = tx
	}

	if data.Tx == nil {
		data.Tx = &bt.Tx{}
	}

	// TeraSlab returns at most MaxInlineBlockEntries block entries inline and flags
	// the surplus as truncated (the overflow lives in on-disk storage the normal
	// read does not return). A truncated set would silently cap BlockIDs and
	// corrupt reorg/rewind block-membership decisions — which depend on the full
	// set — so surface an error rather than hand back an incomplete list. The
	// Aerospike backend stores the full set and never truncates.
	if rec.BlockEntriesTruncated {
		return nil, errors.NewProcessingError("teraslab: block entries truncated (more than the inline cap); the full block-membership set is unavailable")
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
