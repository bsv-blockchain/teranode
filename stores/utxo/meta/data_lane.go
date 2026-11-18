package meta

import (
	"bytes"
	"encoding/binary"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The lane metadata encoding is the versioned form of MetaBytes that carries lane positions. It is
// for the lane-format txmeta topic and cache, never mixed with the unversioned MetaBytes, which
// stays byte for byte what it was:
//
//	[0]      LaneMetaMagic
//	[1]      LaneMetaVersion
//	[2:10]   own lane position, uint64 LE
//	[10:18]  Fee, uint64 LE
//	[18:26]  SizeInBytes, uint64 LE
//	[26]     flags, as MetaBytes (bit 0 IsCoinbase, 1 Frozen, 2 Conflicting, 3 Locked, 4 InBlock)
//	[27:]    TxInpoints, serialized as go-subtree serializes them
//	then     one 8-byte lane position per distinct parent, in TxInpoints parent order
//
// The parent position count is the TxInpoints parent count. A decoder refuses another magic or
// version, a short or long buffer, a zero or invalid position and a parent listed twice. Nothing
// is defaulted: a reader that needs positions treats every refusal as an error, never as a cache
// miss.

const (
	// LaneMetaMagic is the first byte of the lane metadata encoding.
	LaneMetaMagic = byte(0xA7)

	// LaneMetaVersion is the only version of the lane metadata encoding.
	LaneMetaVersion = byte(1)

	// laneMetaHeader is magic, version, own position, fee, size and flags.
	laneMetaHeader = 2 + lane.PositionSize + 8 + 8 + 1
)

// LaneMetaBytesInto appends d's metadata, its own lane position and parents, one lane position
// per distinct parent in d.TxInpoints, to dst[:0] in the lane metadata encoding. It checks every
// position first.
func (d *Data) LaneMetaBytesInto(dst []byte, parents []uint64) ([]byte, error) {
	p := lane.TxPositions{Own: d.LanePosition, Parents: parents}
	if err := p.Check(d.TxInpoints.ParentTxHashes); err != nil {
		return nil, errors.NewProcessingError("lane metadata", err)
	}

	inpointsSize, err := d.TxInpoints.SerializedSize()
	if err != nil {
		return nil, errors.NewProcessingError("lane metadata: sizing inpoints", err)
	}

	need := laneMetaHeader + inpointsSize + len(parents)*lane.PositionSize

	buf := dst[:0]
	if cap(buf) < need {
		buf = make([]byte, 0, need)
	}

	buf = append(buf, LaneMetaMagic, LaneMetaVersion)
	buf = lane.AppendPosition(buf, d.LanePosition)
	buf = binary.LittleEndian.AppendUint64(buf, d.Fee)
	buf = binary.LittleEndian.AppendUint64(buf, d.SizeInBytes)
	buf = append(buf, d.metaFlags())

	if buf, err = d.TxInpoints.AppendSerialized(buf); err != nil {
		return nil, errors.NewProcessingError("lane metadata: serializing inpoints", err)
	}

	return lane.AppendPositions(buf, parents), nil
}

// NewDataFromLaneMetaBytes decodes the lane metadata encoding into d and returns the parents'
// lane positions. On error d is left unchanged.
func NewDataFromLaneMetaBytes(b []byte, d *Data) ([]uint64, error) {
	if len(b) < laneMetaHeader+4 {
		return nil, errors.NewProcessingError("lane metadata is %d bytes, too short", len(b))
	}

	if b[0] != LaneMetaMagic {
		return nil, errors.NewProcessingError("lane metadata starts with %#x, not the lane magic", b[0])
	}

	if b[1] != LaneMetaVersion {
		return nil, errors.NewProcessingError("lane metadata has unsupported version %d", b[1])
	}

	r := bytes.NewReader(b[laneMetaHeader:])

	inpoints, err := subtree.NewTxInpointsFromReader(r)
	if err != nil {
		return nil, errors.NewProcessingError("lane metadata inpoints", err)
	}

	parents, rest, err := lane.ReadPositions(b[len(b)-r.Len():], len(inpoints.ParentTxHashes))
	if err != nil {
		return nil, errors.NewProcessingError("lane metadata parent positions", err)
	}

	if len(rest) != 0 {
		return nil, errors.NewProcessingError("lane metadata has %d trailing bytes", len(rest))
	}

	p := lane.TxPositions{Own: binary.LittleEndian.Uint64(b[2:]), Parents: parents}
	if err = p.Check(inpoints.ParentTxHashes); err != nil {
		return nil, errors.NewProcessingError("lane metadata", err)
	}

	flags := b[laneMetaHeader-1]
	if flags&^0b11111 != 0 {
		return nil, errors.NewProcessingError("lane metadata has unknown flag bits %#x", flags)
	}

	d.LanePosition = p.Own
	d.Fee = binary.LittleEndian.Uint64(b[10:18])
	d.SizeInBytes = binary.LittleEndian.Uint64(b[18:26])
	d.IsCoinbase = flags&0b1 != 0
	d.Frozen = flags&0b10 != 0
	d.Conflicting = flags&0b100 != 0
	d.Locked = flags&0b1000 != 0
	d.InBlock = flags&0b10000 != 0
	d.TxInpoints = inpoints

	return parents, nil
}

// metaFlags is the flags byte MetaBytes writes.
func (d *Data) metaFlags() byte {
	var f byte

	if d.IsCoinbase {
		f |= 0b1
	}

	if d.Frozen {
		f |= 0b10
	}

	if d.Conflicting {
		f |= 0b100
	}

	if d.Locked {
		f |= 0b1000
	}

	if d.InBlock {
		f |= 0b10000
	}

	return f
}
