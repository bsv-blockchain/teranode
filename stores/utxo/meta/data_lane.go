package meta

import (
	"bytes"
	"encoding/binary"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The lane metadata encoding is the versioned form of MetaBytes that carries lane positions. On the
// txmeta topic the action byte says which encoding an entry holds. The txmeta cache keeps the
// embedded MetaBytes, followed at shadow by the own position (CacheBytesInto); the parents'
// positions stay out of it. MetaBytes stays byte for byte what it was:
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
	// LaneMetaMagic is the first byte of the lane metadata encoding. It is not a discriminator: a
	// legacy MetaBytes begins with a fee, and a fee whose low 16 bits are 0x01A7 (423 satoshis,
	// say) starts with the same two bytes as LaneMetaMagic and LaneMetaVersion. So whatever carries
	// the lane encoding must say so outside it, never leaving a reader to guess from the bytes. On
	// the txmeta topic each entry's action byte says it (txmetacache.WireActionADDLane); the txmeta
	// cache keeps MetaBytes, followed at shadow by the own position (CacheBytesInto), whose place is
	// found by walking the inpoints, never by these bytes.
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

// LaneMetaBytesView returns the MetaBytes a lane metadata encoding holds whole, between its header
// and its parent positions, as a sub-slice of b capped at its own length, with nothing parsed into
// objects and nothing allocated. It checks the magic, version, own position and flag bits, walks the
// inpoints' counts to find exactly where they end, and checks that one valid position per parent
// follows and nothing else. It is for a receiver that keeps MetaBytes and not the positions;
// NewDataFromLaneMetaBytes is the full check.
func LaneMetaBytesView(b []byte) ([]byte, error) {
	if len(b) < laneMetaHeader+4 {
		return nil, errors.NewProcessingError("lane metadata is %d bytes, too short", len(b))
	}

	if b[0] != LaneMetaMagic || b[1] != LaneMetaVersion {
		return nil, errors.NewProcessingError("lane metadata starts with %#x %#x, not the lane magic and version", b[0], b[1])
	}

	if err := lane.CheckPosition(binary.LittleEndian.Uint64(b[2:])); err != nil {
		return nil, errors.NewProcessingError("lane metadata own position", err)
	}

	if flags := b[laneMetaHeader-1]; flags&^0b11111 != 0 {
		return nil, errors.NewProcessingError("lane metadata has unknown flag bits %#x", flags)
	}

	size := uint64(len(b))
	parents := uint64(binary.LittleEndian.Uint32(b[laneMetaHeader:]))
	off := uint64(laneMetaHeader) + 4 + parents*32

	for i := uint64(0); i < parents; i++ {
		if off+4 > size {
			return nil, errors.NewProcessingError("lane metadata inpoints truncated at parent %d", i)
		}

		off += 4 + 4*uint64(binary.LittleEndian.Uint32(b[off:]))
	}

	if off > size || size-off != parents*lane.PositionSize {
		return nil, errors.NewProcessingError("lane metadata has %d bytes after its inpoints for %d parent positions", int64(size)-int64(off), parents)
	}

	for p := off; p < size; p += lane.PositionSize {
		if err := lane.CheckPosition(binary.LittleEndian.Uint64(b[p:])); err != nil {
			return nil, errors.NewProcessingError("lane metadata parent position", err)
		}
	}

	return b[2+lane.PositionSize : off : off], nil
}

// The txmeta cache form. The cache keeps MetaBytes, and at utxostore_laneMigration shadow the
// record's own lane position after it:
//
//	MetaBytes
//	then     the own lane position, uint64 LE, only when it is a valid position
//
// MetaBytes ends exactly where its TxInpoints end, so the 8 bytes after them are the position and
// nothing else can be: a decoder that walks the inpoints, as every cache decoder does, reads the
// position from what is left, and one that ignores trailing bytes, as NewMetaDataFromBytes does,
// reads the entry as before. A value with no valid position is plain MetaBytes. The parents'
// positions stay out of the cache. Subtree validation reads the cached own position when it builds
// a subtree's lane bitmaps; the block gate later reads those bitmap files, not the cache.

// CacheBytesInto is MetaBytesInto followed by d.LanePosition when that is a valid position: the
// txmeta cache form.
func (d *Data) CacheBytesInto(dst []byte) ([]byte, error) {
	b, err := d.MetaBytesInto(dst)
	if err != nil || !lane.Valid(d.LanePosition) {
		return b, err
	}

	return lane.AppendPosition(b, d.LanePosition), nil
}

// CacheBytes is CacheBytesInto with a fresh buffer.
func (d *Data) CacheBytes() ([]byte, error) {
	return d.CacheBytesInto(nil)
}

// CachedLanePosition returns the own lane position a txmeta cache value carries after its
// MetaBytes, given rest, the bytes after its TxInpoints, or 0 when it carries none.
func CachedLanePosition(rest []byte) uint64 {
	if len(rest) != lane.PositionSize {
		return 0
	}

	return binary.LittleEndian.Uint64(rest)
}

// metaBytesHeader is MetaBytes' fee (8), size (8) and flags (1), before its TxInpoints.
const metaBytesHeader = 17

// OwnCacheBytesView checks b, a txmeta cache form that carries an own lane position, and returns
// its MetaBytes, as a sub-slice of b with nothing copied, and the position. It refuses b unless
// its TxInpoints end exactly one position before its end and that position is valid: the content
// of a WireActionADDOwn entry, which CacheBytesInto writes for a record with a position.
func OwnCacheBytesView(b []byte) ([]byte, uint64, error) {
	if len(b) < metaBytesHeader+4+lane.PositionSize {
		return nil, 0, errors.NewProcessingError("own cache form is %d bytes, too short", len(b))
	}

	// The flags byte ends MetaBytes' header; CacheBytesInto writes only the five known bits, as
	// LaneMetaBytesView requires of the lane encoding.
	if flags := b[metaBytesHeader-1]; flags&^0b11111 != 0 {
		return nil, 0, errors.NewProcessingError("own cache form has unknown flag bits %#x", flags)
	}

	size := uint64(len(b))
	parents := uint64(binary.LittleEndian.Uint32(b[metaBytesHeader:]))
	off := uint64(metaBytesHeader) + 4 + parents*32

	for i := uint64(0); i < parents; i++ {
		if off+4 > size {
			return nil, 0, errors.NewProcessingError("own cache form inpoints truncated at parent %d", i)
		}

		off += 4 + 4*uint64(binary.LittleEndian.Uint32(b[off:]))
	}

	if off > size || size-off != lane.PositionSize {
		return nil, 0, errors.NewProcessingError("own cache form has %d bytes after its inpoints, not one lane position", int64(size)-int64(off))
	}

	own := binary.LittleEndian.Uint64(b[off:])
	if err := lane.CheckPosition(own); err != nil {
		return nil, 0, errors.NewProcessingError("own cache form position", err)
	}

	return b[:off:off], own, nil
}

// AppendCacheFromLaneMetaBytes appends the txmeta cache form of a lane metadata encoding, its
// MetaBytes and own position, to dst, checking the encoding, own position included, as
// LaneMetaBytesView does.
func AppendCacheFromLaneMetaBytes(dst, b []byte) ([]byte, error) {
	legacy, err := LaneMetaBytesView(b)
	if err != nil {
		return dst, err
	}

	own := binary.LittleEndian.Uint64(b[2:])

	dst = append(dst, legacy...)

	return lane.AppendPosition(dst, own), nil
}
