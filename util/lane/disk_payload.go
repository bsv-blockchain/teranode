package lane

import (
	"bytes"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// The lane disk payload is what block assembly's disk transaction map keeps for one pooled
// transaction once lanes are on: its parents, its own lane position and its parents' lane
// positions, so a removal or a reorg never reads the transaction's record again.
//
//	[1]  format version, 1
//	[8]  own position
//	TxInpoints, serialized as go-subtree serializes them
//	[8]  per distinct parent, in TxInpoints parent order
//
// The parent position count is the TxInpoints parent count; the decoder needs the whole buffer
// and refuses a trailing byte.

// DiskPayloadVersion is the only version of the lane disk payload.
const DiskPayloadVersion = byte(1)

// EncodeDiskPayload serializes a pooled transaction's parents and positions, checking them first.
func EncodeDiskPayload(inpoints *subtreepkg.TxInpoints, p TxPositions) ([]byte, error) {
	if inpoints == nil {
		return nil, errors.NewProcessingError("lane disk payload: inpoints are required")
	}

	if err := p.Check(inpoints.ParentTxHashes); err != nil {
		return nil, errors.NewProcessingError("lane disk payload", err)
	}

	size, err := inpoints.SerializedSize()
	if err != nil {
		return nil, errors.NewProcessingError("lane disk payload: sizing inpoints", err)
	}

	buf := make([]byte, 0, 1+PositionSize+size+len(p.Parents)*PositionSize)
	buf = append(buf, DiskPayloadVersion)
	buf = AppendPosition(buf, p.Own)

	if buf, err = inpoints.AppendSerialized(buf); err != nil {
		return nil, errors.NewProcessingError("lane disk payload: serializing inpoints", err)
	}

	return AppendPositions(buf, p.Parents), nil
}

// DecodeDiskPayload parses and checks a lane disk payload.
func DecodeDiskPayload(b []byte) (subtreepkg.TxInpoints, TxPositions, error) {
	if len(b) < 1+PositionSize+4 {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload is %d bytes, too short", len(b))
	}

	if b[0] != DiskPayloadVersion {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload has unsupported version %d", b[0])
	}

	own, _, err := ReadPositions(b[1:], 1)
	if err != nil {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload own position", err)
	}

	r := bytes.NewReader(b[1+PositionSize:])

	inpoints, err := subtreepkg.NewTxInpointsFromReader(r)
	if err != nil {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload inpoints", err)
	}

	rest := b[len(b)-r.Len():]

	parents, rest, err := ReadPositions(rest, len(inpoints.ParentTxHashes))
	if err != nil {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload parent positions", err)
	}

	if len(rest) != 0 {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload has %d trailing bytes", len(rest))
	}

	p := TxPositions{Own: own[0], Parents: parents}
	if err = p.Check(inpoints.ParentTxHashes); err != nil {
		return subtreepkg.TxInpoints{}, TxPositions{}, errors.NewProcessingError("lane disk payload", err)
	}

	return inpoints, p, nil
}
