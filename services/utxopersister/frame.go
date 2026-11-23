package utxopersister

import (
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// wrapperHeaderSize is TxID (32) + encoded height/coinbase (4) + UTXO count (4).
	wrapperHeaderSize = 32 + 4 + 4

	// utxoFixedSize is index (4) + value (8) + script length (4).
	utxoFixedSize = 4 + 8 + 4
)

// ReadUTXOWrapperFrame reads the next UTXOWrapper record from r as its raw
// bytes (exactly what UTXOWrapper.Bytes produces), without decoding it, and
// returns the highest output index in it (0 for a record without UTXOs).
//
// The record is read into buf (reused from buf[:0], grown when too small) and
// the returned frame aliases it, so a caller that keeps the frame past the
// next call must copy it. This lets a single reader cut a .utxo-set stream
// into records without allocating for records it then skips, and leave
// decoding (DecodeUTXOWrapperFrame) to parallel consumers. The end-of-stream contract matches FromReader: io.EOF at
// a clean end, *ErrRecordBoundary when exactly FooterSize bytes remain, and a
// storage error for any record cut short. Untrusted counts and lengths never
// size an allocation beyond the bytes actually present, as in FromReader.
func ReadUTXOWrapperFrame(r io.Reader, buf []byte) ([]byte, uint32, error) {
	// Read the header straight into buf: a local array passed to io.ReadFull
	// through the io.Reader interface escapes, costing an allocation per record.
	frame := append(buf[:0], make([]byte, wrapperHeaderSize)...)

	n, err := io.ReadFull(r, frame[:32])
	if err != nil {
		if err == io.EOF {
			return nil, 0, io.EOF
		}

		if n == FooterSize && err == io.ErrUnexpectedEOF {
			boundary := &ErrRecordBoundary{}
			copy(boundary.FooterBytes[:], frame[:FooterSize])

			return nil, 0, boundary
		}

		return nil, 0, errors.NewStorageError("failed to read txid, expected 32 bytes got %d", n, err)
	}

	if n, err = io.ReadFull(r, frame[32:wrapperHeaderSize]); err != nil {
		return nil, 0, errors.NewStorageError("failed to read height and number of utxos, expected 8 bytes got %d", n, err)
	}

	numUTXOs := binary.LittleEndian.Uint32(frame[36:40])

	var maxIndex uint32

	for i := uint32(0); i < numUTXOs; i++ {
		start := len(frame)
		frame = append(frame, make([]byte, utxoFixedSize)...)

		if n, err = io.ReadFull(r, frame[start:]); err != nil {
			return nil, 0, errors.NewStorageError("failed to read utxo %d of %d, expected %d bytes got %d", i, numUTXOs, utxoFixedSize, n, err)
		}

		maxIndex = max(maxIndex, binary.LittleEndian.Uint32(frame[start:start+4]))
		l := binary.LittleEndian.Uint32(frame[start+12 : start+16])

		if frame, err = appendScript(frame, r, l); err != nil {
			return nil, 0, err
		}
	}

	return frame, maxIndex, nil
}

// appendScript reads an l-byte script from r onto frame. Up to
// maxExactScriptAlloc it grows frame by l at once; beyond that, l is not
// trusted and frame grows in maxExactScriptAlloc chunks as bytes arrive, so a
// record claiming a 4 GB script fails after reading what is really there.
func appendScript(frame []byte, r io.Reader, l uint32) ([]byte, error) {
	for remaining := l; remaining > 0; {
		chunk := min(remaining, maxExactScriptAlloc)
		start := len(frame)
		frame = append(frame, make([]byte, chunk)...)

		if _, err := io.ReadFull(r, frame[start:]); err != nil {
			return nil, errors.NewStorageError("failed to read utxo script (%d bytes)", l, err)
		}

		remaining -= chunk
	}

	return frame, nil
}

// FrameUTXOCount returns the UTXO count recorded in a frame from
// ReadUTXOWrapperFrame, without decoding it.
func FrameUTXOCount(frame []byte) uint32 {
	return binary.LittleEndian.Uint32(frame[36:40])
}

// DecodeUTXOWrapperFrame decodes a record produced by ReadUTXOWrapperFrame
// (or UTXOWrapper.Bytes). The frame must hold exactly one record.
//
// To keep per-record allocations constant, scripts are views into frame (with
// their capacity limited to their length) and all UTXOs share one backing
// array, so the caller must not modify frame afterwards.
func DecodeUTXOWrapperFrame(frame []byte) (*UTXOWrapper, error) {
	if len(frame) < wrapperHeaderSize {
		return nil, errors.NewProcessingError("utxo wrapper frame too short: %d bytes", len(frame))
	}

	uw := &UTXOWrapper{}
	copy(uw.TxID[:], frame[:32])

	encodedHeight := binary.LittleEndian.Uint32(frame[32:36])
	uw.Height = encodedHeight >> 1
	uw.Coinbase = encodedHeight&1 == 1

	numUTXOs := binary.LittleEndian.Uint32(frame[36:40])

	// Every UTXO takes at least utxoFixedSize bytes, which bounds an untrusted
	// count by the frame's real size before anything is allocated from it.
	if uint64(numUTXOs) > uint64(len(frame)-wrapperHeaderSize)/utxoFixedSize {
		return nil, errors.NewProcessingError("utxo wrapper frame claims %d utxos in %d bytes", numUTXOs, len(frame))
	}

	backing := make([]UTXO, numUTXOs)
	uw.UTXOs = make([]*UTXO, numUTXOs)

	off := wrapperHeaderSize

	for i := range backing {
		if len(frame)-off < utxoFixedSize {
			return nil, errors.NewProcessingError("utxo wrapper frame truncated at utxo %d", i)
		}

		u := &backing[i]
		u.Index = binary.LittleEndian.Uint32(frame[off : off+4])
		u.Value = binary.LittleEndian.Uint64(frame[off+4 : off+12])
		l := binary.LittleEndian.Uint32(frame[off+12 : off+16])
		off += utxoFixedSize

		if uint64(l) > uint64(len(frame)-off) {
			return nil, errors.NewProcessingError("utxo wrapper frame truncated in script of utxo %d", i)
		}

		end := off + int(l)
		u.Script = frame[off:end:end]
		off = end

		uw.UTXOs[i] = u
	}

	if off != len(frame) {
		return nil, errors.NewProcessingError("utxo wrapper frame has %d trailing bytes", len(frame)-off)
	}

	return uw, nil
}
