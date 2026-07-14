package diskblocks

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// framingSize is the per-block header in blk*.dat: [4-byte network magic LE][4-byte size LE].
const framingSize = 8

// BlockReader reads raw blocks from an SV Node datadir's blk*.dat files.
type BlockReader struct {
	blocksDir string
	net       wire.BitcoinNet
}

// NewBlockReader returns a reader for <datadir>/blocks, validating each block's
// framing magic against net.
func NewBlockReader(datadir string, net wire.BitcoinNet) *BlockReader {
	return &BlockReader{
		blocksDir: filepath.Join(datadir, "blocks"),
		net:       net,
	}
}

// ReadBlock reads the block at ref from blk{NFile}.dat. The index nDataPos
// points at the payload start; the 8-byte [magic][size] framing precedes it.
// Returns the decoded block and the total on-disk bytes consumed (framing +
// payload). A short read / truncated final record returns an error so the
// caller can stop cleanly.
func (br *BlockReader) ReadBlock(ref *BlockRef) (*wire.MsgBlock, uint64, error) {
	path := filepath.Join(br.blocksDir, fmt.Sprintf("blk%05d.dat", ref.NFile))
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, errors.NewProcessingError("open %s", path, err)
	}
	defer f.Close()

	framingStart := int64(ref.NDataPos) - framingSize
	if framingStart < 0 {
		return nil, 0, errors.NewProcessingError("invalid nDataPos %d in %s", ref.NDataPos, path)
	}
	if _, err = f.Seek(framingStart, io.SeekStart); err != nil {
		return nil, 0, errors.NewProcessingError("seek %s", path, err)
	}

	var hdr [framingSize]byte
	if _, err = io.ReadFull(f, hdr[:]); err != nil {
		return nil, 0, errors.NewProcessingError("read framing in %s", path, err)
	}
	magic := wire.BitcoinNet(binary.LittleEndian.Uint32(hdr[0:4]))
	size := binary.LittleEndian.Uint32(hdr[4:8])
	if magic != br.net {
		return nil, 0, errors.NewProcessingError("block magic %x at %s:%d does not match network %x", uint32(magic), path, ref.NDataPos, uint32(br.net))
	}

	lr := &io.LimitedReader{R: f, N: int64(size)}
	msg := &wire.MsgBlock{}
	if err = msg.Bsvdecode(lr, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
		return nil, 0, errors.NewProcessingError("decode block in %s", path, err)
	}
	if lr.N != 0 {
		return nil, 0, errors.NewProcessingError("short decode in %s: %d frame bytes unconsumed", path, lr.N)
	}

	return msg, uint64(size) + framingSize, nil
}
