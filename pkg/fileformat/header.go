package fileformat

import (
	"bytes"
	"fmt"
	"io"
)

// FileType is an enum-like type for supported file types.
type FileType string

const (
	FileTypeUtxoAdditions  FileType = "utxo-additions"
	FileTypeUtxoDeletions  FileType = "utxo-deletions"
	FileTypeUtxoHeaders    FileType = "utxo-headers"
	FileTypeUtxoSet        FileType = "utxo-set"
	FileTypeBlock          FileType = "block"
	FileTypeSubtree        FileType = "subtree"
	FileTypeSubtreeToCheck FileType = "subtreeToCheck"
	FileTypeSubtreeData    FileType = "subtreeData"
	FileTypeSubtreeMeta    FileType = "subtreeMeta"
	FileTypeTx             FileType = "tx"
	FileTypeOutputs        FileType = "outputs"
	FileTypeBloomFilter    FileType = "bloomfilter"
	FileTypeDat            FileType = "dat"
	FileTypeMsgBlock       FileType = "msgBlock"
	FileTypeTesting        FileType = "testing"
	FileTypeBatchData      FileType = "batch-data"
	FileTypeBatchKeys      FileType = "batch-keys"
	FileTypePreserveUntil  FileType = "preserveUntil"
	FileTypeUnknown        FileType = ""
)

func (f FileType) String() string {
	return string(f)
}

func (f FileType) ToMagicBytes() [8]byte {
	return fileTypeToMagic[f]
}

// Magic header types for file identification - exactly 8 ASCII characters (8 bytes)
var (
	magicUtxoAdditions  = [8]byte{'U', '-', 'A', '-', '1', '.', '0', ' '} // U-A-1.0
	magicUtxoDeletions  = [8]byte{'U', '-', 'D', '-', '1', '.', '0', ' '} // U-D-1.0
	magicUtxoHeaders    = [8]byte{'U', '-', 'H', '-', '1', '.', '0', ' '} // U-H-1.0 (legacy, without coinbase)
	magicUtxoHeadersV2  = [8]byte{'U', '-', 'H', '-', '2', '.', '0', ' '} // U-H-2.0 (with coinbase tx)
	magicUtxoSet        = [8]byte{'U', '-', 'S', '-', '1', '.', '0', ' '} // U-S-1.0
	magicBlock          = [8]byte{'B', '-', '1', '.', '0', ' ', ' ', ' '} // B-1.0
	magicSubtree        = [8]byte{'S', '-', '1', '.', '0', ' ', ' ', ' '} // S-1.0
	magicSubtreeToCheck = [8]byte{'S', 'C', '-', '1', '.', '0', ' ', ' '} // SC-1.0
	magicSubtreeData    = [8]byte{'S', 'D', '-', '1', '.', '0', ' ', ' '} // SD-1.0
	magicSubtreeMeta    = [8]byte{'S', 'M', '-', '1', '.', '0', ' ', ' '} // SM-1.0
	magicTx             = [8]byte{'T', '-', '1', '.', '0', ' ', ' ', ' '} // T-1.0
	magicOutputs        = [8]byte{'O', '-', '1', '.', '0', ' ', ' ', ' '} // O-1.0
	magicBloomFilter    = [8]byte{'B', 'F', '-', '1', '.', '0', ' ', ' '} // BF-1.0
	magicMsgBlock       = [8]byte{'M', 'B', '-', '1', '.', '0', ' ', ' '} // MB-1.0
	magicDat            = [8]byte{'D', 'A', 'T', '-', '1', '.', '0', ' '} // DAT-1.0
	magicTesting        = [8]byte{'T', 'E', 'S', 'T', 'I', 'N', 'G', ' '} // TESTING
	magicBatchData      = [8]byte{'B', 'D', '-', '1', '.', '0', ' ', ' '} // BD-1.0
	magicBatchKeys      = [8]byte{'B', 'K', '-', '1', '.', '0', ' ', ' '} // BK-1.0
	magicPreserveUntil  = [8]byte{'P', 'U', '-', '1', '.', '0', ' ', ' '} // PU-1.0
)

var fileTypeToMagic = map[FileType][8]byte{
	FileTypeUtxoAdditions:  magicUtxoAdditions,
	FileTypeUtxoDeletions:  magicUtxoDeletions,
	FileTypeUtxoHeaders:    magicUtxoHeadersV2, // Default to V2 (with coinbase)
	FileTypeUtxoSet:        magicUtxoSet,
	FileTypeBlock:          magicBlock,
	FileTypeSubtree:        magicSubtree,
	FileTypeSubtreeToCheck: magicSubtreeToCheck,
	FileTypeSubtreeData:    magicSubtreeData,
	FileTypeSubtreeMeta:    magicSubtreeMeta,
	FileTypeTx:             magicTx,
	FileTypeOutputs:        magicOutputs,
	FileTypeBloomFilter:    magicBloomFilter,
	FileTypeMsgBlock:       magicMsgBlock,
	FileTypeDat:            magicDat,
	FileTypeTesting:        magicTesting,
	FileTypeBatchData:      magicBatchData,
	FileTypeBatchKeys:      magicBatchKeys,
	FileTypePreserveUntil:  magicPreserveUntil,
}

var magicToFileType = map[[8]byte]FileType{
	magicUtxoAdditions:  FileTypeUtxoAdditions,
	magicUtxoDeletions:  FileTypeUtxoDeletions,
	magicUtxoHeaders:    FileTypeUtxoHeaders,
	magicUtxoHeadersV2:  FileTypeUtxoHeaders, // V2 also maps to same FileType
	magicUtxoSet:        FileTypeUtxoSet,
	magicBlock:          FileTypeBlock,
	magicSubtree:        FileTypeSubtree,
	magicSubtreeToCheck: FileTypeSubtreeToCheck,
	magicSubtreeData:    FileTypeSubtreeData,
	magicSubtreeMeta:    FileTypeSubtreeMeta,
	magicTx:             FileTypeTx,
	magicOutputs:        FileTypeOutputs,
	magicBloomFilter:    FileTypeBloomFilter,
	magicMsgBlock:       FileTypeMsgBlock,
	magicDat:            FileTypeDat,
	magicTesting:        FileTypeTesting,
	magicBatchData:      FileTypeBatchData,
	magicBatchKeys:      FileTypeBatchKeys,
	magicPreserveUntil:  FileTypePreserveUntil,
}

type Header struct {
	magic [8]byte
}

func NewHeader(fileType FileType) Header {
	if fileType == "" {
		panic("empty file type")
	}

	magic, found := fileTypeToMagic[fileType]
	if !found {
		panic(fmt.Sprintf("unknown file type %q", fileType))
	}

	return Header{
		magic: magic,
	}
}

func (h Header) Size() int {
	return 8
}

// IsUtxoHeadersV1 returns true if this is a version 1.0 utxo-headers file (without coinbase transactions).
func (h Header) IsUtxoHeadersV1() bool {
	return h.magic == magicUtxoHeaders
}

// IsUtxoHeadersV2 returns true if this is a version 2.0 utxo-headers file (with coinbase transactions).
func (h Header) IsUtxoHeadersV2() bool {
	return h.magic == magicUtxoHeadersV2
}

// Magic returns the magic bytes of this header for version checking.
func (h Header) Magic() [8]byte {
	return h.magic
}

func (h Header) Bytes() []byte {
	buf := new(bytes.Buffer)

	if err := h.Write(buf); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func (h Header) Write(w io.Writer) error {
	if _, err := w.Write(h.magic[:]); err != nil {
		// nolint: forbidigo
		return fmt.Errorf("error writing magic: %w", err)
	}

	return nil
}

func (h *Header) Read(r io.Reader) error {
	n, err := r.Read(h.magic[:])
	if err != nil {
		// nolint: forbidigo
		return fmt.Errorf("error reading magic: %w", err)
	}

	if n != 8 {
		// nolint: forbidigo
		return fmt.Errorf("expected to read 8 bytes, got %d", n)
	}

	// For backward compatibility, replace any trailing 0x00 with 0x20 (space)
	for i := 7; i >= 0; i-- {
		if h.magic[i] == 0 {
			h.magic[i] = ' '
		} else {
			break
		}
	}

	if _, ok := magicToFileType[h.magic]; !ok {
		// nolint: forbidigo
		return fmt.Errorf("unknown magic: %v", h.magic)
	}

	return nil
}

// ReadHeader reads a Header from the given reader and returns it.
func ReadHeader(r io.Reader) (Header, error) {
	var h Header

	if err := h.Read(r); err != nil {
		return Header{}, err
	}

	return h, nil
}

func ReadHeaderFromBytes(b []byte) (Header, error) {
	if len(b) < 8 {
		// nolint: forbidigo
		return Header{}, fmt.Errorf("not enough bytes to read header")
	}

	var header Header
	header.magic = [8]byte{
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
	}

	if _, ok := magicToFileType[header.magic]; !ok {
		// nolint: forbidigo
		return Header{}, fmt.Errorf("unknown magic: %v", header.magic)
	}

	return header, nil
}

func (h Header) FileType() FileType {
	return magicToFileType[h.magic]
}

func FileTypeFromExtension(ext string) (FileType, error) {
	if _, ok := fileTypeToMagic[FileType(ext)]; !ok {
		// nolint: forbidigo
		return "", fmt.Errorf("unknown file type: %s", ext)
	}

	return FileType(ext), nil
}
