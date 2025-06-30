# fileformat Package

This package provides the file format primitives for Teranode, the high-performance Bitcoin SV node implementation. It defines the structure and serialization logic for Teranode's on-disk files, especially the 8-byte magic header used to identify file types.

## Overview
Every Teranode file that uses this package begins with an **8-byte magic header** that uniquely identifies the file type. There is currently **no hash or block height** in the header, and there is **no footer implementation** in this package.

## Main Components

### Header
- **Purpose:** Identifies the file type for the file.
- **Structure:**
  - `magic` ([8]byte): File type magic string (e.g., `U-A-1.0 `, `B-1.0   `, etc.)
- **Key Methods:**
  - `Write(w io.Writer) error`: Serializes the header (8 bytes) to a writer.
  - `Read(r io.Reader) error`: Deserializes the header from a reader.
  - `FileType() FileType`: Returns the file type (as an enum value).
  - `NewHeader(fileType FileType) Header`: Constructs a header for the given file type.

## Supported File Types
The `FileType` enum defines supported file types, including:
- `utxo-additions`, `utxo-deletions`, `utxo-headers`, `utxo-set`, `block`, `subtree`, `subtreeToCheck`, `subtreeData`, `subtreeMeta`, `tx`, `outputs`, `bloomfilter`, `dat`, `msgBlock`, `testing`, `batch-data`, `batch-keys`

Each file type has a unique 8-byte magic header for identification.

## Example Usage
```go
import (
    "os"
    "github.com/bitcoin-sv/teranode/pkg/fileformat"
)

// Writing a header
header := fileformat.NewHeader(fileformat.FileTypeBlock)
f, _ := os.Create("block.dat")
defer f.Close()
header.Write(f)
```

## Testing
Unit tests are provided for header serialization/deserialization in the corresponding `_test.go` file. Run them with:

```
go test
```

## License
This package is part of Teranode and is released under the [Open BSV License](../../LICENSE).
