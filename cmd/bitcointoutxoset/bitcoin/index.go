package bitcoin

import (
	"encoding/binary"
	"reflect"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

// FileIndex represents the structure of a file index in the Bitcoin index database.
type FileIndex struct {
	NBlocks      int `json:"nBlocks"`
	NHeightFirst int `json:"nHeightFirst"`
	NHeightLast  int `json:"nHeightLast"`
	NSize        int `json:"nSize"`
	NTimeFirst   int `json:"nTimeFirst"`
	NTimeLast    int `json:"nTimeLast"`
	NUndoSize    int `json:"nUndoSize"`
}

// IndexDB represents a read-only index database for Bitcoin blocks.
type IndexDB struct {
	db *leveldb.DB
}

// NewIndexDB creates a new IndexDB instance with the specified path.
// It opens a LevelDB database in read-only mode with no compression.
//
// Parameters:
//   - path: The file path to the LevelDB database.
//
// Returns:
//   - A pointer to an IndexDB instance if successful.
//   - An error if the database cannot be opened.
func NewIndexDB(path string) (*IndexDB, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.NoCompression,
		ReadOnly:    true,
	})
	if err != nil {
		return nil, err
	}

	return &IndexDB{
		db: db,
	}, nil
}

// Close closes the index database.
//
// Returns:
//   - An error if the database cannot be closed, otherwise nil.
func (in *IndexDB) Close() error {
	return in.db.Close()
}

// GetLastHeight retrieves the last height from the index database.
// It fetches the last file index and extracts the last block height.
//
// Returns:
//   - The last block height as an integer.
//   - An error if the last file index cannot be retrieved.
func (in *IndexDB) GetLastHeight() (int, error) {
	index, err := in.getLastFileIndex()
	if err != nil {
		return 0, err
	}

	return index.NHeightLast, nil
}

// getFileIndex retrieves the file index for a given file number from the database.
func (in *IndexDB) getFileIndex(file uint32) (*FileIndex, error) {
	bFile := IntToLittleEndianBytes(file)
	bFile = append([]byte{0x66}, bFile...)

	val, err := in.db.Get(bFile, nil)
	if err != nil {
		return nil, err
	}

	index := DeserializeFileIndex(val)

	return index, nil
}

// getLastFileIndex retrieves the last file index from the database.
func (in *IndexDB) getLastFileIndex() (*FileIndex, error) {
	val, err := in.db.Get([]byte{0x6c}, nil)
	if err != nil {
		return nil, err
	}

	var index *FileIndex

	index, err = in.getFileIndex(binary.LittleEndian.Uint32(val))
	if err != nil {
		return nil, err
	}

	return index, nil
}

// IntToLittleEndianBytes converts an integer to a byte slice in little-endian format.
// It supports various integer types, including unsigned and signed integers.
//
// Parameters:
//   - input: An integer value to be converted. Supported types include uint8, uint16, uint32, uint64, int32, and int64.
//
// Returns:
//   - A byte slice representing the input integer in little-endian format.
//
// Panics:
//   - If the input type is not supported, the function will panic with an error message.
func IntToLittleEndianBytes(input interface{}) []byte {
	switch t, v := reflect.TypeOf(input), reflect.ValueOf(input); t.Kind() {
	case reflect.Uint8:
		return []byte{v.Interface().(uint8)}
	case reflect.Uint16:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, v.Interface().(uint16))

		return buf
	case reflect.Uint32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, v.Interface().(uint32))

		return buf
	case reflect.Uint64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v.Interface().(uint64))

		return buf
	case reflect.Int32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(v.Interface().(int32)))

		return buf
	case reflect.Int64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v.Interface().(int64)))

		return buf
	default:
		panic(errors.New(9, "input type is not supported"))
	}
}

// DeserializeFileIndex deserializes a byte slice into a FileIndex struct.
// It extracts multiple variable-length integers from the input data to populate the fields of the FileIndex.
//
// Parameters:
//   - data: A byte slice containing the serialized FileIndex data.
//
// Returns:
//   - A pointer to a FileIndex struct populated with the deserialized values.
func DeserializeFileIndex(data []byte) *FileIndex {
	var pos int

	nBlocks, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nSize, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nUndoSize, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nHeightFirst, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nHeightLast, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nTimeFirst, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	nTimeLast, _ := DecodeVarIntForIndex(data[pos:])

	return &FileIndex{
		NBlocks:      nBlocks,
		NSize:        nSize,
		NUndoSize:    nUndoSize,
		NHeightFirst: nHeightFirst,
		NHeightLast:  nHeightLast,
		NTimeFirst:   nTimeFirst,
		NTimeLast:    nTimeLast,
	}
}

// DecodeVarIntForIndex decodes a variable-length integer from the input byte slice.
// It uses a continuation bit to determine whether more bytes are part of the integer.
//
// Parameters:
//   - input: A byte slice containing the encoded variable-length integer.
//
// Returns:
//   - n: The decoded integer value.
//   - pos: The number of bytes read from the input slice.
func DecodeVarIntForIndex(input []byte) (n int, pos int) {
	for {
		data := input[pos]
		pos += 1
		n = (n << 7) | int(data&0x7f)

		if data&0x80 == 0 {
			return n, pos
		}

		n += 1
	}
}
