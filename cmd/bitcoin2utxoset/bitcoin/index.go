package bitcoin

import (
	"encoding/binary"
	"errors"
	"reflect"

	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

type FileIndex struct {
	NBlocks      int `json:"nBlocks"`
	NSize        int `json:"nSize"`
	NUndoSize    int `json:"nUndoSize"`
	NHeightFirst int `json:"nHeightFirst"`
	NHeightLast  int `json:"nHeightLast"`
	NTimeFirst   int `json:"nTimeFirst"`
	NTimeLast    int `json:"nTimeLast"`
}

type IndexDB struct {
	db *leveldb.DB
}

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

func (in *IndexDB) Close() error {
	return in.db.Close()
}

func (in *IndexDB) GetLastHeight() (int, error) {
	index, err := in.getLastFileIndex()
	if err != nil {
		return 0, err
	}

	return index.NHeightLast, nil
}

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

func (in *IndexDB) getLastFileIndex() (*FileIndex, error) {
	val, err := in.db.Get([]byte{0x6c}, nil)
	if err != nil {
		return nil, err
	}

	index, err := in.getFileIndex(binary.LittleEndian.Uint32(val))
	if err != nil {
		return nil, err
	}

	return index, nil
}

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
		panic(errors.New("input type is not supported"))
	}
}

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
