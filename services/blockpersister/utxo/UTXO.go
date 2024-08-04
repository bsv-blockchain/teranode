package utxo

import (
	"fmt"
	"io"

	"github.com/libsv/go-bt/v2/chainhash"
)

type UTXO struct {
	TxID           *chainhash.Hash
	Index          uint32
	Value          uint64
	SpendingHeight uint32
	Script         []byte
}

/* Binary format is:
32 bytes - txID
4 bytes - index
8 bytes - value
4 bytes - spendingHeight
4 bytes - script length
<variable> - script
*/

func NewUTXOFromReader(r io.Reader) (*UTXO, error) {
	// Read all the fixed size fields
	b := make([]byte, 52)

	n, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}

	if n != 52 {
		return nil, io.ErrUnexpectedEOF
	}

	txID, err := chainhash.NewHash(b[:32])
	if err != nil {
		return nil, err
	}

	u := &UTXO{
		TxID:           txID,
		Index:          uint32(b[32]) | uint32(b[33])<<8 | uint32(b[34])<<16 | uint32(b[35])<<24,
		Value:          uint64(b[36]) | uint64(b[37])<<8 | uint64(b[38])<<16 | uint64(b[39])<<24 | uint64(b[40])<<32 | uint64(b[41])<<40 | uint64(b[42])<<48 | uint64(b[43])<<56,
		SpendingHeight: uint32(b[44]) | uint32(b[45])<<8 | uint32(b[46])<<16 | uint32(b[47])<<24,
	}

	// Read the script length
	l := uint32(b[48]) | uint32(b[49])<<8 | uint32(b[50])<<16 | uint32(b[51])<<24

	// Read the script
	u.Script = make([]byte, l)
	_, err = io.ReadFull(r, u.Script)
	if err != nil {
		return nil, err
	}

	return u, nil
}

func (u *UTXO) Bytes() []byte {
	b := make([]byte, 0, 32+4+8+4+4+len(u.Script))

	b = append(b, u.TxID[:]...)
	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))
	// Append little-endian value
	b = append(b, byte(u.Value), byte(u.Value>>8), byte(u.Value>>16), byte(u.Value>>24), byte(u.Value>>32), byte(u.Value>>40), byte(u.Value>>48), byte(u.Value>>56))
	// Append little-endian spendingHeight
	b = append(b, byte(u.SpendingHeight), byte(u.SpendingHeight>>8), byte(u.SpendingHeight>>16), byte(u.SpendingHeight>>24))

	// Append little-endian script length
	scriptLen := uint32(len(u.Script))
	b = append(b, byte(scriptLen), byte(scriptLen>>8), byte(scriptLen>>16), byte(scriptLen>>24))

	// Append script
	b = append(b, u.Script...)

	return b
}

func (u *UTXO) DeletionBytes() [36]byte {
	var b [36]byte

	copy(b[:], u.TxID[:])
	b[32] = byte(u.Index)
	b[33] = byte(u.Index >> 8)
	b[34] = byte(u.Index >> 16)
	b[35] = byte(u.Index >> 24)

	return b
}

func (u *UTXO) String() string {
	return fmt.Sprintf("%s:%d - %d (%d) - %x", u.TxID.String(), u.Index, u.Value, u.SpendingHeight, u.Script)
}
