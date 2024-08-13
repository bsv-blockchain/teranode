package utxopersister

import (
	"fmt"
	"io"

	"github.com/libsv/go-bt/v2/chainhash"
)

type UTXODeletion struct {
	TxID  *chainhash.Hash
	Index uint32
}

/* Binary format is:
32 bytes - txID
4 bytes - index
*/

func NewUTXODeletionFromReader(r io.Reader) (*UTXODeletion, error) {
	// Read all the fixed size fields
	b := make([]byte, 32+4)

	n, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}

	if n != 32+4 {
		return nil, io.ErrUnexpectedEOF
	}

	txID, err := chainhash.NewHash(b[:32])
	if err != nil {
		return nil, err
	}

	u := &UTXODeletion{
		TxID:  txID,
		Index: uint32(b[32]) | uint32(b[33])<<8 | uint32(b[34])<<16 | uint32(b[35])<<24,
	}

	return u, nil
}

func (u *UTXODeletion) DeletionBytes() []byte {
	b := make([]byte, 0, 32+4)

	b = append(b, u.TxID[:]...)
	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))

	return b
}

func (u *UTXODeletion) String() string {
	return fmt.Sprintf("%s:%d", u.TxID.String(), u.Index)
}
