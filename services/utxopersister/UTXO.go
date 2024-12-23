package utxopersister

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

var EOFMarker = make([]byte, 32) // 32 zero bytes

type UTXOWrapper struct {
	TxID     chainhash.Hash
	Height   uint32
	Coinbase bool
	UTXOs    []*UTXO
}

type UTXO struct {
	Index  uint32
	Value  uint64
	Script []byte
}

func (uw *UTXOWrapper) Bytes() []byte {
	size := 32 + 4 + 4 // TXID + encoded height / coinbase + len(UTXOs)
	for _, u := range uw.UTXOs {
		size += 4 + 8 + 4 + len(u.Script) // index + value + script length + script
	}

	b := make([]byte, 0, size)

	b = append(b, uw.TxID[:]...)

	// To store the height and coinbase flag in a single uint32:
	// 1.	Shift the height left by 1 bit to leave space for the flag.
	// 2.	Set the flag as the least significant bit.

	var flag uint32
	if uw.Coinbase {
		flag = 1
	}

	encodedValue := (uw.Height << 1) | flag

	// Append the encoded height/coinbase
	b = append(b, byte(encodedValue), byte(encodedValue>>8), byte(encodedValue>>16), byte(encodedValue>>24))

	// Append the number of UTXOs
	b = append(b, byte(len(uw.UTXOs)), byte(len(uw.UTXOs)>>8), byte(len(uw.UTXOs)>>16), byte(len(uw.UTXOs)>>24))

	for _, u := range uw.UTXOs {
		b = append(b, u.Bytes()...)
	}

	return b
}

func (uw *UTXOWrapper) DeletionBytes(index uint32) [36]byte {
	var b [36]byte

	copy(b[:], uw.TxID[:])
	b[32] = byte(index)
	b[33] = byte(index >> 8)
	b[34] = byte(index >> 16)
	b[35] = byte(index >> 24)

	return b
}

func NewUTXOWrapperFromReader(ctx context.Context, r io.Reader) (*UTXOWrapper, error) {
	uw := &UTXOWrapper{}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if n, err := io.ReadFull(r, uw.TxID[:]); err != nil || n != 32 {
			return nil, errors.NewStorageError("failed to read txid, expected 32 bytes got %d", n, err)
		}

		// Check if all the bytes are zero
		if bytes.Equal(uw.TxID[:], EOFMarker) {
			// We return an empty UTXOWrapper and io.EOF to signal the end of the stream
			// The empty UTXOWrapper indicates an EOF where the eofMarker was written
			return &UTXOWrapper{}, io.EOF
		}

		// Read the encoded height/coinbase + number of UTXOs
		var b [8]byte
		if n, err := io.ReadFull(r, b[:]); err != nil || n != 8 {
			return nil, errors.NewStorageError("failed to read height and number of utxos, expected 8 bytes got %d", n, err)
		}

		encodedHeight := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		numUTXOs := uint32(b[4]) | uint32(b[5])<<8 | uint32(b[6])<<16 | uint32(b[7])<<24

		uw.Height = encodedHeight >> 1
		uw.Coinbase = (encodedHeight & 1) == 1
		uw.UTXOs = make([]*UTXO, numUTXOs)

		var err error

		for i := uint32(0); i < numUTXOs; i++ {
			if uw.UTXOs[i], err = NewUTXOFromReader(r); err != nil {
				return nil, err
			}
		}
	}

	return uw, nil
}

func NewUTXOWrapperFromBytes(b []byte) (*UTXOWrapper, error) {
	return NewUTXOWrapperFromReader(context.Background(), bytes.NewReader(b))
}

func (uw *UTXOWrapper) String() string {
	s := strings.Builder{}

	if uw.Coinbase {
		s.WriteString(fmt.Sprintf("%s - (height %d coinbase) - %d output(s):\n", uw.TxID.String(), uw.Height, len(uw.UTXOs)))
	} else {
		s.WriteString(fmt.Sprintf("%s - (height %d) - %d output(s):\n", uw.TxID.String(), uw.Height, len(uw.UTXOs)))
	}

	for _, u := range uw.UTXOs {
		s.WriteString(fmt.Sprintf("\t%v\n", u))
	}

	return s.String()
}

func NewUTXOFromReader(r io.Reader) (*UTXO, error) {
	// Read all the fixed size fields
	var b [16]byte // index + value + length of script

	if _, err := io.ReadFull(r, b[:]); err != nil {
		return nil, err
	}

	u := &UTXO{
		Index: uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24,
		Value: uint64(b[4]) | uint64(b[5])<<8 | uint64(b[6])<<16 | uint64(b[7])<<24 | uint64(b[8])<<32 | uint64(b[9])<<40 | uint64(b[10])<<48 | uint64(b[11])<<56,
	}

	// Read the script length
	l := uint32(b[12]) | uint32(b[13])<<8 | uint32(b[14])<<16 | uint32(b[15])<<24

	// Read the script
	u.Script = make([]byte, l)

	if _, err := io.ReadFull(r, u.Script); err != nil {
		return nil, err
	}

	return u, nil
}

// func NewUTXOFromBytes(b []byte) (*UTXO, error) {
// 	return NewUTXOFromReader(bytes.NewReader(b))
// }

func (u *UTXO) Bytes() []byte {
	b := make([]byte, 0, 4+8+4+len(u.Script)) // index + value + length of script + script

	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))
	// Append little-endian value
	b = append(b, byte(u.Value), byte(u.Value>>8), byte(u.Value>>16), byte(u.Value>>24), byte(u.Value>>32), byte(u.Value>>40), byte(u.Value>>48), byte(u.Value>>56))

	// Append little-endian script length
	// nolint: gosec
	scriptLen := uint32(len(u.Script))
	b = append(b, byte(scriptLen), byte(scriptLen>>8), byte(scriptLen>>16), byte(scriptLen>>24))

	// Append script
	b = append(b, u.Script...)

	return b
}

func (u *UTXO) String() string {
	return fmt.Sprintf("%d: %d - %x", u.Index, u.Value, u.Script)
}
