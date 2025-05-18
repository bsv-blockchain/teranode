// Package spend provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package spend

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

// SpendingData contains information about a transaction that spends a UTXO.
type SpendingData struct {
	// TxID is the ID of the transaction that spent this UTXO
	TxID *chainhash.Hash `json:"txId"`

	// Vin is the input index in the spending transaction
	Vin int `json:"vin"`
}

// Clone creates a deep copy of the SpendingData.
func (sd *SpendingData) Clone() *SpendingData {
	if sd == nil {
		return nil
	}

	clone := &SpendingData{
		Vin: sd.Vin,
	}

	if sd.TxID != nil {
		clone.TxID = &chainhash.Hash{}
		copy(clone.TxID[:], sd.TxID[:])
	}

	return clone
}

// NewSpendingData creates a new SpendingData instance.
func NewSpendingData(txID *chainhash.Hash, vin int) *SpendingData {
	if txID == nil {
		return nil
	}

	return &SpendingData{
		TxID: txID,
		Vin:  vin,
	}
}

func NewSpendingDataFromString(s string) (*SpendingData, error) {
	// The first 64 characters are the txID
	// The next 8 characters are the vin
	if len(s) != 72 {
		// to avoid circular import...
		//nolint:forbidigo
		return nil, fmt.Errorf("invalid spending data string (expected 72 characters, got %d)", len(s))
	}

	txID, err := chainhash.NewHashFromStr(s[:64])
	if err != nil {
		return nil, err
	}

	vInBytes, err := hex.DecodeString(s[64:])
	if err != nil {
		return nil, err
	}

	// Vin is little endian
	vin := binary.LittleEndian.Uint32(vInBytes)

	return NewSpendingData(txID, int(vin)), nil
}

func (sd *SpendingData) String() string {
	if sd == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s[%d]", sd.TxID.String(), sd.Vin)
}

// Bytes serializes the SpendingData into a byte slice.
// The format is: [32 bytes txID][4 bytes vin]
func (sd *SpendingData) Bytes() []byte {
	if sd == nil {
		return nil
	}

	buf := make([]byte, 0, 32+4)

	if sd.TxID != nil {
		buf = append(buf, sd.TxID.CloneBytes()...)
	}

	// Create a 4-byte buffer for the Vin value
	uint32Bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(uint32Bytes, uint32(sd.Vin)) //nolint:gosec
	buf = append(buf, uint32Bytes...)

	return buf
}

// FromBytes deserializes a SpendingData from a byte slice.
// The format is: [32 bytes txID][4 bytes vin]
// Returns an error if the byte slice is invalid or too short.
func NewSpendingDataFromBytes(b []byte) (*SpendingData, error) {
	if len(b) < 36 { // 32 bytes for txID + 4 bytes for vin
		// to avoid circular import...
		//nolint:forbidigo
		return nil, fmt.Errorf("invalid byte length for SpendingData (expected 36, got %d)", len(b))
	}

	txID, err := chainhash.NewHash(b[:32])
	if err != nil {
		// to avoid circular import...
		//nolint:forbidigo
		return nil, fmt.Errorf("failed to parse txID in SpendingData: %w", err)
	}

	vin := binary.LittleEndian.Uint32(b[32:36])

	return &SpendingData{
		TxID: txID,
		Vin:  int(vin),
	}, nil
}
