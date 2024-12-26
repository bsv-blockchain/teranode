// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestSpendResponse_Bytes(t *testing.T) {
	txID, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	sr := &SpendResponse{
		Status:       1,
		SpendingTxID: txID,
		LockTime:     123456,
	}

	expected := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)
	expected = append(expected, txID.CloneBytes()...)

	assert.Equal(t, expected, sr.Bytes())
}

func TestSpendResponse_Bytes_NoSpendingTxID(t *testing.T) {
	sr := &SpendResponse{
		Status:   1,
		LockTime: 123456,
	}

	expected := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)

	assert.Equal(t, expected, sr.Bytes())
}

func TestSpendResponse_FromBytes(t *testing.T) {
	txID, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")

	data := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)
	data = append(data, txID.CloneBytes()...)

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.NoError(t, err)
	assert.Equal(t, 1, sr.Status)
	assert.Equal(t, uint32(123456), sr.LockTime)
	assert.Equal(t, txID, sr.SpendingTxID)
}

func TestSpendResponse_FromBytes_NoSpendingTxID(t *testing.T) {
	data := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.NoError(t, err)
	assert.Equal(t, 1, sr.Status)
	assert.Equal(t, uint32(123456), sr.LockTime)
	assert.Nil(t, sr.SpendingTxID)
}

func TestSpendResponse_FromBytes_InvalidLength(t *testing.T) {
	data := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.Error(t, err)
	assert.Equal(t, "INVALID_ARGUMENT (1): invalid byte length", err.Error())
}
