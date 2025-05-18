// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestSpendResponse_Bytes(t *testing.T) {
	txID, _ := chainhash.NewHashFromStr("8888888888888888888888888888888888888888888888888888888888888888")
	sr := &SpendResponse{
		Status:       1,
		SpendingData: spend.NewSpendingData(txID, 1),
		LockTime:     123456,
	}

	expected := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)

	expected = append(expected, spend.NewSpendingData(txID, 1).Bytes()...)

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
	txID, _ := chainhash.NewHashFromStr("8888888888888888888888888888888888888888888888888888888888888888")

	data := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)
	data = append(data, spend.NewSpendingData(txID, 1).Bytes()...)

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.NoError(t, err)
	assert.Equal(t, 1, sr.Status)
	assert.Equal(t, uint32(123456), sr.LockTime)
	assert.Equal(t, txID, sr.SpendingData.TxID)
	assert.Equal(t, 1, sr.SpendingData.Vin)
}

func TestSpendResponse_FromBytes_NoSpendingData(t *testing.T) {
	data := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, []byte{64, 226, 1, 0}...)

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.NoError(t, err)
	assert.Equal(t, 1, sr.Status)
	assert.Equal(t, uint32(123456), sr.LockTime)
	assert.Nil(t, sr.SpendingData)
}

func TestSpendResponse_FromBytes_InvalidLength(t *testing.T) {
	data := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	sr := &SpendResponse{}
	err := sr.FromBytes(data)

	assert.Error(t, err)
	assert.Equal(t, "INVALID_ARGUMENT (1): invalid byte length", err.Error())
}
