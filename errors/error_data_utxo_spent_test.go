package errors

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestUtxoSpentErrData_SetData tests the SetData method of the UtxoSpentErrData type.
func TestUtxoSpentErrData_SetData(t *testing.T) {
	hash := chainhash.Hash{0x01, 0x02, 0x03}
	utxoHash := chainhash.Hash{0x0A, 0x0B, 0x0C}
	vout := uint32(7)

	txID := &chainhash.Hash{0xAA, 0xBB, 0xCC}
	spendingData := &spendpkg.SpendingData{
		TxID: txID,
		Vin:  1,
	}

	t.Run("set hash", func(t *testing.T) {
		var errData UtxoSpentErrData

		errData.SetData("hash", hash)
		require.Equal(t, hash, errData.Hash)
	})

	t.Run("set vout", func(t *testing.T) {
		var errData UtxoSpentErrData

		errData.SetData("vout", vout)
		require.Equal(t, vout, errData.Vout)
	})

	t.Run("set utxo_hash", func(t *testing.T) {
		var errData UtxoSpentErrData

		errData.SetData("utxo_hash", utxoHash)
		require.Equal(t, utxoHash, errData.UtxoHash)
	})

	t.Run("set spending_data", func(t *testing.T) {
		var errData UtxoSpentErrData

		errData.SetData("spending_data", spendingData)
		require.Equal(t, spendingData, errData.SpendingData)
		require.Equal(t, txID, errData.SpendingData.TxID)
		require.Equal(t, 1, errData.SpendingData.Vin)
	})

	t.Run("unknown key does not panic", func(t *testing.T) {
		var errData UtxoSpentErrData

		require.NotPanics(t, func() {
			errData.SetData("nonexistent", "value")
		})
	})
}

// TestUtxoSpentErrData_GetData tests the GetData method of the UtxoSpentErrData type.
func TestUtxoSpentErrData_GetData(t *testing.T) {
	hash := chainhash.Hash{0x01}
	utxoHash := chainhash.Hash{0x02}
	vout := uint32(42)
	txID := &chainhash.Hash{0xAA}

	spendingData := &spendpkg.SpendingData{
		TxID: txID,
		Vin:  5,
	}

	errData := UtxoSpentErrData{
		Hash:         hash,
		UtxoHash:     utxoHash,
		Vout:         vout,
		SpendingData: spendingData,
	}

	t.Run("get hash", func(t *testing.T) {
		val := errData.GetData("hash")
		require.IsType(t, chainhash.Hash{}, val)
		require.Equal(t, hash, val)
	})

	t.Run("get utxo_hash", func(t *testing.T) {
		val := errData.GetData("utxo_hash")
		require.IsType(t, chainhash.Hash{}, val)
		require.Equal(t, utxoHash, val)
	})

	t.Run("get vout", func(t *testing.T) {
		val := errData.GetData("vout")
		require.IsType(t, uint32(0), val)
		require.Equal(t, vout, val)
	})

	t.Run("get spending_data", func(t *testing.T) {
		val := errData.GetData("spending_data")
		require.IsType(t, &spendpkg.SpendingData{}, val)
		require.Equal(t, spendingData, val)
		require.Equal(t, txID, val.(*spendpkg.SpendingData).TxID)
		require.Equal(t, 5, val.(*spendpkg.SpendingData).Vin)
	})

	t.Run("get unknown key returns nil", func(t *testing.T) {
		val := errData.GetData("unknown_key")
		require.Nil(t, val)
	})
}

// TestUtxoSpentErrData_EncodeErrorData tests the EncodeErrorData method of the UtxoSpentErrData type.
func TestUtxoSpentErrData_EncodeErrorData(t *testing.T) {
	t.Run("encode with full data", func(t *testing.T) {
		hash := chainhash.Hash{0x01}
		utxoHash := chainhash.Hash{0x02}
		vout := uint32(42)
		txID := &chainhash.Hash{0xAA}

		spendingData := &spendpkg.SpendingData{
			TxID: txID,
			Vin:  3,
		}

		errData := &UtxoSpentErrData{
			Hash:         hash,
			UtxoHash:     utxoHash,
			Vout:         vout,
			SpendingData: spendingData,
		}

		bytes := errData.EncodeErrorData()
		require.NotEmpty(t, bytes, "encoded JSON should not be empty")

		var decoded UtxoSpentErrData
		err := json.Unmarshal(bytes, &decoded)
		require.NoError(t, err, "unmarshal of encoded data should succeed")
		require.Equal(t, errData.Hash, decoded.Hash)
		require.Equal(t, errData.UtxoHash, decoded.UtxoHash)
		require.Equal(t, errData.Vout, decoded.Vout)
		require.NotNil(t, decoded.SpendingData)
		require.Equal(t, spendingData.Vin, decoded.SpendingData.Vin)
		require.Equal(t, spendingData.TxID, decoded.SpendingData.TxID)
	})

	t.Run("encode with empty data", func(t *testing.T) {
		errData := &UtxoSpentErrData{}
		bytes := errData.EncodeErrorData()
		require.NotEmpty(t, bytes, "even empty struct should encode to valid JSON")

		var decoded map[string]any
		err := json.Unmarshal(bytes, &decoded)
		require.NoError(t, err, "unmarshal of empty-struct JSON should succeed")
		require.Contains(t, decoded, "hash")
		require.Contains(t, decoded, "utxo_hash")
		require.Contains(t, decoded, "vout")
		require.Contains(t, decoded, "spending_data")
	})
}

// TestNewUtxoSpentError tests the NewUtxoSpentError function for creating a UTXO spent error.
func TestNewUtxoSpentError(t *testing.T) {
	t.Run("constructs error with full input", func(t *testing.T) {
		txID := chainhash.Hash{0xAA}
		utxoHash := chainhash.Hash{0xBB}
		vout := uint32(5)

		spendingTxID := &chainhash.Hash{0xCC}
		spendingData := &spendpkg.SpendingData{
			TxID: spendingTxID,
			Vin:  1,
		}

		err := NewUtxoSpentError(txID, vout, utxoHash, spendingData)

		require.NotNil(t, err)
		require.Equal(t, ERR_UTXO_SPENT, err.Code())
		require.Contains(t, err.Error(), txID.String())
		require.Contains(t, err.Error(), "utxo already spent")
		require.Contains(t, err.Error(), "5") // vout

		var data *UtxoSpentErrData
		ok := errors.As(err.Data(), &data)
		require.True(t, ok)
		require.Equal(t, txID, data.Hash)
		require.Equal(t, vout, data.Vout)
		require.Equal(t, utxoHash, data.UtxoHash)
		require.Equal(t, spendingData, data.SpendingData)
		require.Equal(t, spendingTxID, data.SpendingData.TxID)
		require.Equal(t, 1, data.SpendingData.Vin)
	})

	t.Run("handles nil spendingData", func(t *testing.T) {
		txID := chainhash.Hash{0xDD}
		utxoHash := chainhash.Hash{0xEE}
		vout := uint32(9)

		err := NewUtxoSpentError(txID, vout, utxoHash, nil)

		require.NotNil(t, err)
		require.Equal(t, ERR_UTXO_SPENT, err.Code())
		require.Contains(t, err.Error(), "utxo already spent")
		require.Contains(t, err.Error(), txID.String())

		var data *UtxoSpentErrData
		ok := errors.As(err.Data(), &data)
		require.True(t, ok)
		require.Equal(t, txID, data.Hash)
		require.Equal(t, vout, data.Vout)
		require.Equal(t, utxoHash, data.UtxoHash)
		require.Nil(t, data.SpendingData)
	})
}

// TestUtxoSpentErrorMessageCarriesNoCodeRendering pins the producer, not the
// consumer.
//
// This message used to open with errCodeMsgFmt ("UTXO_SPENT (70): "), which
// Error() and UserMessage already prepend - so the rendering was duplicated, and
// worse, the code became part of Message() rather than around it. The JSON-RPC
// boundary reads Message() precisely because it never contains a code, so an
// ordinary double-spend answered "TX rejected: UTXO_SPENT (70): ...".
//
// The boundary now also strips a leading code rendering defensively, which means
// a regression here is invisible from that side. This test is what makes it
// visible: it is the only thing that fails if the prefix comes back.
func TestUtxoSpentErrorMessageCarriesNoCodeRendering(t *testing.T) {
	err := NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)

	require.NotContains(t, err.Message(), "UTXO_SPENT",
		"the code name belongs around the message, not inside it")
	require.NotContains(t, err.Message(), "(70)")
	require.Contains(t, err.Message(), "utxo already spent by tx")

	// Error() on this data-carrying type renders the code numerically ("70: ")
	// rather than via errCodeMsgFmt, so the name appears nowhere at all once the
	// message stops carrying it. Checked rather than assumed: an earlier version
	// of this test asserted the name appeared exactly once and was wrong.
	require.NotContains(t, err.Error(), "UTXO_SPENT")
	require.Contains(t, err.Error(), "70: ")
}
