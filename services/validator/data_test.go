package validator

import (
	"testing"

	"github.com/libsv/go-p2p/test"
	"github.com/stretchr/testify/require"
)

func Test_NewTxValidationData(t *testing.T) {
	t.Run("NewTxValidationData defaults", func(t *testing.T) {
		tx := test.TX1RawBytes
		height := uint32(478558)
		options := NewDefaultOptions()

		data := NewTxValidationData(tx, height, options)

		dataBytes := data.Bytes()

		data2, err := NewTxValidationDataFromBytes(dataBytes)
		require.NoError(t, err)

		require.Equal(t, data.Height, data2.Height)
		require.Equal(t, data.Tx, data2.Tx)
		require.Equal(t, data.Options.skipUtxoCreation, data2.Options.skipUtxoCreation)
		require.Equal(t, data.Options.addTXToBlockAssembly, data2.Options.addTXToBlockAssembly)
		require.Equal(t, data.Options.skipPolicyChecks, data2.Options.skipPolicyChecks)
	})

	t.Run("NewTxValidationData", func(t *testing.T) {
		tx := test.TX1RawBytes
		height := uint32(478558)
		options := &Options{skipUtxoCreation: true, addTXToBlockAssembly: false, skipPolicyChecks: true}

		data := NewTxValidationData(tx, height, options)

		dataBytes := data.Bytes()

		data2, err := NewTxValidationDataFromBytes(dataBytes)
		require.NoError(t, err)

		require.Equal(t, data.Height, data2.Height)
		require.Equal(t, data.Tx, data2.Tx)
		require.Equal(t, data.Options.skipUtxoCreation, data2.Options.skipUtxoCreation)
		require.Equal(t, data.Options.addTXToBlockAssembly, data2.Options.addTXToBlockAssembly)
		require.Equal(t, data.Options.skipPolicyChecks, data2.Options.skipPolicyChecks)
	})

	t.Run("too short", func(t *testing.T) {
		_, err := NewTxValidationDataFromBytes([]byte{0x00, 0x00, 0x00})
		require.Error(t, err)
	})
}
