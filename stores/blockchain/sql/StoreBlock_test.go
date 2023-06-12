package sql

import (
	"encoding/hex"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getCumulativeChainWork(t *testing.T) {
	t.Run("block 1", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		require.NoError(t, err)

		nBits, _ := hex.DecodeString("1d00ffff")

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &bc.BlockHeader{
				Bits: nBits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000100010001", chainWork.String())
	})

	t.Run("block 2", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000100010001")
		require.NoError(t, err)

		nBits, _ := hex.DecodeString("1d00ffff")

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &bc.BlockHeader{
				Bits: nBits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000200020002", chainWork.String())
	})

	t.Run("block 796044", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000001473b8614ab22c164d42204")
		require.NoError(t, err)

		nBits, _ := hex.DecodeString("1810b7f0")

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &bc.BlockHeader{
				Bits: nBits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "000000000000000000000000000000000000000001473b9564a2d255e87e7e86", chainWork.String())
	})
}
