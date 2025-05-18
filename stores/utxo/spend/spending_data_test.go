package spend

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSpendingDataFromString(t *testing.T) {
	s := "38d2f49c6d46a43ed554ed24891265500ee2483978cace68c1cf429aa30cff3701000000"

	spendingData, err := NewSpendingDataFromString(s)
	require.NoError(t, err)

	assert.Equal(t, "38d2f49c6d46a43ed554ed24891265500ee2483978cace68c1cf429aa30cff37", spendingData.TxID.String())
	assert.Equal(t, 1, spendingData.Vin)
}

func TestLittleEndian(t *testing.T) {
	vin := uint32(1)
	vinBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(vinBytes, vin)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00}, vinBytes)

	vinStr := hex.EncodeToString(vinBytes)
	assert.Equal(t, "01000000", vinStr)

	vin2 := binary.LittleEndian.Uint32(vinBytes)
	assert.Equal(t, uint32(1), vin2)
}
