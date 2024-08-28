package bitcoin

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord138(t *testing.T) {
	hashStr := "000000002c4f382be70290c5bf02dac879a73ab3e0e6f7ececbf7858ebd14500"

	b, err := hex.DecodeString("af93d20081cb40801d010083efd05df7986401000000ce5fc77cd8545040bd2b91cd363f4239f1f1e617b90cbcd490daa338000000006915b0d5b08e51b52984b2a3c6f9dfda643ab777f45d2552075884b45970da490fca864be5b3431c95fbd220875f003cb21695ed87f371b73befbe7317ee2f3ad5daf02f858e74f5fd6714e4d700000000000000")
	require.NoError(t, err)

	assert.Len(t, b, 138)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	// t.Logf("Height: %d", bi.Height)

	assert.Equal(t, uint32(42560), bi.Height)
	assert.Equal(t, hashStr, bi.BlockHeader.String())
}

func TestRecord89(t *testing.T) {
	hashStr := "000000000000000004d5034a3c27673ebcdfc51cd4ad7c44a21668bedc321400"

	b, err := hex.DecodeString("af949350a1c57b020000000020af7148cbd10eff33f8ceb37c4428e57a9a33553d14a0bd04000000000000000065ee2a3525933e0c8c0a983f8d1c02b80c208d34d56af07d648bc20173878cb99491445ca0240518013d1e83")
	require.NoError(t, err)

	assert.Len(t, b, 89)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	// t.Logf("Height: %d", bi.Height)

	assert.Equal(t, uint32(566139), bi.Height)
	assert.Equal(t, uint64(0), bi.TxCount)
	assert.Equal(t, hashStr, bi.BlockHeader.String())
}

func TestRecord141(t *testing.T) {
	hashStr := "0000000000000000169cdec8dcfa2e408f59e0d50b1a228f65d8f5480f990000"

	b, err := hex.DecodeString("af93d20093a702801d81600d83b3bfa15eceb3a51602000000fb759231e1fa5f80c3508e3a59ebf301930257d04aa492070000000000000000c11c6bc67af8264be7979db45043f5f5c1e8d2060082af4ce7957658a22147e30bf97f54747b1b187d1eac41788ec3b45e0355ee249249a7bf92ca0216f690585e53c92e76032bc82d20db6ed7f6020000000000")
	require.NoError(t, err)

	assert.Len(t, b, 141)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	// t.Logf("Height: %d", bi.Height)

	assert.Equal(t, uint32(332802), bi.Height)
	assert.Equal(t, hashStr, bi.BlockHeader.String())
}

func TestRecord137(t *testing.T) {
	hashStr := "00000000659e1402f1cdf3d2fd94065369e5cde43d6a00b6b5edcff01db50000"

	b, err := hex.DecodeString("af93d200c931801d01008085e93798990801000000d71b1aa9127ab8461b8242f171ee9b4700bc3e43b46452340819bfa9000000002b0213be2b6c3a42f870474bd4e20ac11381ab97168090303eff112fd0760760d52dd449ffff001d065603e5cc0326252ac99205d7f4e2b2d5ab9a44600b5b2b0b0bb32080ed5a1400b07649d800000000000000")
	require.NoError(t, err)

	assert.Len(t, b, 137)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	// t.Logf("Height: %d", bi.Height)

	assert.Equal(t, uint32(9521), bi.Height)
	assert.Equal(t, hashStr, bi.BlockHeader.String())
}

func TestRecord0000000000000000093d722ea52a23d599f1cacad9498dcab07d800cc4509054(t *testing.T) {
	hashStr := "0000000000000000093d722ea52a23d599f1cacad9498dcab07d800cc4509054"

	b, err := hex.DecodeString("af949350b3b962801d57ad7d85e6dcce3cfacd824e00e02332ac37fcce8ebbc48273c7782e3cd56dc63a3208eeabb443040000000000000000aee57815cf86c83c0df538b6557bcadd92d9bf830e5c1a9ba37032c825ee55fb40e3cd66924b0d18206119a089e31014c908b6913f6926207f8cbd07351d0be948c76a459f8d00ab9b24bcf58eac000000000000")
	require.NoError(t, err)

	assert.Len(t, b, 141)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	// t.Logf("Height: %d", bi.Height)

	assert.Equal(t, uint32(859490), bi.Height)
	assert.Equal(t, hashStr, bi.BlockHeader.String())
}
