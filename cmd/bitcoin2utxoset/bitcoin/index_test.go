package bitcoin

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	b, err := hex.DecodeString("af93d20081cb40801d010083efd05df7986401000000ce5fc77cd8545040bd2b91cd363f4239f1f1e617b90cbcd490daa338000000006915b0d5b08e51b52984b2a3c6f9dfda643ab777f45d2552075884b45970da490fca864be5b3431c95fbd220875f003cb21695ed87f371b73befbe7317ee2f3ad5daf02f858e74f5fd6714e4d700000000000000")
	require.NoError(t, err)

	bi, err := DeserializeBlockIndex(b)
	require.NoError(t, err)

	assert.Equal(t, uint32(42560), bi.Height)
	assert.Equal(t, "000000002c4f382be70290c5bf02dac879a73ab3e0e6f7ececbf7858ebd14500", bi.BlockHeader.String())
}
