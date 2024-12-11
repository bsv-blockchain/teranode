package aerospike

import (
	"encoding/binary"
	"testing"

	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestCalculateKeySource(t *testing.T) {
	hash := chainhash.HashH([]byte("test"))

	h := uaerospike.CalculateKeySource(&hash, 0)
	assert.Equal(t, hash[:], h)

	h = uaerospike.CalculateKeySource(&hash, 1)
	extra := make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(1))
	assert.Equal(t, append(hash[:], extra...), h)

	h = uaerospike.CalculateKeySource(&hash, 2)
	extra = make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(2))
	assert.Equal(t, append(hash[:], extra...), h)
}

func TestCalculateOffsetOutput(t *testing.T) {
	db := &Store{utxoBatchSize: 20_000}

	offset := db.calculateOffsetForOutput(0)
	assert.Equal(t, uint32(0), offset)

	offset = db.calculateOffsetForOutput(30_000)
	assert.Equal(t, uint32(10_000), offset)

	offset = db.calculateOffsetForOutput(40_000)
	assert.Equal(t, uint32(0), offset)
}
