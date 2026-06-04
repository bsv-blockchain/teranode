package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTxPolicyRejectedCache_SetAndGet(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	require.NoError(t, err)

	hash := *tx.TxIDChainHash()

	cache.Set(hash, tx)

	got, ok := cache.Get(hash)
	require.True(t, ok)
	assert.Equal(t, tx.TxID(), got.TxID())
}

func TestTxPolicyRejectedCache_MissReturnsNotFound(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	var fakeHash chainhash.Hash
	fakeHash[0] = 0xAB

	_, ok := cache.Get(fakeHash)
	require.False(t, ok)
}

func TestTxPolicyRejectedCache_EvictsWhenFull(t *testing.T) {
	cache := newTxPolicyRejectedCache(1) // very small -> maxSize = 1024 (minimum)
	cache.maxSize = 2                    // override for test

	tx1, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	tx2, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")

	var hash1, hash2, hash3 chainhash.Hash
	hash1[0] = 1
	hash2[0] = 2
	hash3[0] = 3

	cache.Set(hash1, tx1)
	cache.Set(hash2, tx2)
	require.Equal(t, 2, cache.Len())

	cache.Set(hash3, tx1)
	require.Equal(t, 2, cache.Len(), "cache should evict one entry to stay at max size")
}

func TestLookupPolicyRejectedTxs(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	require.NoError(t, err)

	var cachedHash chainhash.Hash
	cachedHash[0] = 0xAA
	cache.Set(cachedHash, tx)

	var missHash chainhash.Hash
	missHash[0] = 0xBB

	server := &Server{
		policyRejectedTxCache: cache,
	}

	toCheck := []missingTxHash{
		{hash: cachedHash, idx: 0},
		{hash: missHash, idx: 1},
	}

	found, still := server.lookupPolicyRejectedTxs(toCheck)

	require.Len(t, found, 1)
	assert.Equal(t, 0, found[0].idx)
	assert.Equal(t, tx.TxID(), found[0].tx.TxID())

	require.Len(t, still, 1)
	assert.Equal(t, missHash, still[0].hash)
	assert.Equal(t, 1, still[0].idx)
}

func TestLookupPolicyRejectedTxs_NilCache(t *testing.T) {
	server := &Server{
		policyRejectedTxCache: nil,
	}

	input := []missingTxHash{{hash: chainhash.Hash{}, idx: 0}}

	found, still := server.lookupPolicyRejectedTxs(input)
	assert.Nil(t, found)
	assert.Equal(t, input, still)
}

func TestPolicyRejectedTxMessageHandler(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	m := &kafkamessage.KafkaTxPolicyRejectedTopicMessage{
		TxHash: txHash.CloneBytes(),
		RawTx:  tx.SerializeBytes(),
		Reason: "insufficient fee",
	}

	value, err := proto.Marshal(m)
	require.NoError(t, err)

	server := &Server{
		policyRejectedTxCache: cache,
		logger:                ulogger.New("test"),
	}

	handler := server.policyRejectedTxMessageHandler(context.Background())

	err = handler(&kafka.KafkaMessage{
		Value: value,
	})
	require.NoError(t, err)

	got, ok := cache.Get(*txHash)
	require.True(t, ok)
	assert.Equal(t, tx.TxID(), got.TxID())
}

func TestPolicyRejectedTxMessageHandler_InvalidProto(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	server := &Server{
		policyRejectedTxCache: cache,
		logger:                ulogger.New("test"),
	}

	handler := server.policyRejectedTxMessageHandler(context.Background())

	err := handler(&kafka.KafkaMessage{
		Value: []byte("not-a-proto"),
	})
	require.NoError(t, err, "handler should swallow proto errors")
	assert.Equal(t, 0, cache.Len())
}

func TestPolicyRejectedTxMessageHandler_EmptyRawTx(t *testing.T) {
	cache := newTxPolicyRejectedCache(1024 * 1024)

	m := &kafkamessage.KafkaTxPolicyRejectedTopicMessage{
		TxHash: make([]byte, 32),
		RawTx:  nil,
		Reason: "test",
	}

	value, _ := proto.Marshal(m)

	server := &Server{
		policyRejectedTxCache: cache,
		logger:                ulogger.New("test"),
	}

	handler := server.policyRejectedTxMessageHandler(context.Background())

	err := handler(&kafka.KafkaMessage{Value: value})
	require.NoError(t, err)
	assert.Equal(t, 0, cache.Len(), "should skip messages with empty rawTx")
}
