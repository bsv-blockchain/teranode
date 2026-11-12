package subtreevalidation

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// TestServer_txmetaHandler_CacheDoesNotRetainMessageBuffer pins the contract
// that lets the handler pass sub-slices of msg.Value to the cache without
// copying them first: Kafka may recycle msg.Value once the handler returns, so
// every backend must have copied the key and value by then.
func TestServer_txmetaHandler_CacheDoesNotRetainMessageBuffer(t *testing.T) {
	for _, bucket := range []txmetacache.BucketType{txmetacache.Unallocated, txmetacache.Pointer, txmetacache.Lockfree} {
		t.Run(bucketName(bucket), func(t *testing.T) {
			testCacheBucket = bucket

			t.Cleanup(func() { testCacheBucket = txmetacache.Unallocated })

			cache := setupTestCache(t)
			server := &Server{logger: ulogger.TestLogger{}, utxoStore: cache}

			hash := chainhash.HashH([]byte("buffer-reuse"))
			want := &meta.Data{
				Fee:         1234,
				SizeInBytes: 321,
				TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte("parent"))}, []uint32{1, 7}),
			}

			payload, err := want.MetaBytes()
			require.NoError(t, err)

			msg := &kafka.KafkaMessage{Value: buildV1TxmetaMessage(t, 1, hash, payload)}
			require.NoError(t, server.txmetaHandler(context.Background(), msg))

			// Kafka reuses the buffer.
			for i := range msg.Value {
				msg.Value[i] = 0xAA
			}

			got, ok := cache.GetMetaCached(context.Background(), hash)
			require.True(t, ok, "the entry must have been cached under the original key")
			require.Equal(t, want.Fee, got.Fee)
			require.Equal(t, want.SizeInBytes, got.SizeInBytes)
			require.Equal(t, want.TxInpoints.ParentTxHashes, got.TxInpoints.ParentTxHashes)
		})
	}
}

func bucketName(b txmetacache.BucketType) string {
	switch b {
	case txmetacache.Pointer:
		return "pointer"
	case txmetacache.Lockfree:
		return "lockfree"
	case txmetacache.Native:
		return "native"
	default:
		return "unallocated"
	}
}

// TestServer_txmetaHandler_DeleteEvictsOnEveryBackend covers the DELETEs block
// assembly publishes for transactions it demotes to conflicting (v1 wire,
// [count][32 hash][DELETE][0 length] per entry): after one, the entry must be
// gone, or a block containing the demoted tx would pass the cached-meta fast
// path.
func TestServer_txmetaHandler_DeleteEvictsOnEveryBackend(t *testing.T) {
	for _, bucket := range []txmetacache.BucketType{txmetacache.Unallocated, txmetacache.Pointer, txmetacache.Lockfree} {
		t.Run(bucketName(bucket), func(t *testing.T) {
			testCacheBucket = bucket

			t.Cleanup(func() { testCacheBucket = txmetacache.Unallocated })

			cache := setupTestCache(t)
			server := &Server{logger: ulogger.TestLogger{}, utxoStore: cache}

			demoted := chainhash.HashH([]byte("demoted"))
			kept := chainhash.HashH([]byte("kept"))

			payload, err := (&meta.Data{
				Fee:         1,
				SizeInBytes: 2,
				TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte("p"))}, []uint32{1, 0}),
			}).MetaBytes()
			require.NoError(t, err)

			for _, h := range []chainhash.Hash{demoted, kept} {
				msg := &kafka.KafkaMessage{Value: buildV1TxmetaMessage(t, 1, h, payload)}
				require.NoError(t, server.txmetaHandler(context.Background(), msg))
			}

			del := make([]byte, 4, 4+txmetacache.WireV1MinEntrySize)
			binary.LittleEndian.PutUint32(del, 1)
			del = append(del, demoted[:]...)
			del = append(del, txmetacache.WireActionDELETE, 0, 0, 0, 0)

			require.NoError(t, server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: del}))

			_, ok := cache.GetMetaCached(context.Background(), demoted)
			require.False(t, ok, "a DELETEd entry must not be served")

			_, ok = cache.GetMetaCached(context.Background(), kept)
			require.True(t, ok, "a DELETE must not evict other entries")
		})
	}
}

// TestServer_txmetaHandler_V2MultiEntryDoesNotRetainBuffer is the buffer-reuse
// contract for a multi-entry v2 message: every entry is handed to the cache as
// a sub-slice of msg.Value (with its wire xxhash), and all of them must
// survive the buffer being overwritten.
func TestServer_txmetaHandler_V2MultiEntryDoesNotRetainBuffer(t *testing.T) {
	for _, bucket := range []txmetacache.BucketType{txmetacache.Unallocated, txmetacache.Native, txmetacache.Pointer, txmetacache.Lockfree} {
		t.Run(bucketName(bucket), func(t *testing.T) {
			testCacheBucket = bucket

			t.Cleanup(func() { testCacheBucket = txmetacache.Unallocated })

			cache := setupTestCache(t)
			server := &Server{logger: ulogger.TestLogger{}, utxoStore: cache}

			const n = 5

			hashes := make([]chainhash.Hash, n)
			msg := []byte{txmetacache.WireV2Magic, txmetacache.WireV2Version, 0, 0}
			msg = binary.LittleEndian.AppendUint32(msg, n)

			for i := range hashes {
				hashes[i] = chainhash.HashH([]byte{byte(i), 'v', '2'})

				payload, err := (&meta.Data{
					Fee:         uint64(100 + i),
					SizeInBytes: 250,
					TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte{byte(i), 'p'})}, []uint32{1, uint32(i)}),
				}).MetaBytes()
				require.NoError(t, err)

				msg = binary.LittleEndian.AppendUint64(msg, txmetacache.KeyHash(&hashes[i]))
				msg = append(msg, hashes[i][:]...)
				msg = append(msg, txmetacache.WireActionADD)
				msg = binary.LittleEndian.AppendUint32(msg, uint32(len(payload))) //nolint:gosec // small
				msg = append(msg, payload...)
			}

			km := &kafka.KafkaMessage{Value: msg}
			require.NoError(t, server.txmetaHandler(context.Background(), km))

			for i := range km.Value {
				km.Value[i] = 0xAA
			}

			for i := range hashes {
				got, ok := cache.GetMetaCached(context.Background(), hashes[i])
				require.True(t, ok, "entry %d", i)
				require.Equal(t, uint64(100+i), got.Fee, "entry %d", i)
			}
		})
	}
}
