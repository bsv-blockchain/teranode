package aerospike

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// payloadTestHashes builds n distinct hashes with asymmetric content and zero
// bytes at both ends, so a hex encoder that skips the display-order byte reversal
// or trims leading zeros cannot match chainhash.Hash.String().
func payloadTestHashes(n int) []*chainhash.Hash {
	hashes := make([]*chainhash.Hash, n)

	for i := range hashes {
		var h chainhash.Hash
		for j := range h {
			h[j] = byte(j*7 + i*13)
		}

		h[0] = 0
		h[chainhash.HashSize-1] = 0
		h[1] = byte(i)
		h[2] = byte(i >> 8)
		h[3] = byte(i >> 16)

		hashes[i] = &h
	}

	return hashes
}

// legacyAddDeletedChildrenPayload is the encoding the pruner sent before the
// hand-written encoder: the child hashes as hex strings, passed as the op's single
// argument through the generic msgpack encoder.
func legacyAddDeletedChildrenPayload(t *testing.T, children []*chainhash.Hash) []byte {
	t.Helper()

	hexHashes := make([]interface{}, 0, len(children))
	for _, h := range children {
		hexHashes = append(hexHashes, h.String())
	}

	payload, err := encodeNativeOpPayload(subOpAddDeletedChildren, []any{hexHashes})
	require.NoError(t, err)

	return payload
}

// TestEncodeAddDeletedChildrenPayload pins the hand-written addDeletedChildren
// payload to the bytes the generic encoder produced. The server decodes these
// bytes and the pruner deletes the children in the same batch, so a mismatch
// would lose the deletedChildren marker on every parent while the deletes still
// went through. The counts cross msgpack's array-header boundaries: fixarray
// below 16, array16 up to 65535, array32 above.
func TestEncodeAddDeletedChildrenPayload(t *testing.T) {
	for _, n := range []int{0, 1, 2, 15, 16, 17, 255, 256, 65535, 65536, 65537} {
		t.Run(fmt.Sprintf("%d children", n), func(t *testing.T) {
			children := payloadTestHashes(n)

			require.Equal(t, legacyAddDeletedChildrenPayload(t, children), encodeAddDeletedChildrenPayload(children))
		})
	}
}

// TestPrunerAddDeletedChildrenRecord covers both transports of the record the
// pruner's BuildAddDeletedChildrenRecord hook builds.
func TestPrunerAddDeletedChildrenRecord(t *testing.T) {
	key := prunerRecordTestKey(t)
	children := payloadTestHashes(3)

	t.Run("native store builds an UPDATE_ONLY BatchWrite", func(t *testing.T) {
		s := nativeStoreWithSharedPolicy(t)

		write, ok := s.prunerAddDeletedChildrenRecord(aerospike.NewBatchUDFPolicy(), key, children).(*aerospike.BatchWrite)
		require.True(t, ok, "native path must build a BatchWrite")
		require.Equal(t, aerospike.UPDATE_ONLY, write.Policy.RecordExistsAction, "addDeletedChildren must never create a missing parent")
		require.Len(t, write.Ops, 1)
	})

	t.Run("native store carries a caller filter onto a policy copy", func(t *testing.T) {
		s := nativeStoreWithSharedPolicy(t)

		policy := aerospike.NewBatchUDFPolicy()
		policy.FilterExpression = aerospike.ExpBinExists("txid")

		write, ok := s.prunerAddDeletedChildrenRecord(policy, key, children).(*aerospike.BatchWrite)
		require.True(t, ok)
		require.Equal(t, policy.FilterExpression, write.Policy.FilterExpression)
		require.Nil(t, s.nativeOpBatchWritePolicy.FilterExpression, "the shared policy must not be mutated")
	})

	// The UDF fallback must make exactly the call teranodeBatchRecord makes, so a
	// runtime demotion to the UDF path behaves as it did before.
	t.Run("UDF fallback matches teranodeBatchRecord", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}

		hexHashes := make([]interface{}, 0, len(children))
		for _, h := range children {
			hexHashes = append(hexHashes, h.String())
		}

		want, ok := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, key,
			subOpAddDeletedChildren, "addDeletedChildren", hexHashes).(*aerospike.BatchUDF)
		require.True(t, ok)

		got, ok := s.prunerAddDeletedChildrenRecord(aerospike.NewBatchUDFPolicy(), key, children).(*aerospike.BatchUDF)
		require.True(t, ok, "UDF fallback must build a NewBatchUDF")

		require.Equal(t, want.PackageName, got.PackageName)
		require.Equal(t, want.FunctionName, got.FunctionName)
		require.Equal(t, want.FunctionArgs, got.FunctionArgs)
	})
}

// TestHasResultCode covers the bare-error fast path and the chain walk it falls
// back to.
func TestHasResultCode(t *testing.T) {
	require.True(t, hasResultCode(aeroErr(types.KEY_NOT_FOUND_ERROR), types.KEY_NOT_FOUND_ERROR))
	require.False(t, hasResultCode(aeroErr(types.KEY_NOT_FOUND_ERROR), types.PARAMETER_ERROR))

	wrapped := fmt.Errorf("op failed: %w", aeroErr(types.PARAMETER_ERROR)) //nolint:forbidigo // std %w chain needed; errors.New* erases the aerospike type
	require.True(t, hasResultCode(wrapped, types.PARAMETER_ERROR))
	require.False(t, hasResultCode(wrapped, types.KEY_NOT_FOUND_ERROR))

	require.False(t, hasResultCode(nil, types.KEY_NOT_FOUND_ERROR))

	var typedNil *aerospike.AerospikeError
	require.False(t, hasResultCode(typedNil, types.KEY_NOT_FOUND_ERROR))
}
