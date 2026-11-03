package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// batchRecordWithErr builds a BatchRecord that failed with the given error.
func batchRecordWithErr(t *testing.T, batchErr aerospike.Error) *aerospike.BatchRecord {
	t.Helper()

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	return &aerospike.BatchRecord{Key: key, Err: batchErr}
}

// TestClassifyParentUpdateResult is the fail-closed contract. Every outcome the
// pruner cannot positively read as a success must be classified as a failure:
// the caller deletes the child records in the same batch on the strength of this
// classification, so counting an unreadable response as success would delete
// children whose parent's deletedChildren map was never updated — the exact
// referential-integrity break the parent-update phase exists to prevent.
func TestClassifyParentUpdateResult(t *testing.T) {

	// KEY_NOT_FOUND is how the UPDATE_ONLY marker write reports an
	// already-deleted parent, which is benign.
	t.Run("KEY_NOT_FOUND is a skip", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(batchRecordWithErr(t, aerospike.ErrKeyNotFound))
		require.NoError(t, err)
		require.Equal(t, parentUpdateNotFound, outcome)
	})

	t.Run("other batch errors are failures", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(batchRecordWithErr(t, aerospike.ErrTimeout))
		require.Equal(t, parentUpdateFailed, outcome)
		require.Error(t, err)
	})

	// The MapPutItems marker write has no response map: a nil Err with an OK
	// result code is the whole of its success signal.
	t.Run("a marker write succeeds without a response map", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyParentUpdateResult(&aerospike.BatchRecord{Key: key})
		require.NoError(t, classifyErr)
		require.Equal(t, parentUpdateOK, outcome)
	})

	// A record the batch never got an answer for keeps the NO_RESPONSE the client
	// prepared it with and a nil Err: the client stamps Err only while parsing a
	// response. That used to read as success, so a node-level BatchOperate
	// failure deleted every child with no marker.
	t.Run("a record with no answer fails closed", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyParentUpdateResult(
			&aerospike.BatchRecord{Key: key, ResultCode: types.NO_RESPONSE})
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, classifyErr, "no answer")
	})
}

// spentUtxoElement builds one utxos-bin element recording spender as the
// transaction that spent it: a 32-byte utxo hash (zero here) followed by the
// 36 bytes of spending data, txid then vin.
func spentUtxoElement(spender *chainhash.Hash) []byte {
	element := make([]byte, 68)
	copy(element[32:64], spender[:])

	return element
}

// TestSpendHolderNamesTheChild pins the rule that decides whether a pruned child may be
// marked on a parent record: the output its input names must record that child
// as the spender. A conflicting loser names the same outpoint and never held it.
func TestSpendHolderNamesTheChild(t *testing.T) {
	var winner, loser chainhash.Hash
	winner[0] = 0xAA
	loser[0] = 0xBB

	spentBy := func(spender chainhash.Hash) []byte {
		element := make([]byte, 68)
		copy(element[32:64], spender[:])

		return element
	}

	unspent := make([]byte, 32)
	utxos := []interface{}{spentBy(winner), unspent}

	require.True(t, spendHolder(utxos, []uint32{0}, &winner) == holderNamesChild, "the holder of the output is marked")
	require.False(t, spendHolder(utxos, []uint32{0}, &loser) == holderNamesChild, "a loser that never held the output is not")
	require.False(t, spendHolder(utxos, []uint32{1}, &winner) == holderNamesChild, "an unspent output names nobody")
	require.False(t, spendHolder(utxos, []uint32{7}, &winner) == holderNamesChild, "an offset past the page names nobody")
	require.True(t, spendHolder(utxos, []uint32{1, 0}, &winner) == holderNamesChild, "any claimed offset that names the child is enough")
	require.False(t, spendHolder(nil, []uint32{0}, &winner) == holderNamesChild, "a record with no utxos bin names nobody")
}

// TestSpendHolderFrozenOutputIsUnverified: spendHolder is the one place that
// decides a child may be deleted with no marker (holderNamesOther, a
// conflicting loser). A frozen output stores the 36-byte all-0xFF sentinel
// where the spending data goes, which names no transaction, so it must hold the
// child back like any other output that says nothing about it, not read as
// "spent by someone else".
func TestSpendHolderFrozenOutputIsUnverified(t *testing.T) {
	var child, other chainhash.Hash
	child[0] = 0xAA
	other[0] = 0xBB

	frozen := make([]byte, 68)
	copy(frozen[32:], subtree.FrozenBytes[:])

	spentByOther := make([]byte, 68)
	copy(spentByOther[32:64], other[:])

	require.Equal(t, holderUnverified, spendHolder([]interface{}{frozen}, []uint32{0}, &child),
		"a frozen output names no spender, so the child is held back")
	require.Equal(t, holderUnverified, spendHolder([]interface{}{spentByOther, frozen}, []uint32{0, 1}, &child),
		"one frozen output among the claimed ones is enough to hold the child back")
	require.Equal(t, holderNamesOther, spendHolder([]interface{}{spentByOther}, []uint32{0}, &child),
		"control: an output spent by a different, well-formed spender is still a conflicting loser")
}

// TestIsKeyNotFound covers the bare-error fast path and the Matches fallback
// for wrapped errors.
func TestIsKeyNotFound(t *testing.T) {
	require.True(t, isKeyNotFound(&aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}))
	require.False(t, isKeyNotFound(&aerospike.AerospikeError{ResultCode: types.TIMEOUT}))

	var typedNil *aerospike.AerospikeError
	require.False(t, isKeyNotFound(typedNil))

	// Any shape other than a bare *AerospikeError skips the ResultCode fast path
	// and takes the Matches fallback. Embedding is the only way to build such an
	// aerospike.Error outside the client package.
	type nonBare struct{ *aerospike.AerospikeError }

	require.True(t, isKeyNotFound(nonBare{&aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}}))
	require.False(t, isKeyNotFound(nonBare{&aerospike.AerospikeError{ResultCode: types.TIMEOUT}}))
}
