package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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

// nativeAnswer sets a batch record to the answer a sub-op 15 write gets: a
// response map in the SUCCESS bin, or a wire error.
func nativeAnswer(br *aerospike.BatchRecord, resp interface{}, wireErr *aerospike.AerospikeError) {
	if wireErr != nil {
		br.Err = wireErr
		br.ResultCode = wireErr.ResultCode

		return
	}

	br.ResultCode = types.OK
	br.Record = &aerospike.Record{Key: br.Key, Bins: aerospike.BinMap{}}

	if resp != nil {
		br.Record.Bins[nativeResultBin] = resp
	}
}

// TestClassifyNativeMarkerResult maps every answer a sub-op 15 marker write can
// get onto the outcomes the filtered write has. NOT_SPENDER must take the
// refused path, as FILTERED_OUT does, and every answer that does not prove the
// write landed must fail closed.
func TestClassifyNativeMarkerResult(t *testing.T) {
	notSpender := map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "not the spender"}

	for _, tc := range []struct {
		name    string
		resp    interface{}
		wireErr *aerospike.AerospikeError
		inDoubt bool
		want    parentUpdateOutcome
	}{
		{name: "OK is written", resp: map[interface{}]interface{}{"status": "OK"}, want: parentUpdateOK},
		{name: "OK in a string-keyed map is written", resp: map[string]interface{}{"status": "OK"}, want: parentUpdateOK},
		{name: "OK as map pairs is written", resp: []aerospike.MapPair{{Key: "status", Value: "OK"}}, want: parentUpdateOK},
		{name: "NOT_SPENDER is refused", resp: notSpender, want: parentUpdateRefused},
		{name: "NOT_SPENDER in doubt is a failure", resp: notSpender, inDoubt: true, want: parentUpdateFailed},
		{name: "KEY_NOT_FOUND is a parent that is gone", wireErr: &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}, want: parentUpdateNotFound},
		{name: "TX_NOT_FOUND is a parent that is gone", resp: map[interface{}]interface{}{"status": "ERROR", "errorCode": "TX_NOT_FOUND"}, want: parentUpdateNotFound},
		{name: "INVALID_PARAMETER is a failure", resp: map[interface{}]interface{}{"status": "ERROR", "errorCode": "INVALID_PARAMETER", "message": "bad"}, want: parentUpdateFailed},
		{name: "PARAMETER_ERROR is a failure", wireErr: &aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}, want: parentUpdateFailed},
		{name: "ERROR without a code is a failure", resp: map[interface{}]interface{}{"status": "ERROR"}, want: parentUpdateFailed},
		{name: "an unknown status is a failure", resp: map[interface{}]interface{}{"status": "MAYBE"}, want: parentUpdateFailed},
		{name: "a response that is not a map is a failure", resp: "OK", want: parentUpdateFailed},
		{name: "a missing response bin is a failure", want: parentUpdateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
			require.NoError(t, err)

			br := &aerospike.BatchRecord{Key: key, InDoubt: tc.inDoubt}
			nativeAnswer(br, tc.resp, tc.wireErr)

			outcome, classifyErr := classifyNativeMarkerResult(br)
			require.Equal(t, tc.want, outcome)

			if tc.want == parentUpdateFailed {
				require.Error(t, classifyErr)
			} else {
				require.NoError(t, classifyErr)
			}
		})
	}

	t.Run("a record with no answer fails closed", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyNativeMarkerResult(&aerospike.BatchRecord{Key: key, ResultCode: types.NO_RESPONSE})
		require.Equal(t, parentUpdateFailed, outcome)
		require.Error(t, classifyErr)
	})

	t.Run("an answer with no record fails closed", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyNativeMarkerResult(&aerospike.BatchRecord{Key: key, ResultCode: types.OK})
		require.Equal(t, parentUpdateFailed, outcome)
		require.Error(t, classifyErr)
	})
}

// TestWriteVerifiedMarkers_NativeAnswers sends one native marker write per
// answer shape through writeVerifiedMarkers and checks where each lands: OK
// counts the parent present, NOT_SPENDER goes to the refused set that
// markParents re-reads and re-writes, a parent that is gone needs nothing, and
// INVALID_PARAMETER and PARAMETER_ERROR hold their child back. Every wire
// error is handed to the store's observer, which demotes on PARAMETER_ERROR.
func TestWriteVerifiedMarkers_NativeAnswers(t *testing.T) {
	s := newTestService(t)
	s.buildVerifiedMarkerRecord = testNativeMarkerRecord

	var observed []types.ResultCode

	s.observeNativeOpError = func(err error) {
		var aeroErr *aerospike.AerospikeError
		require.ErrorAs(t, err, &aeroErr)
		observed = append(observed, aeroErr.ResultCode)
	}

	type answer struct {
		resp    interface{}
		wireErr *aerospike.AerospikeError
	}

	answers := map[string]answer{
		"ok":          {resp: map[interface{}]interface{}{"status": "OK"}},
		"not-spender": {resp: map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER"}},
		"gone":        {wireErr: &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}},
		"invalid":     {resp: map[interface{}]interface{}{"status": "ERROR", "errorCode": "INVALID_PARAMETER"}},
		"no-sub-op":   {wireErr: &aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}},
	}

	updates := make(map[string]*parentUpdateInfo, len(answers))
	children := make(map[string]chainhash.Hash, len(answers))
	byDigest := make(map[string]answer, len(answers))

	for name, a := range answers {
		parent := chainhash.HashH([]byte("parent-" + name))
		child := chainhash.HashH([]byte("child-" + name))
		children[name] = child

		require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

		key, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySource(&parent, 0, s.utxoBatchSize))
		require.NoError(t, err)

		byDigest[string(key.Digest())] = a
	}

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		for _, rec := range records {
			a, ok := byDigest[string(rec.BatchRec().Key.Digest())]
			require.True(t, ok)
			nativeAnswer(rec.BatchRec(), a.resp, a.wireErr)
		}

		return nil
	}

	plan := &markingPlan{}

	refused, failed, sent, err := s.writeVerifiedMarkers(context.Background(), updates, plan, nil)
	require.NoError(t, err)
	require.True(t, sent)

	require.Len(t, refused, 1)

	for _, info := range refused {
		require.Len(t, info.childHashes, 1)
		require.Equal(t, children["not-spender"], *info.childHashes[0], "NOT_SPENDER takes the refused path")
	}

	require.Equal(t, map[chainhash.Hash]struct{}{children["invalid"]: {}, children["no-sub-op"]: {}}, failed,
		"INVALID_PARAMETER and PARAMETER_ERROR hold their child back")
	require.Equal(t, 1, plan.existing, "only the written parent counts as present")
	require.ElementsMatch(t, []types.ResultCode{types.KEY_NOT_FOUND_ERROR, types.PARAMETER_ERROR}, observed,
		"every wire error of a native write reaches the observer")
}
