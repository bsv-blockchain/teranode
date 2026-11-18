package aerospike

import (
	"context"
	"fmt"
	"os"
	"time"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	aeropruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// useNativeVerifiedMarkerWrite reports whether the pruner's replay-marker write
// goes out as native sub-op 15. It needs native ops on and the sub-op's own
// probe passed, because a server built before sub-op 15 serves every other
// native op.
func (s *Store) useNativeVerifiedMarkerWrite() bool {
	return s.useNativeTeranodeOps.Load() && s.useNativeVerifiedMarker.Load()
}

// encodeVerifiedMarkerPayload writes the sub-op 15 payload [15, [entries]] directly.
// Each entry is [key, txid, offsets]: the child txid as the deletedChildren key
// (chainhash.Hash.String(), 64 hex digits in display order), the same txid as 32
// bytes in internal order, which the server compares with bytes 32 to 64 of the
// utxos element, and the utxos indexes the child claims on this record.
//
// The bytes are identical to encodeNativeOpPayload(subOpAddVerifiedDeleted,
// []any{entries}) with the entries built as []any{hex, raw, []any{int64...}}.
// Writing them into one pre-sized buffer avoids the hex strings, the []any
// boxing and msgpack's reflection, which the pruner pays once per parent record.
// TestEncodeVerifiedMarkerPayloadMatchesMsgpack pins the equality.
func encodeVerifiedMarkerPayload(children []aeropruner.MarkerChild) []byte {
	const hexDigits = "0123456789abcdef"

	const hexLen = chainhash.HashSize * 2

	// fixarray(2) + uint8(subOp) + fixarray(1) + array header (up to 5 bytes), then per
	// child: fixarray(3) + str8 header and 64 hex digits + bin8 header and 32 bytes +
	// offsets array header (up to 5 bytes) + 9 bytes per offset (int64).
	size := 4 + 5

	for _, child := range children {
		size += 1 + (2 + hexLen) + (2 + chainhash.HashSize) + 5 + 9*len(child.Offsets)
	}

	buf := make([]byte, 0, size)

	buf = append(buf, 0x92, 0xcc, subOpAddVerifiedDeleted, 0x91)
	buf = appendMsgpackArrayHeader(buf, len(children))

	for _, child := range children {
		buf = append(buf, 0x93, 0xd9, hexLen)

		for i := chainhash.HashSize - 1; i >= 0; i-- {
			buf = append(buf, hexDigits[child.Hash[i]>>4], hexDigits[child.Hash[i]&0x0f])
		}

		buf = append(buf, 0xc4, chainhash.HashSize)
		buf = append(buf, child.Hash[:]...)

		buf = appendMsgpackArrayHeader(buf, len(child.Offsets))

		for _, offset := range child.Offsets {
			buf = appendMsgpackInt64(buf, offset)
		}
	}

	return buf
}

// appendMsgpackArrayHeader appends the shortest MessagePack array header for n elements.
func appendMsgpackArrayHeader(buf []byte, n int) []byte {
	switch {
	case n < 16:
		return append(buf, 0x90|byte(n))
	case n <= 0xffff:
		return append(buf, 0xdc, byte(n>>8), byte(n))
	default:
		return append(buf, 0xdd, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

// appendMsgpackInt64 appends v as a MessagePack int64, the width msgpack.Marshal gives an
// int64 value, so the payload is byte for byte what the reflection encoding wrote.
func appendMsgpackInt64(buf []byte, v uint32) []byte {
	return append(buf, 0xd3, 0, 0, 0, 0, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// newVerifiedMarkerWrite builds the native marker write for one parent record.
// Its policy matches the filtered write it replaces: UPDATE_ONLY, so a parent
// the pruner already deleted answers KEY_NOT_FOUND and is never re-created,
// and TTLDontUpdate, so a parent expiring in TTL mode keeps its lifetime. The
// commit level stays the default COMMIT_ALL, because the record survives.
func newVerifiedMarkerWrite(key *aerospike.Key, payload []byte) *aerospike.BatchWrite {
	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.Expiration = aerospike.TTLDontUpdate

	return aerospike.NewBatchWrite(policy, key, aerospike.TeranodeModifyOp(nativeOpResultBin, payload))
}

// prunerVerifiedMarkerRecord is the pruner's Options.BuildVerifiedMarkerRecord.
// It returns false when the marker write must take the filtered path: native
// sub-op 15 is off.
func (s *Store) prunerVerifiedMarkerRecord(key *aerospike.Key, children []aeropruner.MarkerChild) (aerospike.BatchRecordIfc, bool) {
	if !s.useNativeVerifiedMarkerWrite() {
		return nil, false
	}

	return newVerifiedMarkerWrite(key, encodeVerifiedMarkerPayload(children)), true
}

// observePrunerNativeOpError is the pruner's Options.ObserveNativeOpError. A
// PARAMETER_ERROR on a sub-op 15 write comes from a node that does not serve
// it, in a mixed-version cluster the start-up probe did not fully sample. It
// turns the native marker write off and leaves every other native op alone,
// as a sub-op 14 rejection does for the longest-chain mark: such a node still
// serves the rest. A node with no native ops at all is demoted by the next
// call site that sends one of those.
func (s *Store) observePrunerNativeOpError(err error) {
	if err == nil || !hasResultCode(err, types.PARAMETER_ERROR) {
		return
	}

	if s.useNativeVerifiedMarker.CompareAndSwap(true, false) {
		s.publishNativeRouting()
		s.logger.Warnf("[teranode-native-op] server rejected sub-op 15 (addVerifiedDeletedChildren); the pruner writes replay markers with the filtered map write from now on")
	}
}

// detectNativeVerifiedMarkerSupport proves the server serves sub-op 15 with the
// semantics of the filtered marker write. On a short-lived probe record whose
// utxos bin holds a spent element naming child A at index 0 and an unspent
// hash at index 1, sent through the production batch write:
//
//  1. [[A, A, [0]]] must answer OK, write deletedChildren[A] = true and keep
//     the record's TTL;
//  2. [[B, B, [0]]], a child the output does not name, must answer NOT_SPENDER
//     and write no B;
//  3. [[A, A, [1]]], the unspent element, must answer NOT_SPENDER.
//
// A server built before sub-op 15 answers step 1 with PARAMETER_ERROR. That,
// and any other mismatch, keeps the filtered write and every other native op.
func (s *Store) detectNativeVerifiedMarkerSupport(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	key, err := aerospike.NewKey(s.namespace, s.setName, fmt.Sprintf("_teranode-native-marker-probe-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err != nil {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe key creation failed: %v; %s", err, verifiedMarkerProbeKeep)
		return false
	}

	childA := chainhash.DoubleHashH([]byte(key.String() + "-a"))
	childB := chainhash.DoubleHashH([]byte(key.String() + "-b"))

	// A spent element is the 32-byte utxo hash followed by the 36 bytes of
	// spending data, the spender's txid then its vin.
	spent := make([]byte, 68)
	copy(spent[32:64], childA.CloneBytes())

	unspent := make([]byte, 32)

	policy := aerospike.NewWritePolicy(0, verifiedMarkerProbeTTL)
	policy.TotalTimeout = 2 * time.Second

	if err = s.client.PutBins(policy, key, aerospike.NewBin(fields.Utxos.String(), []any{spent, unspent})); err != nil {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe record setup failed: %v; %s", err, verifiedMarkerProbeKeep)
		return false
	}
	defer func() {
		_, _ = s.client.Delete(policy, key)
	}()

	readPolicy := aerospike.NewPolicy()
	readPolicy.TotalTimeout = 2 * time.Second

	return s.judgeVerifiedMarkerProbe(verifiedMarkerProbeIO{
		send: func(child *chainhash.Hash, offset uint32) (LuaErrorCode, error) {
			return s.probeVerifiedMarker(key, child, offset)
		},
		read: func() (*aerospike.Record, error) {
			rec, getErr := s.client.Get(readPolicy, key, fields.DeletedChildren.String())
			if getErr != nil {
				return nil, getErr
			}

			return rec, nil
		},
	}, &childA, &childB) == replayProbeProven
}

const (
	verifiedMarkerProbeKeep = "the pruner keeps the filtered marker write"
	verifiedMarkerProbeTTL  = 60
)

// verifiedMarkerProbeIO is what the sub-op 15 probe asks of the server: one marker write, and
// the probe record's deletedChildren bin read back. A test supplies fake answers.
type verifiedMarkerProbeIO struct {
	send func(child *chainhash.Hash, offset uint32) (LuaErrorCode, error)
	read func() (*aerospike.Record, error)
}

// judgeVerifiedMarkerProbe runs the three probe steps against srv and says what the server proved.
// It splits the two ways a step can fail the way recordReplayProtection does, because the verdict
// is final for the life of the process: a server that answered wrongly is an Error, since its
// build has to be replaced, and a probe that could not finish is a Warn. A server without sub-op
// 15 answers step 1 with PARAMETER_ERROR, which says nothing about its semantics, so it is a Warn.
func (s *Store) judgeVerifiedMarkerProbe(srv verifiedMarkerProbeIO, childA, childB *chainhash.Hash) replayProbeVerdict {
	const (
		mismatch    = "the server answered the sub-op 15 probe wrongly, so the pruner writes replay markers with the filtered map write for the life of this process; replace it with a mod-teranode build whose sub-op 15 matches"
		unavailable = "the pruner writes replay markers with the filtered map write for the life of this process because the verdict is not retried"
	)

	unfinished := func(format string, args ...any) replayProbeVerdict {
		s.logger.Warnf("[teranode-native-op] "+format+"; "+unavailable, args...)

		return replayProbeUnavailable
	}

	wrong := func(format string, args ...any) replayProbeVerdict {
		s.logger.Errorf("[teranode-native-op] addVerifiedDeletedChildren probe "+format+"; "+mismatch, args...)

		return replayProbeMismatch
	}

	// Step 1: a child the output names.
	code, opErr := srv.send(childA, 0)
	if opErr != nil {
		if hasResultCode(opErr, types.PARAMETER_ERROR) {
			return unfinished("server does not serve sub-op 15 (addVerifiedDeletedChildren)")
		}

		return unfinished("addVerifiedDeletedChildren probe failed: %v", opErr)
	}

	if code != "" {
		return wrong("refused a child the output names (%s)", code)
	}

	after, err := srv.read()
	if err != nil || after == nil {
		return unfinished("addVerifiedDeletedChildren probe could not read the marker back (%v)", err)
	}

	if !markerSet(after.Bins[fields.DeletedChildren.String()], childA.String()) {
		return wrong("did not write the marker")
	}

	if after.Expiration == 0 || after.Expiration > verifiedMarkerProbeTTL {
		return wrong("changed the record's TTL to %d", after.Expiration)
	}

	// Steps 2 and 3: a child the output does not name, and an unspent output.
	for _, refusal := range []struct {
		child  *chainhash.Hash
		offset uint32
	}{{childB, 0}, {childA, 1}} {
		code, opErr = srv.send(refusal.child, refusal.offset)
		if opErr != nil {
			return unfinished("addVerifiedDeletedChildren probe failed for a child output %d does not name: %v", refusal.offset, opErr)
		}

		if code != LuaErrorCodeNotSpender {
			return wrong("answered %q for a child output %d does not name", code, refusal.offset)
		}
	}

	after, err = srv.read()
	if err != nil || after == nil {
		return unfinished("addVerifiedDeletedChildren probe could not read the marker back (%v)", err)
	}

	if markerSet(after.Bins[fields.DeletedChildren.String()], childB.String()) {
		return wrong("wrote a marker it refused")
	}

	return replayProbeProven
}

// probeVerifiedMarker sends one sub-op 15 write for child at offset through
// the production record builder in a batch, as the pruner does. It returns the
// response's error code, empty for OK, or the record's wire error.
func (s *Store) probeVerifiedMarker(key *aerospike.Key, child *chainhash.Hash, offset uint32) (LuaErrorCode, error) {
	record := newVerifiedMarkerWrite(key, encodeVerifiedMarkerPayload([]aeropruner.MarkerChild{{Hash: child, Offsets: []uint32{offset}}}))

	batchPolicy := aerospike.NewBatchPolicy()
	batchPolicy.TotalTimeout = 2 * time.Second

	if batchErr := s.client.BatchOperate(batchPolicy, []aerospike.BatchRecordIfc{record}); batchErr != nil && record.Err == nil {
		return "", batchErr
	}

	return s.verifiedMarkerAnswer(&record.BatchRecord)
}

// verifiedMarkerAnswer reads one sub-op 15 batch record: the response's error
// code, empty for OK, or the record's wire error. Split from
// probeVerifiedMarker so the probe's reading can be tested without a server, as
// rejectsWith is.
func (s *Store) verifiedMarkerAnswer(record *aerospike.BatchRecord) (LuaErrorCode, error) {
	if record.Err != nil {
		return "", record.Err
	}

	if record.Record == nil || record.Record.Bins == nil {
		return "", errors.NewProcessingError("no response record")
	}

	res, err := s.ParseLuaMapResponse(record.Record.Bins[nativeOpResultBin])
	if err != nil {
		return "", err
	}

	switch res.Status {
	case LuaStatusOK:
		return "", nil
	case LuaStatusError:
		if res.ErrorCode == "" {
			return "", errors.NewProcessingError("ERROR response without an error code")
		}

		return res.ErrorCode, nil
	default:
		return "", errors.NewProcessingError("unknown status %q", res.Status)
	}
}

// markerSet reports whether a deletedChildren bin holds key = true.
func markerSet(bin interface{}, key string) bool {
	switch m := bin.(type) {
	case map[interface{}]interface{}:
		v, ok := m[key].(bool)
		return ok && v
	case map[string]interface{}:
		v, ok := m[key].(bool)
		return ok && v
	default:
		return false
	}
}
