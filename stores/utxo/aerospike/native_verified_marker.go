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

// encodeVerifiedMarkerPayload builds the sub-op 15 payload [15, [entries]].
// Each entry is [key, txid, offsets]: the child txid as the deletedChildren key
// (chainhash.Hash.String()), the same txid as 32 bytes in internal order, which
// the server compares with bytes 32 to 64 of the utxos element, and the utxos
// indexes the child claims on this record.
func encodeVerifiedMarkerPayload(children []aeropruner.MarkerChild) ([]byte, error) {
	entries := make([]any, len(children))

	for i, child := range children {
		offsets := make([]any, len(child.Offsets))
		for j, offset := range child.Offsets {
			offsets[j] = int64(offset)
		}

		entries[i] = []any{child.Hash.String(), child.Hash.CloneBytes(), offsets}
	}

	return encodeNativeOpPayload(subOpAddVerifiedDeleted, []any{entries})
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
// sub-op 15 is off, or the payload could not be encoded.
func (s *Store) prunerVerifiedMarkerRecord(key *aerospike.Key, children []aeropruner.MarkerChild) (aerospike.BatchRecordIfc, bool) {
	if !s.useNativeVerifiedMarkerWrite() {
		return nil, false
	}

	payload, err := encodeVerifiedMarkerPayload(children)
	if err != nil {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren payload encode failed: %v; this marker write uses the filtered path", err)
		return nil, false
	}

	return newVerifiedMarkerWrite(key, payload), true
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

	const keep = "the pruner keeps the filtered marker write"

	key, err := aerospike.NewKey(s.namespace, s.setName, fmt.Sprintf("_teranode-native-marker-probe-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err != nil {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe key creation failed: %v; %s", err, keep)
		return false
	}

	childA := chainhash.DoubleHashH([]byte(key.String() + "-a"))
	childB := chainhash.DoubleHashH([]byte(key.String() + "-b"))

	// A spent element is the 32-byte utxo hash followed by the 36 bytes of
	// spending data, the spender's txid then its vin.
	spent := make([]byte, 68)
	copy(spent[32:64], childA.CloneBytes())

	unspent := make([]byte, 32)

	const probeTTL = 60

	policy := aerospike.NewWritePolicy(0, probeTTL)
	policy.TotalTimeout = 2 * time.Second

	if err = s.client.PutBins(policy, key, aerospike.NewBin(fields.Utxos.String(), []any{spent, unspent})); err != nil {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe record setup failed: %v; %s", err, keep)
		return false
	}
	defer func() {
		_, _ = s.client.Delete(policy, key)
	}()

	readPolicy := aerospike.NewPolicy()
	readPolicy.TotalTimeout = 2 * time.Second

	// Step 1: a child the output names.
	code, opErr := s.probeVerifiedMarker(key, &childA, 0)
	if opErr != nil {
		if hasResultCode(opErr, types.PARAMETER_ERROR) {
			s.logger.Warnf("[teranode-native-op] server does not serve sub-op 15 (addVerifiedDeletedChildren); %s", keep)
		} else {
			s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe failed: %v; %s", opErr, keep)
		}

		return false
	}

	if code != "" {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe refused a child the output names (%s); %s", code, keep)
		return false
	}

	after, err := s.client.Get(readPolicy, key, fields.DeletedChildren.String())
	if err != nil || after == nil || !markerSet(after.Bins[fields.DeletedChildren.String()], childA.String()) {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe did not write the marker (%v); %s", err, keep)
		return false
	}

	if after.Expiration == 0 || after.Expiration > probeTTL {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe changed the record's TTL to %d; %s", after.Expiration, keep)
		return false
	}

	// Steps 2 and 3: a child the output does not name, and an unspent output.
	for _, refusal := range []struct {
		child  *chainhash.Hash
		offset uint32
	}{{&childB, 0}, {&childA, 1}} {
		code, opErr = s.probeVerifiedMarker(key, refusal.child, refusal.offset)
		if opErr != nil || code != LuaErrorCodeNotSpender {
			s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe answered %q (%v) for a child output %d does not name; %s", code, opErr, refusal.offset, keep)
			return false
		}
	}

	after, err = s.client.Get(readPolicy, key, fields.DeletedChildren.String())
	if err != nil || after == nil || markerSet(after.Bins[fields.DeletedChildren.String()], childB.String()) {
		s.logger.Warnf("[teranode-native-op] addVerifiedDeletedChildren probe wrote a marker it refused (%v); %s", err, keep)
		return false
	}

	return true
}

// probeVerifiedMarker sends one sub-op 15 write for child at offset through
// the production record builder in a batch, as the pruner does. It returns the
// response's error code, empty for OK, or the record's wire error.
func (s *Store) probeVerifiedMarker(key *aerospike.Key, child *chainhash.Hash, offset uint32) (LuaErrorCode, error) {
	payload, err := encodeVerifiedMarkerPayload([]aeropruner.MarkerChild{{Hash: child, Offsets: []uint32{offset}}})
	if err != nil {
		return "", err
	}

	record := newVerifiedMarkerWrite(key, payload)

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
