// Package aerospike — native operate-path support for mod-teranode.
//
// Each mod-teranode UDF function (spend, setMined, freeze, ...) can be
// invoked through one of two paths:
//
//  1. UDF path (legacy):  aerospike.NewBatchUDF(..., "<fn>", args...)
//  2. Native path:        aerospike.NewBatchWrite(..., TeranodeModifyOp("SUCCESS", payload))
//
// The native path bypasses the UDF executor and runs under the same
// lock as native ops like LIST_APPEND. It requires both:
//
//   - A server running the BSV fork of aerospike-server (which
//     adds wire opcodes 200/201 and the SUBOP_TABLE dispatcher).
//   - The setting `AerospikeSettings.UseNativeTeranodeOps = true`.
//
// If either is missing, calls fall back transparently to the UDF
// path. The decision is made once at store init and cached.
//
// Call sites should not branch themselves — they use the wrappers
// teranodeBatchRecord / executeTeranodeOp here, which hide the
// path selection.

package aerospike

import (
	"context"
	"fmt"
	"os"
	"time"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/vmihailenco/msgpack/v5"
)

// Sub-op IDs are wire contract — frozen, must match the SUBOP_TABLE
// in modules/mod-teranode/src/main/mod_teranode_native_op.c on the
// server-private fork. Never renumber.
const (
	subOpSpend                  uint8 = 1
	subOpSpendMulti             uint8 = 2
	subOpUnspend                uint8 = 3
	subOpSetMined               uint8 = 4
	subOpFreeze                 uint8 = 5
	subOpUnfreeze               uint8 = 6
	subOpReassign               uint8 = 7
	subOpSetConflicting         uint8 = 8
	subOpPreserveUntil          uint8 = 9
	subOpSetLocked              uint8 = 10
	subOpIncrementSpentExtraRec uint8 = 11
	subOpSetDeleteAtHeight      uint8 = 12
	subOpAddDeletedChildren     uint8 = 13
)

// nativeOpResultBin is the bin name used by the native-op result message.
// Keep this aligned with LuaSuccess so existing batch response parsing works
// for both UDF and native paths.
const nativeOpResultBin = string(LuaSuccess)

// encodeNativeOpPayload serializes a sub-op invocation onto the wire
// as MessagePack `[sub_op_id, [args...]]` matching the dispatcher's
// expected format. A nil args slice is normalized to an empty list so a
// zero-arg sub-op encodes as `[id, []]` rather than `[id, nil]` — the
// documented contract is always an args *list*.
func encodeNativeOpPayload(subOp uint8, args []any) ([]byte, error) {
	if args == nil {
		args = []any{}
	}
	return msgpack.Marshal([]any{subOp, args})
}

// useNativeForSubOp decides whether a given mod-teranode sub-op may use the
// native operate-path. It requires the setting+capability flag, and additionally
// FENCES unspend (subOpUnspend) to the UDF/Lua path regardless of the flag.
//
// Rationale (#899): the UDF unspend path always enforces the #766 SpendingData
// ownership check before reversing a spend. The native path would forward
// SpendingData to the server-fork subOpUnspend=3 dispatcher, whose enforcement
// the startup probe does not exercise — it proves setLocked dispatch and
// spendMulti first-seen semantics (see probeNativeSpendSemantics), not
// unspend's ownership check. Running native unspend against a server that
// accepts sub-op 3 without enforcing ownership (an older fork build, a
// mixed-version cluster, or a dispatcher that silently ignores the arg) would
// be a silent spend-reversal / UTXO-resurrection primitive on the reorg /
// ProcessConflicting / catchup path, so unspend stays on the UDF path. An
// ownership-rejection probe analogous to the spend probe could un-fence it in
// a follow-up once that scenario is exercised end-to-end.
func (s *Store) useNativeForSubOp(subOp uint8) bool {
	return s.useNativeTeranodeOps.Load() && subOp != subOpUnspend
}

// demoteNativeOnUnsupported permanently demotes the store to the UDF path when
// a mod-teranode operation comes back with PARAMETER_ERROR — the result code an
// unpatched aerospike-server returns for the unknown wire op 200. The startup
// probe samples a single partition master, so a mixed-version cluster (or a
// node rolled back to a stock build after startup) can pass the probe and then
// reject native writes on other partitions at runtime. Without demotion every
// mod-teranode write to those partitions fails for the process lifetime; with
// it the affected batch fails once and everything after falls back to UDF.
//
// Called from the per-record error branches of every native-capable call site
// and from executeTeranodeOp. A no-op when native ops are already off (the
// UDF path can also surface PARAMETER_ERROR for unrelated reasons; we only
// interpret it as "native unsupported" while native ops are active).
func (s *Store) demoteNativeOnUnsupported(err error) {
	if err == nil || !s.useNativeTeranodeOps.Load() {
		return
	}

	var aerr aerospike.Error
	if errors.As(err, &aerr) && aerr.Matches(types.PARAMETER_ERROR) && s.useNativeTeranodeOps.CompareAndSwap(true, false) {
		s.logger.Errorf("[teranode-native-op] server rejected a native op with PARAMETER_ERROR; demoting to the UDF path for the rest of this process")
	}
}

// isKeyNotFound reports whether err carries the per-record KEY_NOT_FOUND_ERROR
// result code — the shape a missing record takes on the native path under
// UPDATE_ONLY. The UDF path reports the same condition as a Lua TX_NOT_FOUND
// status instead, so result handlers use this to keep both transports
// producing identical not-found semantics.
func isKeyNotFound(err error) bool {
	var aerr aerospike.Error
	return errors.As(err, &aerr) && aerr.Matches(types.KEY_NOT_FOUND_ERROR)
}

// teranodeBatchRecordResponse validates one mod-teranode batch record result
// and extracts its parsed response. Returns:
//   - (res, nil) when the record carries a parseable response map (the caller
//     still branches on res.Status);
//   - (nil, TxNotFoundError) when the record is missing (native UPDATE_ONLY
//     KEY_NOT_FOUND — the UDF path's TX_NOT_FOUND equivalent);
//   - (nil, StorageError) for every other failure shape (nil record, batch
//     error, missing response bin, unparsable response).
//
// prefix is the caller's log tag (e.g. "[freeze][3][txid:vout]") and is
// prepended verbatim to every error. Also feeds demoteNativeOnUnsupported so
// a runtime PARAMETER_ERROR from a mixed-version cluster demotes the store
// to the UDF path.
func (s *Store) teranodeBatchRecordResponse(prefix string, record aerospike.BatchRecordIfc) (*LuaMapResponse, error) {
	if record == nil {
		return nil, errors.NewStorageError("%s missing batch record; %s", prefix, describeAerospikeBatchRecord(record))
	}

	batchRec := record.BatchRec()
	if batchRec == nil {
		return nil, errors.NewStorageError("%s missing batch record; %s", prefix, describeAerospikeBatchRecord(record))
	}

	if batchRec.Err != nil {
		s.demoteNativeOnUnsupported(batchRec.Err)

		if isKeyNotFound(batchRec.Err) {
			return nil, errors.NewTxNotFoundError("%s transaction not found", prefix, batchRec.Err)
		}

		return nil, errors.NewStorageError("%s batch record failed; %s: %s", prefix, describeAerospikeBatchRecord(record), batchRec.Err.Error(), batchRec.Err)
	}

	response := batchRec.Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		return nil, errors.NewStorageError("%s missing expected response bin %q; %s", prefix, LuaSuccess.String(), describeAerospikeBatchRecord(record))
	}

	rawResponse := response.Bins[LuaSuccess.String()]

	res, err := s.ParseLuaMapResponse(rawResponse)
	if err != nil {
		return nil, errors.NewStorageError("%s failed to parse response bin %q (value %s); %s: %s", prefix, LuaSuccess.String(), describeAerospikeValue(rawResponse), describeAerospikeBatchRecord(record), err.Error(), err)
	}

	return res, nil
}

// teranodeBatchRecord builds a BatchRecord that invokes a mod-teranode
// function. The record uses either the native operate-path (when the
// store has determined both the setting + capability allow it) or the
// legacy UDF path. Args must be plain Go values (int64, []byte, bool,
// string, []any, map[any]any, ...) that round-trip through both
// MessagePack and aerospike.NewValue.
//
// udfPolicy / udfPackage are used only when falling back to the UDF
// path — on the native path only udfPolicy's FilterExpression is
// honoured; any other non-default BatchUDFPolicy field is dropped.
// Most call sites pass LuaPackage as udfPackage; setMined optionally
// uses LuaPackageMined; spendMulti optionally uses one of
// s.spendLuaPackages. Pass nil for udfPolicy to get default.
func (s *Store) teranodeBatchRecord(
	udfPolicy *aerospike.BatchUDFPolicy,
	udfPackage string,
	key *aerospike.Key,
	subOp uint8,
	udfFnName string,
	args ...any,
) aerospike.BatchRecordIfc {
	if s.useNativeForSubOp(subOp) {
		payload, err := encodeNativeOpPayload(subOp, args)
		if err != nil {
			// Fallback: encoding shouldn't fail for plain Go types.
			// Log once and use UDF.
			s.logger.Warnf("[teranodeBatchRecord] msgpack encode failed for sub_op=%d: %v; falling back to UDF",
				subOp, err)
			return s.teranodeBatchUDFRecord(udfPolicy, udfPackage, key, udfFnName, args)
		}
		// Reuse the Store's shared filterless BatchWritePolicy (allocated once
		// in initNativeTeranodeOps) on the hot path. The Aerospike client only
		// reads from the policy during BatchOperate, and no caller mutates it
		// after init.
		writePolicy := s.nativeOpBatchWritePolicy

		// Carry a caller-supplied server-side FilterExpression onto the native
		// write so filtered-out records are skipped server-side exactly as on
		// the UDF path. PreserveTransactions sets the deleteAtHeight/preserveUntil
		// eligibility gate here; without carrying it the native path would write
		// preserveUntil to every record — including the partially-spent parents
		// the gate deliberately excludes. Allocate a per-call copy only when a
		// filter is actually present (today only subOpPreserveUntil), leaving the
		// shared filterless policy for the hot spend/setMined/locked ops.
		if udfPolicy != nil && udfPolicy.FilterExpression != nil {
			wp := *s.nativeOpBatchWritePolicy
			wp.FilterExpression = udfPolicy.FilterExpression
			writePolicy = &wp
		}

		return aerospike.NewBatchWrite(
			writePolicy,
			key,
			aerospike.TeranodeModifyOp(nativeOpResultBin, payload),
		)
	}
	return s.teranodeBatchUDFRecord(udfPolicy, udfPackage, key, udfFnName, args)
}

// teranodeBatchUDFRecord is the UDF-path branch of teranodeBatchRecord.
// Split out so it can also be called explicitly when a fallback is
// needed mid-flight (e.g. after the native path returned an error
// the call site classifies as transient).
func (s *Store) teranodeBatchUDFRecord(
	udfPolicy *aerospike.BatchUDFPolicy,
	udfPackage string,
	key *aerospike.Key,
	udfFnName string,
	args []any,
) aerospike.BatchRecordIfc {
	if udfPolicy == nil {
		udfPolicy = aerospike.NewBatchUDFPolicy()
	}
	if udfPackage == "" {
		udfPackage = LuaPackage
	}
	udfArgs := make([]aerospike.Value, len(args))
	for i, a := range args {
		udfArgs[i] = aerospike.NewValue(a)
	}
	return aerospike.NewBatchUDF(udfPolicy, key, udfPackage, udfFnName, udfArgs...)
}

// executeTeranodeOp issues a single-record (non-batch) mod-teranode
// invocation against the store. Returns the same shape of result as
// client.Execute would, regardless of which path was taken: an
// interface{} carrying the result map (or nil if the function
// returned no value).
func (s *Store) executeTeranodeOp(
	udfPolicy *aerospike.WritePolicy,
	key *aerospike.Key,
	subOp uint8,
	udfFnName string,
	args ...any,
) (any, aerospike.Error) {
	if s.useNativeForSubOp(subOp) {
		payload, err := encodeNativeOpPayload(subOp, args)
		if err != nil {
			s.logger.Warnf("[executeTeranodeOp] msgpack encode failed for sub_op=%d: %v; falling back to UDF",
				subOp, err)
			return s.executeTeranodeUDF(udfPolicy, key, udfFnName, args)
		}
		// Reuse the caller's WritePolicy so timeouts/retries/commit-level/TTL stay
		// consistent with the UDF path. Only synthesise a default when the caller
		// passed nil (the UDF path's client.Execute tolerates a nil policy, but
		// client.Operate does not).
		writePolicy := udfPolicy
		if writePolicy == nil {
			writePolicy = aerospike.NewWritePolicy(0, 0)
		}
		rec, opErr := s.client.Operate(writePolicy, key, aerospike.TeranodeModifyOp(nativeOpResultBin, payload))
		if opErr != nil {
			s.demoteNativeOnUnsupported(opErr)
			return nil, opErr
		}
		if rec == nil || rec.Bins == nil {
			return nil, nil
		}
		return rec.Bins[nativeOpResultBin], nil
	}
	return s.executeTeranodeUDF(udfPolicy, key, udfFnName, args)
}

func (s *Store) executeTeranodeUDF(
	udfPolicy *aerospike.WritePolicy,
	key *aerospike.Key,
	udfFnName string,
	args []any,
) (any, aerospike.Error) {
	udfArgs := make([]aerospike.Value, len(args))
	for i, a := range args {
		udfArgs[i] = aerospike.NewValue(a)
	}
	return s.client.Execute(udfPolicy, key, LuaPackage, udfFnName, udfArgs...)
}

// detectNativeTeranodeOpSupport probes the cluster to confirm it
// understands AS_MSG_OP_TERANODE_MODIFY (wire op 200). Returns true
// only when the server accepts a valid native sub-op, returns a parseable
// response, applies the expected record mutation, AND proves spend
// semantics: a synthetic single-UTXO record built through the production
// GetBinsToStore path must spend natively and then REJECT a re-spend
// with different SpendingData (LuaErrorCodeSpent). Anything else biases
// toward false-negative so the fallback never runs against a server that
// doesn't support the op — or supports the opcode without enforcing
// first-seen on spend.
//
// The probe honours ctx between round-trips: cancellation aborts it and
// falls back to the UDF path.
//
// Called once during store construction; the result is cached in
// s.useNativeTeranodeOps.
func (s *Store) detectNativeTeranodeOpSupport(ctx context.Context) bool {
	if !s.settings.Aerospike.UseNativeTeranodeOps {
		return false
	}

	if ctx.Err() != nil {
		s.logger.Warnf("[teranode-native-op] probe aborted by context: %v; falling back to UDF path", ctx.Err())
		return false
	}

	// Probe key — chosen to never collide with real txid keys (always
	// 32 bytes / chainhash). Create a short-lived record and run a valid
	// setLocked sub-op via TeranodeModifyOp: a patched server recognises wire
	// op 200, executes the sub-op, and returns a structured response; an
	// unpatched server rejects the unknown opcode with PARAMETER_ERROR.
	// Per-process probe key. Two teranode instances booting concurrently against
	// the same cluster would otherwise share the same key — their PutBins /
	// Operate / Get sequences can interleave so one instance observes a record
	// whose Locked bin was overwritten by the other and returns a false-negative.
	// PID + nanosecond starttime is unique across simultaneous processes on any
	// host, and the probe record's 60s TTL bounds the namespace residency in the
	// rare case Delete fails.
	probeKeyName := fmt.Sprintf("_teranode-native-op-probe-%d-%d", os.Getpid(), time.Now().UnixNano())
	probeKey, err := aerospike.NewKey(s.namespace, s.setName, probeKeyName)
	if err != nil {
		s.logger.Warnf("[teranode-native-op] probe key creation failed: %v; falling back to UDF path", err)
		return false
	}

	payload, encErr := encodeNativeOpPayload(subOpSetLocked, []any{true})
	if encErr != nil {
		s.logger.Warnf("[teranode-native-op] probe payload encode failed: %v; falling back to UDF path", encErr)
		return false
	}

	probeOp := aerospike.TeranodeModifyOp(nativeOpResultBin, payload)

	policy := aerospike.NewWritePolicy(0, 0)
	policy.TotalTimeout = 2 * time.Second
	policy.Expiration = 60

	if putErr := s.client.PutBins(policy, probeKey, aerospike.NewBin("_nativeProbe", true)); putErr != nil {
		s.logger.Warnf("[teranode-native-op] probe record setup failed: %v; falling back to UDF path", putErr)
		return false
	}
	defer func() {
		_, _ = s.client.Delete(policy, probeKey)
	}()

	if ctx.Err() != nil {
		s.logger.Warnf("[teranode-native-op] probe aborted by context: %v; falling back to UDF path", ctx.Err())
		return false
	}

	rec, opErr := s.client.Operate(policy, probeKey, probeOp)
	if opErr == nil {
		if rec == nil || rec.Bins == nil || rec.Bins[nativeOpResultBin] == nil {
			s.logger.Warnf("[teranode-native-op] probe returned no %q bin; %s; falling back to UDF path", nativeOpResultBin, describeAerospikeRecord(rec))
			return false
		}

		rawResponse := rec.Bins[nativeOpResultBin]
		res, parseErr := s.ParseLuaMapResponse(rawResponse)
		if parseErr != nil {
			s.logger.Warnf("[teranode-native-op] probe returned unparsable response bin %q (value %s); %s: %v; falling back to UDF path", nativeOpResultBin, describeAerospikeValue(rawResponse), describeAerospikeRecord(rec), parseErr)
			return false
		}

		if res.Status != LuaStatusOK {
			s.logger.Warnf("[teranode-native-op] probe returned non-OK response (%+v); falling back to UDF path", res)
			return false
		}

		readPolicy := aerospike.NewPolicy()
		readPolicy.TotalTimeout = 2 * time.Second

		probeRecord, readErr := s.client.Get(readPolicy, probeKey, fields.Locked.String())
		if readErr != nil {
			s.logger.Warnf("[teranode-native-op] probe verification failed: %v; falling back to UDF path", readErr)
			return false
		}
		if probeRecord == nil || probeRecord.Bins == nil {
			s.logger.Warnf("[teranode-native-op] probe verification returned no record; falling back to UDF path")
			return false
		}
		if locked, ok := probeRecord.Bins[fields.Locked.String()].(bool); !ok || !locked {
			s.logger.Warnf("[teranode-native-op] probe did not set %q=true; falling back to UDF path", fields.Locked.String())
			return false
		}

		if ctx.Err() != nil {
			s.logger.Warnf("[teranode-native-op] probe aborted by context: %v; falling back to UDF path", ctx.Err())
			return false
		}

		// Opcode dispatch is proven; now prove the semantics the gate actually
		// protects. spendMulti is the consensus-critical sub-op — an opcode-only
		// probe says nothing about first-seen enforcement.
		if !s.probeNativeSpendSemantics(ctx, policy) {
			return false
		}

		return true
	}

	var aerr aerospike.Error
	if errors.As(opErr, &aerr) {
		if aerr.Matches(types.PARAMETER_ERROR) {
			s.logger.Infof("[teranode-native-op] server rejected native-op probe; falling back to UDF path")
			return false
		}
		s.logger.Warnf("[teranode-native-op] probe got unexpected error code (%v); "+
			"falling back to UDF path", aerr)
		return false
	}

	s.logger.Warnf("[teranode-native-op] probe failed with non-aerospike error (%v); "+
		"falling back to UDF path", opErr)
	return false
}

// probeNativeSpendSemantics proves the native dispatcher enforces first-seen
// spend semantics before spendMulti is allowed onto the native path. The UDF
// unspend fence exists because server-side enforcement cannot be read from
// the client; for spend it CAN be exercised: build a synthetic single-UTXO
// record through the production GetBinsToStore path (so the record layout is
// exactly what real transactions get), spend it natively, then attempt a
// re-spend of the same offset with different SpendingData and require the
// dispatcher to reject it with LuaErrorCodeSpent. Any other outcome — first
// spend fails, re-spend succeeds, re-spend fails with anything but SPENT —
// reports unsupported and keeps the store on the UDF path.
//
// The record is keyed with the same CalculateKeySource form as real records;
// the synthetic transaction's satoshi value carries per-process entropy so
// concurrent booting instances never contend on one probe record. The 60s
// TTL on the shared probe policy bounds residue if the deferred Delete fails.
func (s *Store) probeNativeSpendSemantics(ctx context.Context, policy *aerospike.WritePolicy) bool {
	const (
		probeBlockHeight = uint32(1)
		probeVout        = uint32(0)
	)

	if s.utxoBatchSize <= 0 {
		s.logger.Warnf("[teranode-native-op] spend probe skipped: invalid utxoBatchSize %d; falling back to UDF path", s.utxoBatchSize)
		return false
	}

	lockingScript, scriptErr := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	if scriptErr != nil {
		s.logger.Warnf("[teranode-native-op] spend probe script build failed: %v; falling back to UDF path", scriptErr)
		return false
	}

	tx := bt.NewTx()
	tx.Outputs = append(tx.Outputs, &bt.Output{
		// Per-process entropy → unique txid → unique record key, so two
		// instances probing the same cluster never interleave on one record.
		Satoshis:      (uint64(os.Getpid())<<32 | uint64(time.Now().UnixNano())&0xFFFFFFFF) | 1,
		LockingScript: lockingScript,
	})

	txHash := tx.TxIDChainHash()

	utxoHashes, hashErr := utxo.GetUtxoHashes(tx, txHash)
	if hashErr != nil || len(utxoHashes) != 1 {
		s.logger.Warnf("[teranode-native-op] spend probe utxo hash failed (%v, %d hashes); falling back to UDF path", hashErr, len(utxoHashes))
		return false
	}

	bins, binsErr := s.GetBinsToStore(tx, probeBlockHeight, nil, nil, nil, false, txHash, false, false, false, cohort.Unset, nil)
	if binsErr != nil || len(bins) != 1 {
		s.logger.Warnf("[teranode-native-op] spend probe record build failed (%v, %d batches); falling back to UDF path", binsErr, len(bins))
		return false
	}

	key, keyErr := aerospike.NewKey(s.namespace, s.setName, uaerospike.CalculateKeySource(txHash, probeVout, s.utxoBatchSize))
	if keyErr != nil {
		s.logger.Warnf("[teranode-native-op] spend probe key creation failed: %v; falling back to UDF path", keyErr)
		return false
	}

	if putErr := s.client.PutBins(policy, key, bins[0]...); putErr != nil {
		s.logger.Warnf("[teranode-native-op] spend probe record setup failed: %v; falling back to UDF path", putErr)
		return false
	}
	defer func() {
		_, _ = s.client.Delete(policy, key)
	}()

	spendOnce := func(spendingData *spendpkg.SpendingData) (*LuaMapResponse, bool) {
		if ctx.Err() != nil {
			s.logger.Warnf("[teranode-native-op] spend probe aborted by context: %v; falling back to UDF path", ctx.Err())
			return nil, false
		}

		items := []aerospike.MapValue{aerospike.NewMapValue(map[any]any{
			"idx":          0,
			"offset":       s.calculateOffsetForOutput(probeVout),
			"vOut":         probeVout,
			"utxoHash":     utxoHashes[0][:],
			"spendingData": spendingData.Bytes(),
		})}

		payload, encErr := encodeNativeOpPayload(subOpSpendMulti, []any{
			items, false, false, probeBlockHeight, s.settings.GetUtxoStoreBlockHeightRetention(),
		})
		if encErr != nil {
			s.logger.Warnf("[teranode-native-op] spend probe payload encode failed: %v; falling back to UDF path", encErr)
			return nil, false
		}

		rec, opErr := s.client.Operate(policy, key, aerospike.TeranodeModifyOp(nativeOpResultBin, payload))
		if opErr != nil {
			s.logger.Warnf("[teranode-native-op] spend probe operate failed: %v; falling back to UDF path", opErr)
			return nil, false
		}
		if rec == nil || rec.Bins == nil || rec.Bins[nativeOpResultBin] == nil {
			s.logger.Warnf("[teranode-native-op] spend probe returned no %q bin; %s; falling back to UDF path", nativeOpResultBin, describeAerospikeRecord(rec))
			return nil, false
		}

		res, parseErr := s.ParseLuaMapResponse(rec.Bins[nativeOpResultBin])
		if parseErr != nil {
			s.logger.Warnf("[teranode-native-op] spend probe returned unparsable response (value %s): %v; falling back to UDF path", describeAerospikeValue(rec.Bins[nativeOpResultBin]), parseErr)
			return nil, false
		}
		return res, true
	}

	firstRes, ok := spendOnce(spendpkg.NewSpendingData(&chainhash.Hash{0x01}, 0))
	if !ok {
		return false
	}
	if firstRes.Status != LuaStatusOK || len(firstRes.Errors) != 0 {
		s.logger.Warnf("[teranode-native-op] spend probe first spend not accepted (%+v); falling back to UDF path", firstRes)
		return false
	}

	secondRes, ok := spendOnce(spendpkg.NewSpendingData(&chainhash.Hash{0x02}, 0))
	if !ok {
		return false
	}

	if secondRes.Status != LuaStatusError {
		s.logger.Warnf("[teranode-native-op] spend probe double-spend was NOT rejected (%+v); native dispatcher does not enforce first-seen; falling back to UDF path", secondRes)
		return false
	}

	spentRejected := secondRes.ErrorCode == LuaErrorCodeSpent
	for _, e := range secondRes.Errors {
		if e.ErrorCode == LuaErrorCodeSpent {
			spentRejected = true
		}
	}
	if !spentRejected {
		s.logger.Warnf("[teranode-native-op] spend probe double-spend rejected with wrong error (%+v), want %s; falling back to UDF path", secondRes, LuaErrorCodeSpent)
		return false
	}

	return true
}

// initNativeTeranodeOps caches the native-op decision on the store and
// allocates the shared BatchWritePolicy used by every record the native-op
// path emits. Idempotent; safe to call from store construction.
//
// nativeOpBatchWritePolicy is shared across every NewBatchWrite the native
// path constructs (one allocation per Store, not per record). The Aerospike
// client only reads from this policy during BatchOperate, so concurrent reads
// from many goroutines are safe. Do NOT mutate the policy after this call.
func (s *Store) initNativeTeranodeOps(ctx context.Context) {
	supported := s.detectNativeTeranodeOpSupport(ctx)
	s.useNativeTeranodeOps.Store(supported)
	s.nativeOpBatchWritePolicy = aerospike.NewBatchWritePolicy()
	// UPDATE_ONLY: a mod-teranode invocation must never create a record. On
	// the UDF path a missing key runs the Lua function against no record and
	// returns TX_NOT_FOUND without persisting anything; the native dispatcher
	// lives out-of-repo, so pin the guarantee client-side. A missing record
	// therefore surfaces as a per-record KEY_NOT_FOUND_ERROR, which the result
	// handlers map to the same TX_NOT_FOUND semantics as the Lua status.
	s.nativeOpBatchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	if s.settings.Aerospike.UseNativeTeranodeOps && !supported {
		s.logger.Infof("[teranode-native-op] setting requested native ops but server " +
			"capability probe rejected; using UDF path")
	} else if supported {
		s.logger.Infof("[teranode-native-op] enabled (op type 200, sub_op_id wire format)")
	}
}
