package aerospike

import (
	"context"
	"os"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// probeLaneSlotPosition is the position the probe spends with: lane 200 sets the top bit, so a
// server that stored anything but the full 64 bits fails the probe.
var probeLaneSlotPosition = lane.Pack(200, 1)

// probeLaneSlotWrites reports whether the server records a spend's lane position in the parent's
// slot, the spendLanePos bin. It writes a one-output probe record, spends it with a lane position
// through spendMulti (the native sub-op when that is in use, the Lua UDF otherwise), reads the bin
// back and deletes the record. It does not probe the expression operate
// (Aerospike.EnableSpendFilterExpressions with utxoBatchSize 1), which sends no lane position:
// countLaneSpends counts those spends as not carried, including a FILTERED_OUT spend retried
// through Lua that does write the bin. It runs only with lane positions on, and its answer only
// decides how spends are counted: a spend carries its position either way, and a server that
// predates the lanePos key ignores it. Every failure answers false, with a warning.
//
// Like probeConflictingIndex, the probe record is no transaction anything else reads: it carries no
// txID, unminedSince or createdAt bin, so block assembly's unmined iterator and the pruner's
// old-unmined query never pick it up, and its spend runs with a block height retention of 0, so it
// never gets a deleteAtHeight. It is written without a TTL, because a namespace with expiration
// disabled (nsup-period=0) refuses a write that carries one. The deferred delete removes it; a
// process that dies in between leaves it behind, harmless but permanent.
func (s *Store) probeLaneSlotWrites(ctx context.Context) bool {
	warn := func(format string, args ...interface{}) bool {
		s.logger.Warnf("[lane] slot position probe: "+format+"; spends are counted as not carrying lane positions", args...)
		return false
	}

	if s.utxoBatchSize <= 0 {
		return warn("invalid utxoBatchSize %d", s.utxoBatchSize)
	}

	lockingScript, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	if err != nil {
		return warn("script: %v", err)
	}

	tx := bt.NewTx()
	tx.Outputs = append(tx.Outputs, &bt.Output{
		// Per-process entropy gives a unique txid, so two instances probing one cluster never
		// share a record.
		Satoshis:      (uint64(os.Getpid())<<32 | uint64(time.Now().UnixNano())&0xFFFFFFFF) | 1, //nolint:gosec // entropy only
		LockingScript: lockingScript,
	})

	txHash := tx.TxIDChainHash()

	utxoHashes, err := utxo.GetUtxoHashes(tx, txHash)
	if err != nil || len(utxoHashes) != 1 {
		return warn("utxo hash: %v", err)
	}

	bins, err := s.GetBinsToStore(tx, 1, nil, nil, nil, false, txHash, false, false, false, false, lane.None, nil)
	if err != nil || len(bins) != 1 {
		return warn("record: %v", err)
	}

	probeBins := withoutProbeBins(bins[0])

	key, err := aerospike.NewKey(s.namespace, s.setName, uaerospike.CalculateKeySource(txHash, 0, s.utxoBatchSize))
	if err != nil {
		return warn("key: %v", err)
	}

	policy := aerospike.NewWritePolicy(0, aerospike.TTLDontExpire)

	// Deleted even when the write reports an error, which may have applied.
	defer func() {
		if _, delErr := s.client.Delete(policy, key); delErr != nil {
			s.logger.Warnf("[lane] slot position probe: could not delete the probe record %s, delete it by hand: %v", key.Value(), delErr)
		}
	}()

	if err := s.client.PutBins(policy, key, probeBins...); err != nil {
		return warn("record setup: %v", err)
	}

	if ctx.Err() != nil {
		return warn("context: %v", ctx.Err())
	}

	item := s.createSpendMapValue(0, &batchSpend{
		spend: &utxo.Spend{
			TxID:         txHash,
			Vout:         0,
			UTXOHash:     utxoHashes[0],
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{0x01}, 0),
		},
		lanePos: probeLaneSlotPosition,
	})

	record := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, key, subOpSpendMulti, "spendMulti",
		[]aerospike.MapValue{item}, false, false, uint32(1), uint32(0))

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), []aerospike.BatchRecordIfc{record}); err != nil {
		return warn("spend: %v", err)
	}

	res, err := s.teranodeBatchRecordResponse("[lane] slot position probe", record)
	if err != nil {
		return warn("spend response: %v", err)
	}

	if res.Status != LuaStatusOK || len(res.Errors) != 0 {
		return warn("spend not accepted (%+v)", res)
	}

	rec, err := s.client.Get(nil, key, fields.SpendingLanePositions.String())
	if err != nil || rec == nil {
		return warn("read back: %v", err)
	}

	positions, err := decodeSpendingLanePositions(rec.Bins[fields.SpendingLanePositions.String()], 1)
	if err != nil {
		return warn("decode: %v", err)
	}

	if positions[0] != probeLaneSlotPosition {
		return warn("the server stored %#x for %#x; this server does not record lane positions", positions[0], probeLaneSlotPosition)
	}

	s.logger.Infof("[lane] slot position probe: the server records spenders' lane positions")

	return true
}

// withoutProbeBins is a probe record's bins without the ones that would make it a transaction other
// readers see: txID, unminedSince and createdAt. Neither teranode.lua nor the C module's spend reads
// them.
func withoutProbeBins(bins []*aerospike.Bin) []*aerospike.Bin {
	kept := make([]*aerospike.Bin, 0, len(bins))

	for _, bin := range bins {
		switch bin.Name {
		case fields.TxID.String(), fields.UnminedSince.String(), fields.CreatedAt.String():
			continue
		}

		kept = append(kept, bin)
	}

	return kept
}

// LaneSlotPositionsRecorded reports what the start-up probe found: whether the server records a
// spender's lane position in the parent's slot. It is false with lane positions off, where the
// probe does not run.
func (s *Store) LaneSlotPositionsRecorded() bool {
	return s.laneSlotWrites.Load()
}
