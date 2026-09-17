package aerospike

import (
	"context"
	"sort"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// freezeRecord is one output's alert-system freeze record as read back from the
// utxoFreezeFrom/Until/Exp bins. present is the utxoFreezeFrom entry: it is always
// written when a freeze is recorded, whatever its value, so its presence says "this
// outpoint carries a freeze record" independently of the 0xFF sentinel in the
// spending-data slot, which a block-validation spend may legitimately overwrite
// (issue #1422). Mirrors freezeRecordFromMaps in teranode.lua.
type freezeRecord struct {
	present       bool
	from          uint32
	until         uint32
	policyExpires bool
}

// readFreezeRecord reads output offset's freeze record out of a record's bins. Absent
// bins or entries read as "no record"; a record with no until entry has no end.
func readFreezeRecord(bins aerospike.BinMap, offset int) freezeRecord {
	from, ok := mapBinEntryInt(bins, fields.UtxoFreezeFrom, offset)
	if !ok {
		return freezeRecord{}
	}

	until, _ := mapBinEntryInt(bins, fields.UtxoFreezeUntil, offset)

	_, policyExpires := mapBinEntry(bins, fields.UtxoFreezeExp, offset)

	return freezeRecord{
		present:       true,
		from:          uint32(from),  // nolint:gosec // heights are written as uint32 by FreezeUTXOs
		until:         uint32(until), // nolint:gosec // heights are written as uint32 by FreezeUTXOs
		policyExpires: policyExpires,
	}
}

// readFreezeRecords collects every output's freeze record for a transaction into the
// shape meta.Data carries: the main record's bins, plus the freeze bins of each extra
// record that the main record's utxoFreezeRecs marker names — a record on a paginated
// output lives on the extra record that holds that output. Only marked extra records are
// read, so a transaction with no frozen output costs no extra round trip however many
// outputs it has; this read rides on block validation's parent check (see
// model.getParentTxMetaBlockIDs). Returns nil when no output carries a record.
//
// A marked extra record that cannot be read is storage damage, not evidence that the
// output is free — the full UTXO read classifies the same condition the same way — and
// this result decides whether a block is valid, so it is an error rather than a skip.
//
// The legacy 0xFF sentinel in the utxos list is deliberately not consulted here. An
// output frozen before the bins existed carries only the sentinel, but the sentinel also
// occupies the spending-data slot, so such an output is unspent by construction and every
// spend of it goes through spendMulti, which reads the sentinel as an always-active
// record. There is therefore no already-validated spender for this block-level check to
// catch, and reading the utxos bins of every parent here would put the largest bin on
// the hottest read in block validation. The one path that overwrites a sentinel without
// that check is the below-checkpoint bypass, whose block is canonical by definition.
func (s *Store) readFreezeRecords(ctx context.Context, txID *chainhash.Hash, bins aerospike.BinMap) (map[uint32]meta.FreezeRecord, error) {
	records := collectFreezeRecords(bins, 0, nil)

	extraRecordNums := markedFreezeExtraRecords(bins)
	if len(extraRecordNums) == 0 {
		return records, nil
	}

	policy := util.GetAerospikeReadPolicy(s.settings)

	for _, recordNum := range extraRecordNums {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		extraKey, err := aerospike.NewKey(s.namespace, s.setName, uaerospike.CalculateKeySourceInternal(txID, uint32(recordNum))) // nolint:gosec
		if err != nil {
			return nil, errors.NewProcessingError("failed to create key for extra record", err)
		}

		extraRecord, err := s.client.Get(policy, extraKey,
			fields.UtxoFreezeFrom.String(), fields.UtxoFreezeUntil.String(), fields.UtxoFreezeExp.String())
		if err != nil {
			return nil, errors.NewStorageError("[readFreezeRecords][%s] failed to get extra record %d, which the freeze marker names", txID.String(), recordNum, err)
		}

		if extraRecord == nil || extraRecord.Bins == nil {
			return nil, errors.NewStorageError("[readFreezeRecords][%s] extra record %d, which the freeze marker names, has no bins — torn or mis-keyed record", txID.String(), recordNum)
		}

		records = collectFreezeRecords(extraRecord.Bins, recordNum*s.utxoBatchSize, records)
	}

	return records, nil
}

// markedFreezeExtraRecords returns, in ascending order, the extra record numbers named
// by the main record's utxoFreezeRecs marker. The marker is a map keyed by record
// number, which Aerospike hands back as map[interface{}]interface{} with int keys.
func markedFreezeExtraRecords(bins aerospike.BinMap) []int {
	raw, found := bins[fields.UtxoFreezeRecs.String()]
	if !found || raw == nil {
		return nil
	}

	m, ok := raw.(map[interface{}]interface{})
	if !ok {
		return nil
	}

	nums := make([]int, 0, len(m))

	for k := range m {
		if n, ok := k.(int); ok && n > 0 {
			nums = append(nums, n)
		}
	}

	sort.Ints(nums)

	return nums
}

// collectFreezeRecords adds the freeze records found in one record's bins to records,
// keyed by absolute output index (baseOffset + map key). The utxoFreezeFrom map's keys
// are the outputs carrying a record; its presence is the record.
func collectFreezeRecords(bins aerospike.BinMap, baseOffset int, records map[uint32]meta.FreezeRecord) map[uint32]meta.FreezeRecord {
	raw, found := bins[fields.UtxoFreezeFrom.String()]
	if !found || raw == nil {
		return records
	}

	fromMap, ok := raw.(map[interface{}]interface{})
	if !ok {
		return records
	}

	for k := range fromMap {
		offset, ok := k.(int)
		if !ok {
			continue
		}

		rec := readFreezeRecord(bins, offset)
		if !rec.present {
			continue
		}

		if records == nil {
			records = make(map[uint32]meta.FreezeRecord)
		}

		records[uint32(baseOffset+offset)] = meta.FreezeRecord{ // nolint:gosec
			From:          rec.from,
			Until:         rec.until,
			PolicyExpires: rec.policyExpires,
		}
	}

	return records
}

// mapBinEntry returns the entry for offset in a per-offset map bin, if the bin and the
// entry exist. Aerospike hands map bins back as map[interface{}]interface{} keyed by the
// integer offset, as the utxoSpendableIn reader in GetSpend already relies on.
func mapBinEntry(bins aerospike.BinMap, bin fields.FieldName, offset int) (interface{}, bool) {
	raw, found := bins[bin.String()]
	if !found || raw == nil {
		return nil, false
	}

	m, ok := raw.(map[interface{}]interface{})
	if !ok {
		return nil, false
	}

	v, found := m[offset]
	if !found || v == nil {
		return nil, false
	}

	return v, true
}

func mapBinEntryInt(bins aerospike.BinMap, bin fields.FieldName, offset int) (int, bool) {
	v, ok := mapBinEntry(bins, bin, offset)
	if !ok {
		return 0, false
	}

	n, ok := v.(int)

	return n, ok
}
