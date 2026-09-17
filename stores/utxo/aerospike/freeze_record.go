package aerospike

import (
	"context"

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
// shape meta.Data carries, from the main record's bins plus — only when the transaction
// is paginated — each extra record's freeze bins, since a record on a paginated output
// lives on the extra record that holds that output. Returns nil when no output carries a
// record, which is every transaction until an alert names one of its outputs.
func (s *Store) readFreezeRecords(ctx context.Context, txID *chainhash.Hash, bins aerospike.BinMap) (map[uint32]meta.FreezeRecord, error) {
	records := collectFreezeRecords(bins, 0, nil)

	totalExtraRecs, ok := bins[fields.TotalExtraRecs.String()].(int)
	if !ok || totalExtraRecs <= 0 {
		return records, nil
	}

	policy := util.GetAerospikeReadPolicy(s.settings)

	for recordNum := 1; recordNum <= totalExtraRecs; recordNum++ {
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
			if errors.Is(err, aerospike.ErrKeyNotFound) {
				continue
			}

			return nil, errors.NewStorageError("failed to get extra record", err)
		}

		if extraRecord != nil && extraRecord.Bins != nil {
			records = collectFreezeRecords(extraRecord.Bins, recordNum*s.utxoBatchSize, records)
		}
	}

	return records, nil
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
