package aerospike

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

func describeAerospikeBatchRecord(batchRecord aerospike.BatchRecordIfc) string {
	if batchRecord == nil {
		return "batchRecord=<nil>"
	}

	rec := batchRecord.BatchRec()
	if rec == nil {
		return "batchRecord.BatchRec=<nil>"
	}

	parts := []string{
		fmt.Sprintf("resultCode=%s", rec.ResultCode.String()),
		fmt.Sprintf("inDoubt=%t", rec.InDoubt),
	}

	if rec.Key != nil {
		parts = append(parts, fmt.Sprintf("key=%s", rec.Key.String()))
	}

	if rec.Err != nil {
		parts = append(parts, fmt.Sprintf("recordErr=%s", rec.Err.Error()))
	}

	if rec.Record == nil {
		parts = append(parts, "record=<nil>")
		return strings.Join(parts, ", ")
	}

	parts = append(parts, describeAerospikeRecord(rec.Record))
	return strings.Join(parts, ", ")
}

func describeAerospikeRecord(record *aerospike.Record) string {
	if record == nil {
		return "record=<nil>"
	}

	// Generation distinguishes a stale-read failure from a missing record, and
	// the originating node identifies which cluster member served the result —
	// the signature of a partially-upgraded cluster, where writes fail only on
	// the nodes that have not been upgraded yet. Both are worth the few
	// characters even when Bins is absent.
	parts := []string{fmt.Sprintf("generation=%d", record.Generation)}

	if record.Node != nil {
		parts = append(parts, fmt.Sprintf("node=%s", record.Node.String()))
	}

	if record.Bins == nil {
		parts = append(parts, "bins=<nil>")
	} else {
		parts = append(parts, fmt.Sprintf("bins=%s", describeAerospikeBins(record.Bins)))
	}

	return strings.Join(parts, ", ")
}

// binDiagnosticPriority ranks the bins that actually explain a failed
// BatchOperate — the UTXO accounting counters and the state flags a spend or
// setMined decision turns on. Bins listed here are rendered before any others.
//
// Without this, plain alphabetical ordering plus truncation is actively
// misleading: fields.go defines 33 bin names, so an alphabetical cut always
// renders blockHeights…deletedChildren and always discards spentUtxos,
// totalUtxos, recordUtxos, utxos, utxoSpendableIn and spendingHeight.
var binDiagnosticPriority = map[string]int{
	fields.SpentUtxos.String():      0,
	fields.TotalUtxos.String():      1,
	fields.RecordUtxos.String():     2,
	fields.Utxos.String():           3,
	fields.UtxoSpendableIn.String(): 4,
	fields.SpendingHeight.String():  5,
	fields.SpentExtraRecs.String():  6,
	fields.TotalExtraRecs.String():  7,
	fields.Conflicting.String():     8,
	fields.Locked.String():          9,
	fields.External.String():        10,
	fields.DeleteAtHeight.String():  11,
	fields.PreserveUntil.String():   12,
}

func describeAerospikeBins(bins aerospike.BinMap) string {
	if len(bins) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(bins))
	for key := range bins {
		keys = append(keys, key)
	}

	// Priority bins first, then alphabetical for everything else so the output
	// stays stable across calls.
	sort.Slice(keys, func(i, j int) bool {
		pi, iPrioritised := binDiagnosticPriority[keys[i]]
		pj, jPrioritised := binDiagnosticPriority[keys[j]]

		if iPrioritised != jPrioritised {
			return iPrioritised
		}

		if iPrioritised && pi != pj {
			return pi < pj
		}

		return keys[i] < keys[j]
	})

	// Sized to cover every prioritised bin plus a few of the alphabetical tail,
	// keeping the line bounded without cutting into the bins that matter.
	const maxBins = 16

	parts := make([]string, 0, min(len(keys), maxBins)+1)

	for idx, key := range keys {
		if idx == maxBins {
			parts = append(parts, fmt.Sprintf("...+%d more", len(keys)-idx))
			break
		}

		parts = append(parts, fmt.Sprintf("%s:%s", key, describeAerospikeValue(bins[key])))
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// maxStringValueLen bounds a rendered string bin so one oversized value cannot
// dominate the log line.
const maxStringValueLen = 64

func describeAerospikeValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "<nil>"
	case []byte:
		return fmt.Sprintf("%T(len=%d)", value, len(v))
	case []interface{}:
		return fmt.Sprintf("%T(len=%d)", value, len(v))
	case map[interface{}]interface{}:
		return fmt.Sprintf("%T(len=%d)", value, len(v))
	case map[string]interface{}:
		return fmt.Sprintf("%T(len=%d)", value, len(v))
	case aerospike.OpResults:
		return fmt.Sprintf("%T(len=%d)", value, len(v))
	case string:
		if len(v) > maxStringValueLen {
			return fmt.Sprintf("%q...(len=%d)", v[:maxStringValueLen], len(v))
		}

		return fmt.Sprintf("%q", v)
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		// The counters and flags that explain a batch failure (spentUtxos,
		// totalUtxos, conflicting, locked, …) land here. Rendering %T would print
		// "int" / "bool" and tell the reader nothing.
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%T", value)
	}
}

func describeChainHash(hash *chainhash.Hash) string {
	if hash == nil {
		return "<nil>"
	}
	return hash.String()
}

func describeUTXOSpend(spend *utxo.Spend) string {
	if spend == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s:%d", describeChainHash(spend.TxID), spend.Vout)
}
