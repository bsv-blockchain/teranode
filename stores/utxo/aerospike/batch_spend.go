package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// SpendResult reports the outcome of a single spend operation within BatchSpend.
// Index i in the result slice corresponds to index i in the input spends slice.
type SpendResult struct {
	// Err is non-nil when the Lua UDF rejected this spend (e.g. double-spend,
	// frozen, tx-not-found). A nil Err means the spend succeeded.
	Err error
}

// BatchSpend marks parent outputs as spent. It accepts the same []*utxo.Spend
// slice that the existing single-threaded Spend path accepts, groups them by
// parent record (TxID), and issues a single BatchOperate carrying one BatchUDF
// entry per distinct parent.  Per-record Lua failures are surfaced as
// SpendResult.Err on each affected entry; whole-call transport errors are
// returned as the second return value.
//
// blockHeight must be greater than zero (same contract as Spend).
// ignoreFlags mirrors the IgnoreFlags parameter accepted by Spend.
func (s *Store) BatchSpend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]SpendResult, error) {
	if len(spends) == 0 {
		return []SpendResult{}, nil
	}

	ignoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	ignoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	blockHeightRetention := s.settings.GetUtxoStoreBlockHeightRetention()

	// --- group spends by Aerospike record key ---
	//
	// Each Aerospike record may hold a window of utxoBatchSize outputs.
	// CalculateKeySource computes the key for the record that owns vout,
	// and calculateOffsetForOutput gives the in-record offset.
	type recordGroup struct {
		key        *aerospike.Key
		txIDHash   [32]byte // first spend's TxID (all in group share it)
		spendItems []aerospike.MapValue
		inputIdxs  []int // index back into the original spends slice
	}

	// Maintain insertion order so result[i] matches spends[i].
	type groupKey struct {
		keyStr  string
		txIDKey [32]byte
	}
	groupOrder := make([]groupKey, 0, len(spends))
	groups := make(map[groupKey]*recordGroup, len(spends))

	for i, sp := range spends {
		if sp == nil {
			return nil, errors.NewProcessingError("[BatchSpend] spend at index %d is nil", i)
		}
		if sp.SpendingData == nil {
			return nil, errors.NewProcessingError("[BatchSpend][%s:%d] SpendingData is nil", sp.TxID.String(), sp.Vout)
		}
		if sp.UTXOHash == nil {
			return nil, errors.NewProcessingError("[BatchSpend][%s:%d] UTXOHash is nil", sp.TxID.String(), sp.Vout)
		}

		keySource := uaerospike.CalculateKeySource(sp.TxID, sp.Vout, s.utxoBatchSize)
		aeroKey, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return nil, errors.NewProcessingError("[BatchSpend][%s:%d] failed to build Aerospike key", sp.TxID.String(), sp.Vout, err)
		}

		var txIDKey [32]byte
		copy(txIDKey[:], sp.TxID[:])

		gk := groupKey{keyStr: string(keySource), txIDKey: txIDKey}
		g, exists := groups[gk]
		if !exists {
			g = &recordGroup{
				key: aeroKey,
			}
			copy(g.txIDHash[:], sp.TxID[:])
			groups[gk] = g
			groupOrder = append(groupOrder, gk)
		}

		offset := s.calculateOffsetForOutput(sp.Vout)
		g.spendItems = append(g.spendItems, aerospike.NewMapValue(map[any]any{
			"idx":          i,
			"offset":       offset,
			"vOut":         sp.Vout,
			"utxoHash":     sp.UTXOHash[:],
			"spendingData": sp.SpendingData.Bytes(),
		}))
		g.inputIdxs = append(g.inputIdxs, i)
	}

	// --- build BatchUDF records in insertion order ---
	batchRecords := make([]aerospike.BatchRecordIfc, len(groupOrder))
	for i, gk := range groupOrder {
		g := groups[gk]

		useLuaPackage := LuaPackage
		if s.settings.Aerospike.SeparateSpendUDFModuleCount > 0 {
			useLuaPackage = s.spendLuaPackages[g.txIDHash[0]%uint8(s.settings.Aerospike.SeparateSpendUDFModuleCount)] //nolint:gosec
		}

		batchRecords[i] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			g.key,
			useLuaPackage,
			"spendMulti",
			aerospike.NewValue(g.spendItems),
			aerospike.NewValue(ignoreConflicting),
			aerospike.NewValue(ignoreLocked),
			aerospike.NewValue(blockHeight),
			aerospike.NewValue(blockHeightRetention),
		)
	}

	if asErr := s.client.BatchOperate(batchPolicy, batchRecords); asErr != nil {
		return nil, errors.NewStorageError("[BatchSpend] BatchOperate failed", asErr)
	}

	// --- map results back to input indexes ---
	results := make([]SpendResult, len(spends))

	for i, gk := range groupOrder {
		g := groups[gk]
		rec := batchRecords[i].BatchRec()

		// Transport-level error for this record — propagate to all spends in group.
		if rec.Err != nil {
			for _, idx := range g.inputIdxs {
				results[idx].Err = errors.NewStorageError("[BatchSpend][%s] batch record error", spends[g.inputIdxs[0]].TxID.String(), rec.Err)
			}
			continue
		}

		if rec.Record == nil || rec.Record.Bins == nil || rec.Record.Bins[LuaSuccess.String()] == nil {
			for _, idx := range g.inputIdxs {
				results[idx].Err = errors.NewProcessingError("[BatchSpend][%s] nil/empty response from spendMulti", spends[g.inputIdxs[0]].TxID.String())
			}
			continue
		}

		parsed, parseErr := s.ParseLuaMapResponse(rec.Record.Bins[LuaSuccess.String()])
		if parseErr != nil {
			for _, idx := range g.inputIdxs {
				results[idx].Err = errors.NewProcessingError("[BatchSpend][%s] failed to parse Lua response", spends[g.inputIdxs[0]].TxID.String(), parseErr)
			}
			continue
		}

		switch parsed.Status {
		case LuaStatusOK:
			// All spends in this group succeeded — results[idx].Err remains nil.

		case LuaStatusError:
			if parsed.Message != "" {
				// Record-level error (tx not found, frozen, locked, etc.).
				generalErr := s.createGeneralError(parsed.ErrorCode, spends[g.inputIdxs[0]].TxID, blockHeight, 0, parsed.Message)
				for _, idx := range g.inputIdxs {
					results[idx].Err = generalErr
				}
			} else if parsed.Errors != nil {
				// Per-spend errors keyed by the original input index.
				for _, idx := range g.inputIdxs {
					if errInfo, hasErr := parsed.Errors[idx]; hasErr {
						results[idx].Err = batchSpendError(errInfo, spends[idx])
					}
				}
			} else {
				generalErr := errors.NewStorageError("[BatchSpend][%s] Lua returned ERROR with no details", spends[g.inputIdxs[0]].TxID.String())
				for _, idx := range g.inputIdxs {
					results[idx].Err = generalErr
				}
			}
		}
	}

	return results, nil
}

// batchSpendError converts a per-UTXO LuaErrorInfo into a typed teranode error
// using the spend data available from the original utxo.Spend entry. This is
// the per-record equivalent of (*Store).createSpendError but operates on
// *utxo.Spend rather than the internal *batchSpend type.
func batchSpendError(errInfo LuaErrorInfo, sp *utxo.Spend) error {
	txID := sp.TxID
	vout := sp.Vout

	switch errInfo.ErrorCode {
	case LuaErrorCodeSpent:
		if errInfo.SpendingData != "" {
			spendingData, parseErr := spendpkg.NewSpendingDataFromString(errInfo.SpendingData)
			if parseErr != nil {
				return errors.NewStorageError("[BatchSpend][%s] invalid spending data in error: %s", txID.String(), errInfo.SpendingData)
			}
			return errors.NewUtxoSpentError(*txID, vout, *sp.UTXOHash, spendingData)
		}
		return errors.NewStorageError("[BatchSpend][%s] UTXO already spent but no spending data provided", txID.String())

	case LuaErrorCodeInvalidSpend:
		return errors.NewUtxoError("[BatchSpend][%s] invalid spend for vout %d: %s", txID.String(), vout, errInfo.Message)

	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[BatchSpend][%s] UTXO is frozen, vout %d: %s", txID.String(), vout, errInfo.Message)

	case LuaErrorCodeFrozenUntil:
		return errors.NewUtxoFrozenError("[BatchSpend][%s] UTXO frozen until block, vout %d: %s", txID.String(), vout, errInfo.Message)

	case LuaErrorCodeUtxoNotFound:
		return errors.NewTxNotFoundError("[BatchSpend][%s] UTXO not found for vout %d: %s", txID.String(), vout, errInfo.Message)

	case LuaErrorCodeUtxoHashMismatch:
		return errors.NewUtxoHashMismatchError("[BatchSpend][%s] UTXO hash mismatch for vout %d: %s", txID.String(), vout, errInfo.Message)

	case LuaErrorCodeUtxoInvalidSize:
		return errors.NewUtxoInvalidSize("[BatchSpend][%s] UTXO invalid size for vout %d: %s", txID.String(), vout, errInfo.Message)

	default:
		return errors.NewStorageError("[BatchSpend][%s] error for vout %d (code: %s): %s", txID.String(), vout, errInfo.ErrorCode, errInfo.Message)
	}
}
