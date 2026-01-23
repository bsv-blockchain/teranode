// Package logger provides a logging wrapper for UTXO store implementations.
//
// The logger package implements a decorator pattern that wraps any utxo.Store
// implementation with comprehensive logging capabilities. It logs all store
// operations including method calls, parameters, return values, and execution
// times for debugging and monitoring purposes.
//
// This is particularly useful for:
//   - Debugging UTXO store operations
//   - Performance monitoring and profiling
//   - Audit trails for transaction processing
//   - Development and testing environments
package logger

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Store wraps a utxo.Store implementation with logging capabilities.
// It implements the decorator pattern to add comprehensive logging to all store operations.
type Store struct {
	logger ulogger.Logger
	store  utxo.Store
}

// New creates a new logging wrapper around the provided UTXO store.
// All operations on the returned store will be logged with detailed information
// including method names, parameters, return values, and execution times.
func New(ctx context.Context, logger ulogger.Logger, store utxo.Store) utxo.Store {
	s := &Store{
		logger: logger,
		store:  store,
	}

	return s
}

func caller() string {
	var callers []string

	depth := 5

	for i := 0; i < depth; i++ {
		// Get caller information for the given depth
		pc, file, line, ok := runtime.Caller(2 + i)
		if !ok {
			break
		}

		// The file path is too long, it seems to start from the home directory.
		// We need to remove the first few folders to make it more readable.
		// TODO Is there a better way to do this? This only works for my local setup.
		folders := strings.Split(file, string(filepath.Separator))
		if len(folders) > 0 {
			if folders[0] == "github.com" {
				folders = folders[1:]
			}

			if folders[0] == "bitcoin-sv" {
				folders = folders[1:]
			}

			if folders[0] == "teranode" {
				folders = folders[1:]
			}
		}

		file = filepath.Join(folders...)

		// Get function name
		funcName := runtime.FuncForPC(pc).Name()
		funcPaths := strings.Split(funcName, "/")
		funcName = funcPaths[len(funcPaths)-1]

		// Add formatted caller information to the result
		callers = append(callers, fmt.Sprintf("called from %s: %s:%d", funcName, file, line))
	}

	return strings.Join(callers, ",")
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	return s.store.SetBlockHeight(blockHeight)
}

func (s *Store) GetBlockHeight() uint32 {
	height := s.store.GetBlockHeight()
	s.logger.Debugf("[UTXOStore][logger][GetBlockHeight] %d : %s", height, caller())

	return height
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	err := s.store.SetMedianBlockTime(medianTime)
	s.logger.Debugf("[UTXOStore][logger][SetMedianBlockTime] medianTime %d err %v : %s", medianTime, err, caller())

	return err
}

func (s *Store) GetMedianBlockTime() uint32 {
	res := s.store.GetMedianBlockTime()
	s.logger.Debugf("[UTXOStore][logger][GetMedianBlockTime] %d : %s", res, caller())

	return res
}

func (s *Store) GetBlockState() utxo.BlockState {
	blockState := s.store.GetBlockState()
	s.logger.Debugf("[UTXOStore][logger][GetBlockState] height: %d, medianTime: %d : %s", blockState.Height, blockState.MedianTime, caller())

	return blockState
}

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	s.logger.Debugf("[UTXOStore][logger][Health] : %s", caller())
	return s.store.Health(ctx, checkLiveness)
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	data, err := s.store.Create(ctx, tx, blockHeight, opts...)
	inputDetails := make([]string, len(tx.Inputs))

	for i, input := range tx.Inputs {
		inputDetails[i] = fmt.Sprintf("{Input %d: PreviousTxID %s, PreviousTxOutIndex %d, SequenceNumber %d}",
			i, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, input.SequenceNumber)
	}

	outputDetails := make([]string, len(tx.Outputs))
	skipLogging := false

	for i, output := range tx.Outputs {
		if output == nil {
			skipLogging = true
			break
		}

		outputDetails[i] = fmt.Sprintf("{Output %d: Satoshis %d}",
			i, output.Satoshis)
	}

	if !skipLogging {
		s.logger.Debugf("[UTXOStore][logger][Create] tx %s, inputs: [%s], outputs: [%s], isCoinbase %t, blockHeight %d, lockTime %d, version %d, data %s, err %v : %s",
			tx.TxIDChainHash(),
			strings.Join(inputDetails, ", "),
			strings.Join(outputDetails, ", "),
			tx.IsCoinbase(),
			blockHeight,
			tx.LockTime,
			tx.Version,
			data.String(),
			err,
			caller())
	}

	return data, err
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	err := s.store.GetMeta(ctx, hash, data)
	s.logger.Debugf("[UTXOStore][logger][GetMeta] hash %s data %v err %v : %s", hash.String(), data, err, caller())

	return err
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	data, err := s.store.Get(ctx, hash, fields...)
	s.logger.Debugf("[UTXOStore][logger][Get] hash %s, fields %v data %v err %v : %s", hash.String(), fields, data, err, caller())

	return data, err
}

func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	spends, err := s.store.Spend(ctx, tx, blockHeight, ignoreFlags...)
	spendDetails := make([]string, len(spends))

	for i, spend := range spends {
		spendDetails[i] = fmt.Sprintf("{SpendingData: %v, TxID: %s, Vout: %d}", spend.SpendingData, spend.TxID, spend.Vout)
	}

	s.logger.Debugf("[UTXOStore][logger][Spend] spends: [%s], blockHeight: %d, err: %v : %s",
		strings.Join(spendDetails, ", "), s.store.GetBlockHeight(), err, caller())

	return spends, err
}

func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	err := s.store.Unspend(ctx, spends, false)
	spendDetails := make([]string, len(spends))

	for i, spend := range spends {
		spendDetails[i] = fmt.Sprintf("{SpendingData: %v, TxID: %s, Vout: %d}", spend.SpendingData, spend.TxID, spend.Vout)
	}

	s.logger.Debugf("[UTXOStore][logger][Unspend] spends: [%s], err: %v : %s",
		strings.Join(spendDetails, ", "), err, caller())

	return err
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	err := s.store.Delete(ctx, hash)
	s.logger.Debugf("[UTXOStore][logger][Delete] hash %s err %v : %s", hash.String(), err, caller())

	return err
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	blockIDsMap, err := s.store.SetMinedMulti(ctx, hashes, minedBlockInfo)
	s.logger.Debugf("[UTXOStore][logger][SetMinedMulti] hashes %v blockID %d err %v : %s", hashes, minedBlockInfo.BlockID, err, caller())

	return blockIDsMap, err
}

func (s *Store) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return s.store.GetUnminedTxIterator(false)
}

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	resp, err := s.store.GetSpend(ctx, spend)
	s.logger.Debugf("[UTXOStore][logger][GetSpend] spend %v resp %v err %v : %s", spend, resp, err, caller())

	return resp, err
}

func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	err := s.store.BatchDecorate(ctx, unresolvedMetaDataSlice, fields...)
	s.logger.Debugf("[UTXOStore][logger][BatchDecorate] unresolvedMetaDataSlice %v, fields %v err %v : %s", unresolvedMetaDataSlice, fields, err, caller())

	return err
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	err := s.store.PreviousOutputsDecorate(ctx, tx)
	s.logger.Debugf("[UTXOStore][logger][PreviousOutputsDecorate] outpoints %v err %v : %s", tx, err, caller())

	return err
}

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	err := s.store.FreezeUTXOs(ctx, spends, tSettings)
	s.logger.Debugf("[UTXOStore][logger][FreezeUTXOs] spends %v err %v : %s", spends, err, caller())

	return err
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	err := s.store.UnFreezeUTXOs(ctx, spends, tSettings)
	s.logger.Debugf("[UTXOStore][logger][UnFreezeUTXOs] spends %v err %v : %s", spends, err, caller())

	return err
}

func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	err := s.store.ReAssignUTXO(ctx, utxo, newUtxo, tSettings)
	s.logger.Debugf("[UTXOStore][logger][ReAssignUTXO] utxo %v newUtxo %v err %v : %s", utxo, newUtxo, err, caller())

	return err
}

func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	conflictingHashes, err := s.store.GetCounterConflicting(ctx, txHash)
	s.logger.Debugf("[UTXOStore][logger][GetCounterConflicting] txHash %s err %v : %s", txHash.String(), err, caller())

	return conflictingHashes, err
}

func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	conflictingHashes, err := s.store.GetConflictingChildren(ctx, txHash)
	s.logger.Debugf("[UTXOStore][logger][GetConflictingChildren] txHash %s err %v : %s", txHash.String(), err, caller())

	return conflictingHashes, err
}

func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	spends, conflictingHashes, err := s.store.SetConflicting(ctx, txHashes, setValue)
	s.logger.Debugf("[UTXOStore][logger][SetConflicting] txHashes %v setValue %v err %v : %s", txHashes, setValue, err, caller())

	return spends, conflictingHashes, err
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	err := s.store.SetLocked(ctx, txHashes, setValue)
	s.logger.Debugf("[UTXOStore][logger][SetLocked] txHashes %v setValue %v err %v : %s", txHashes, setValue, err, caller())

	return err
}

func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	err := s.store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
	s.logger.Debugf("[UTXOStore][logger][MarkTransactionsOnLongestChain] txHashes %v onLongestChain %v err %v : %s", txHashes, onLongestChain, err, caller())

	return err
}

func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	hashes, err := s.store.QueryOldUnminedTransactions(ctx, cutoffBlockHeight)
	s.logger.Debugf("[UTXOStore][logger][QueryOldUnminedTransactions] cutoffBlockHeight %d count %d err %v : %s", cutoffBlockHeight, len(hashes), err, caller())

	return hashes, err
}

func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	err := s.store.PreserveTransactions(ctx, txIDs, preserveUntilHeight)
	s.logger.Debugf("[UTXOStore][logger][PreserveTransactions] txIDs count %d preserveUntilHeight %d err %v : %s", len(txIDs), preserveUntilHeight, err, caller())

	return err
}

func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	err := s.store.ProcessExpiredPreservations(ctx, currentHeight)
	s.logger.Debugf("[UTXOStore][logger][ProcessExpiredPreservations] currentHeight %d err %v : %s", currentHeight, err, caller())

	return err
}
