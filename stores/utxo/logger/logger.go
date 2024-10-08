package logger

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func init() {
}

type Store struct {
	logger ulogger.Logger
	store  utxo.Store
}

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

			if folders[0] == "ubsv" {
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
	s.logger.Infof("[UTXOStore][logger][GetBlockHeight] %d : %s", height, caller())

	return height
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	err := s.store.SetMedianBlockTime(medianTime)
	s.logger.Infof("[UTXOStore][logger][SetMedianBlockTime] medianTime %d err %v : %s", medianTime, err, caller())

	return err
}

func (s *Store) GetMedianBlockTime() uint32 {
	res := s.store.GetMedianBlockTime()
	s.logger.Infof("[UTXOStore][logger][GetMedianBlockTime] %d : %s", res, caller())

	return res
}

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	s.logger.Infof("[UTXOStore][logger][Health] : %s", caller())
	return s.store.Health(ctx, checkLiveness)
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	data, err := s.store.Create(ctx, tx, blockHeight, opts...)
	inputDetails := make([]string, len(tx.Inputs))

	for i, input := range tx.Inputs {
		inputDetails[i] = fmt.Sprintf("{Input %d: PreviousTxID %s, PreviousTxOutIndex %d, SequenceNumber %d}",
			i, input.PreviousTxID(), input.PreviousTxOutIndex, input.SequenceNumber)
	}

	outputDetails := make([]string, len(tx.Outputs))
	for i, output := range tx.Outputs {
		outputDetails[i] = fmt.Sprintf("{Output %d: Satoshis %d, LockingScript %x}",
			i, output.Satoshis, output.LockingScript)
	}

	s.logger.Infof("[UTXOStore][logger][Create] tx %s, inputs: [%s], outputs: [%s], isCoinbase %t, blockHeight %d, lockTime %d, version %d, data %s, err %v : %s",
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

	return data, err
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	data, err := s.store.GetMeta(ctx, hash)
	s.logger.Infof("[UTXOStore][logger][GetMeta] hash %s data %v err %v : %s", hash.String(), data, err, caller())

	return data, err
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	data, err := s.store.Get(ctx, hash, fields...)
	s.logger.Infof("[UTXOStore][logger][Get] hash %s, fields %v data %v err %v : %s", hash.String(), fields, data, err, caller())

	return data, err
}

func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	err := s.store.Spend(ctx, spends, blockHeight)
	spendDetails := make([]string, len(spends))

	for i, spend := range spends {
		spendDetails[i] = fmt.Sprintf("{SpendingTxID: %s, TxID: %s, Vout: %d}", spend.SpendingTxID, spend.TxID, spend.Vout)
	}

	s.logger.Infof("[UTXOStore][logger][Spend] spends: [%s], blockHeight: %d, err: %v : %s",
		strings.Join(spendDetails, ", "), blockHeight, err, caller())

	return err
}

func (s *Store) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	err := s.store.UnSpend(ctx, spends)
	spendDetails := make([]string, len(spends))

	for i, spend := range spends {
		spendDetails[i] = fmt.Sprintf("{SpendingTxID: %s, TxID: %s, Vout: %d}", spend.SpendingTxID, spend.TxID, spend.Vout)
	}

	s.logger.Infof("[UTXOStore][logger][UnSpend] spends: [%s], err: %v : %s",
		strings.Join(spendDetails, ", "), err, caller())

	return err
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	err := s.store.Delete(ctx, hash)
	s.logger.Infof("[UTXOStore][logger][Delete] hash %s err %v : %s", hash.String(), err, caller())

	return err
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	err := s.store.SetMinedMulti(ctx, hashes, blockID)
	s.logger.Infof("[UTXOStore][logger][SetMinedMulti] hashes %v blockID %d err %v : %s", hashes, blockID, err, caller())

	return err
}

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	resp, err := s.store.GetSpend(ctx, spend)
	s.logger.Infof("[UTXOStore][logger][GetSpend] spend %v resp %v err %v : %s", spend, resp, err, caller())

	return resp, err
}

func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...string) error {
	err := s.store.BatchDecorate(ctx, unresolvedMetaDataSlice, fields...)
	s.logger.Infof("[UTXOStore][logger][BatchDecorate] unresolvedMetaDataSlice %v, fields %v err %v : %s", unresolvedMetaDataSlice, fields, err, caller())

	return err
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	err := s.store.PreviousOutputsDecorate(ctx, outpoints)
	s.logger.Infof("[UTXOStore][logger][PreviousOutputsDecorate] outpoints %v err %v : %s", outpoints, err, caller())

	return err
}

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxostore.Spend) error {
	err := s.store.FreezeUTXOs(ctx, spends)
	s.logger.Infof("[UTXOStore][logger][FreezeUTXOs] spends %v err %v : %s", spends, err, caller())

	return err
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxostore.Spend) error {
	err := s.store.UnFreezeUTXOs(ctx, spends)
	s.logger.Infof("[UTXOStore][logger][UnFreezeUTXOs] spends %v err %v : %s", spends, err, caller())

	return err
}

func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxostore.Spend, newUtxo *utxostore.Spend) error {
	err := s.store.ReAssignUTXO(ctx, utxo, newUtxo)
	s.logger.Infof("[UTXOStore][logger][ReAssignUTXO] utxo %v newUtxo %v err %v : %s", utxo, newUtxo, err, caller())

	return err
}
