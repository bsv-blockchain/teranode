package pebble

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/cockroachdb/pebble/v2"
)

type builtTx struct {
	hash     chainhash.Hash
	master   *masterRecord
	pages    map[uint32]*pageRecord
	hashes   map[uint32][]byte
	payload  []byte
	children [][]byte
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	built, md, err := s.buildTx(tx, blockHeight, options)
	if err != nil {
		return nil, err
	}

	unlock := s.lockStripes(built.hash[:])
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err = s.stageCreate(batch, built); err != nil {
		return nil, err
	}

	if err = batch.Commit(s.sync); err != nil {
		return nil, errors.NewStorageError("pebble: failed to commit create", err)
	}

	return md, nil
}

func (s *Store) stageCreate(batch *pebble.Batch, built *builtTx) error {
	if _, err := s.getValue(masterKey(built.hash[:])); err == nil {
		return errors.NewTxExistsError("pebble: transaction %s already exists", built.hash)
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return errors.NewStorageError("pebble: create existence check failed for %s", built.hash, err)
	}

	if err := batch.Set(masterKey(built.hash[:]), encodeMaster(built.master), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage master for %s", built.hash, err)
	}

	for page, rec := range built.pages {
		if err := batch.Set(pageKey(built.hash[:], page), encodePage(rec), nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage page %d for %s", page, built.hash, err)
		}
	}

	for page, hashes := range built.hashes {
		if err := batch.Set(hashesKey(built.hash[:], page), hashes, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage hashes page %d for %s", page, built.hash, err)
		}
	}

	if err := batch.Set(payloadKey(built.hash[:]), built.payload, nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage payload for %s", built.hash, err)
	}

	if built.master.unminedSince > 0 {
		if err := batch.Set(heightIndexKey(prefixUnminedIdx, built.master.unminedSince, built.hash[:]), nil, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage unmined index for %s", built.hash, err)
		}
	}

	if built.master.deleteAtHeight > 0 {
		if err := batch.Set(heightIndexKey(prefixDAHIdx, built.master.deleteAtHeight, built.hash[:]), nil, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage dah index for %s", built.hash, err)
		}
	}

	if built.master.flags&flagConflicting != 0 {
		if err := batch.Set(conflictIdxKey(built.hash[:]), nil, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage conflict index for %s", built.hash, err)
		}

		createdAt := make([]byte, 8)
		putInt64(createdAt, built.master.createdAt)

		for _, parent := range built.children {
			if err := batch.Set(childrenKey(parent, built.hash[:]), createdAt, nil); err != nil {
				return errors.NewStorageError("pebble: failed to stage conflicting child for %s", built.hash, err)
			}

			if err := batch.Set(childrenRevKey(built.hash[:], parent), nil, nil); err != nil {
				return errors.NewStorageError("pebble: failed to stage conflicting child reverse for %s", built.hash, err)
			}
		}
	}

	return nil
}

func (s *Store) buildTx(tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*builtTx, *meta.Data, error) {
	var (
		md  *meta.Data
		err error
	)

	if options.SkipExtendedInputs {
		md, err = util.TxMetaDataFromTxNoFee(tx)
	} else {
		md, err = util.TxMetaDataFromTx(tx)
	}

	if err != nil {
		return nil, nil, errors.NewProcessingError("pebble: failed to build tx meta data", err)
	}

	if options.Conflicting {
		md.Conflicting = true
	}

	if options.Locked {
		md.Locked = true
	}

	txHash := tx.TxIDChainHash()
	if options.TxID != nil {
		txHash = options.TxID
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	m := &masterRecord{
		outputCount: uint32(len(tx.Outputs)), //nolint:gosec
		version:     tx.Version,
		lockTime:    tx.LockTime,
		fee:         md.Fee,
		sizeInBytes: md.SizeInBytes,
		createdAt:   time.Now().UnixMilli(),
	}

	if isCoinbase {
		m.flags |= flagCoinbase
		m.coinbaseSpendingHeight = blockHeight + uint32(s.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec
	}

	if options.Frozen {
		m.flags |= flagFrozen
	}

	if options.Conflicting {
		m.flags |= flagConflicting
	}

	if options.Locked {
		m.flags |= flagLocked
	}

	if m.outputCount > s.pageSize {
		m.pagesTotal = (m.outputCount - 1) / s.pageSize
	}

	built := &builtTx{
		hash:   *txHash,
		master: m,
		pages:  make(map[uint32]*pageRecord),
		hashes: make(map[uint32][]byte),
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight

	for page := uint32(0); page <= m.pagesTotal; page++ {
		start := page * s.pageSize
		end := min(start+s.pageSize, m.outputCount)
		slots := end - start
		spends := make([]byte, int(slots)*slotSpendSize)
		hashes := make([]byte, int(slots)*slotHashSize)
		spendable := uint32(0)

		for v := start; v < end; v++ {
			output := tx.Outputs[v]
			if output == nil || !utxo.ShouldStoreOutputAsUTXO(output, blockHeight, genesisHeight) {
				continue
			}

			utxoHash, err := util.UTXOHashFromOutput(txHash, output, v)
			if err != nil {
				return nil, nil, errors.NewProcessingError("pebble: failed to compute utxo hash for vout %d", v, err)
			}

			copy(hashes[(v-start)*slotHashSize:], utxoHash[:])

			spendable++
		}

		m.totalCount += spendable

		if page == 0 {
			m.page0Count = spendable
			m.spends = spends
		} else {
			if spendable == 0 {
				m.pagesSpent++
			}

			built.pages[page] = &pageRecord{spendableCount: spendable, spends: spends}
		}

		built.hashes[page] = hashes
	}

	if len(options.MinedBlockInfos) > 0 {
		m.blockRefs = packBlockRefs(options.MinedBlockInfos)
	} else {
		m.unminedSince = int64(blockHeight)
	}

	if !options.Conflicting && len(options.MinedBlockInfos) > 0 && m.totalCount == 0 && s.retention() > 0 {
		m.deleteAtHeight = int64(blockHeight) + s.retention()
	}

	inputItems := make([][]byte, len(tx.Inputs))

	for i, input := range tx.Inputs {
		if options.SkipExtendedInputs {
			stripped := *input
			stripped.PreviousTxSatoshis = 0
			stripped.PreviousTxScript = nil
			inputItems[i] = stripped.ExtendedBytes(false)
		} else {
			inputItems[i] = input.ExtendedBytes(false)
		}
	}

	outputItems := make([][]byte, len(tx.Outputs))
	for i, output := range tx.Outputs {
		outputItems[i] = output.Bytes()
	}

	built.payload = packOffsetBlob([][]byte{packOffsetBlob(inputItems), packOffsetBlob(outputItems)})

	if options.Conflicting {
		for _, input := range tx.Inputs {
			built.children = append(built.children, input.PreviousTxIDChainHash()[:])
		}
	}

	return built, md, nil
}

func putInt64(b []byte, v int64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}
