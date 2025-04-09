package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/legacy/wire"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/greatroar/blobloom"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

const GenesisBlockID = 0

// LastV1Block https://github.com/bitcoin/bips/blob/master/bip-0034.mediawiki
const LastV1Block = 227_835

var (
	emptyTX = &bt.Tx{}
)

type missingParentTx struct {
	parentTxHash chainhash.Hash
	txHash       chainhash.Hash
}

type Block struct {
	Header           *BlockHeader      `json:"header"`
	CoinbaseTx       *bt.Tx            `json:"coinbase_tx"`
	TransactionCount uint64            `json:"transaction_count"`
	SizeInBytes      uint64            `json:"size_in_bytes"`
	Subtrees         []*chainhash.Hash `json:"subtrees"`
	SubtreeSlices    []*util.Subtree   `json:"-"`
	Height           uint32            `json:"height"` // SAO - This can be left empty (i.e 0) as it is only used in legacy before the height was encoded in the coinbase tx (BIP-34)
	ID               uint32            `json:"id"`

	// local
	hash            *chainhash.Hash
	subtreeLength   uint64
	subtreeSlicesMu sync.RWMutex
	txMap           util.TxMap
	medianTimestamp uint32
	settings        *settings.Settings
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash, transactionCount uint64, sizeInBytes uint64, blockHeight uint32, id uint32, optionalSettings *settings.Settings) (*Block, error) {
	var tSettings *settings.Settings
	if optionalSettings != nil {
		tSettings = optionalSettings
	} else {
		tSettings = settings.NewSettings()
	}

	return &Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Subtrees:         subtrees,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		subtreeLength:    uint64(len(subtrees)),
		Height:           blockHeight,
		ID:               id,
		settings:         tSettings,
	}, nil
}

// NewBlockFromMsgBlock creates a new model.Block from a wire.MsgBlock
func NewBlockFromMsgBlock(msgBlock *wire.MsgBlock, optionalSettings *settings.Settings) (*Block, error) {
	if msgBlock == nil {
		return nil, errors.NewInvalidArgumentError("msgBlock is nil")
	}

	bitsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitsBytes, msgBlock.Header.Bits)

	nbits, err := NewNBitFromSlice(bitsBytes)
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to create NBit from Bits", err)
	}

	versionUint32, err := util.SafeInt32ToUint32(msgBlock.Header.Version)
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to convert version to uint32", err)
	}

	timestampUint32, err := util.SafeInt64ToUint32(msgBlock.Header.Timestamp.Unix())
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to convert timestamp to uint32", err)
	}

	header := &BlockHeader{
		Version:        versionUint32,
		HashPrevBlock:  &msgBlock.Header.PrevBlock,
		HashMerkleRoot: &msgBlock.Header.MerkleRoot,
		Timestamp:      timestampUint32,
		Bits:           *nbits,
		Nonce:          msgBlock.Header.Nonce,
	}

	if len(msgBlock.Transactions) == 0 {
		return nil, errors.NewBlockInvalidError("block has no transactions")
	}

	var coinbase bytes.Buffer
	if err = msgBlock.Transactions[0].Serialize(&coinbase); err != nil {
		return nil, errors.NewProcessingError("failed to serialize coinbase", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return nil, errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	txCount := uint64(len(msgBlock.Transactions))

	sizeInBytes, err := util.SafeIntToUint64(msgBlock.SerializeSize())
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to convert msgBlock size to uint64", err)
	}

	subtrees := make([]*chainhash.Hash, 0)

	subtree, err := util.NewIncompleteTreeByLeafCount(len(msgBlock.Transactions))
	if err != nil {
		return nil, errors.NewSubtreeError("failed to create subtree", err)
	}

	if err = subtree.AddCoinbaseNode(); err != nil {
		return nil, errors.NewSubtreeError("failed to add coinbase placeholder", err)
	}
	// TODO: support more than coinbase tx in the subtree
	// if txCount > 1 {
	// 	// loop through the transactions ignoring the first coinbase tx and add them to the subtrees list

	// 	// subtrees = append(subtrees, subtree.RootHash())
	// }

	// Create and return the new Block
	return NewBlock(header, coinbaseTx, subtrees, txCount, sizeInBytes, 0, 0, optionalSettings)
}

func NewBlockFromBytes(blockBytes []byte, optionalSettings *settings.Settings) (block *Block, err error) {
	startTime := time.Now()

	var tSettings *settings.Settings
	if optionalSettings != nil {
		tSettings = optionalSettings
	} else {
		tSettings = settings.NewSettings()
	}

	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())

		if r := recover(); r != nil {
			err = errors.NewBlockInvalidError("error creating block from bytes", r)
			fmt.Println("Recovered in NewBlockFromBytes", r)
		}
	}()

	// check minimal block size
	// 92 bytes is the bare minimum, but will not be valid
	if len(blockBytes) < 92 {
		return nil, errors.NewBlockInvalidError("block is too small")
	}

	block = &Block{
		settings: tSettings,
	}

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]

	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, errors.NewBlockInvalidError("invalid block header", err)
	}

	return readBlockFromReader(block, bytes.NewReader(blockBytes[80:]))
}

func NewBlockFromReader(blockReader io.Reader, optionalSettings *settings.Settings) (block *Block, err error) {
	startTime := time.Now()

	var tSettings *settings.Settings

	if optionalSettings != nil {
		tSettings = optionalSettings
	} else {
		tSettings = settings.NewSettings()
	}

	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())

		if r := recover(); r != nil {
			err = errors.NewBlockInvalidError("error creating block from reader", r)
			fmt.Println("Recovered in NewBlockFromReader", r)
		}
	}()

	block = &Block{
		settings: tSettings,
	}

	blockHeaderBytes := make([]byte, 80)
	// read the first 80 bytes as the block header
	_, err = io.ReadFull(blockReader, blockHeaderBytes)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading block header", err)
	}

	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, errors.NewBlockInvalidError("invalid block header", err)
	}

	return readBlockFromReader(block, blockReader)
}

func readBlockFromReader(block *Block, buf io.Reader) (*Block, error) {
	var err error

	// read the transaction count
	block.TransactionCount, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading transaction count", err)
	}

	// read the size in bytes
	block.SizeInBytes, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading size in bytes", err)
	}

	// read the length of the subtree list
	block.subtreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading subtree length", err)
	}

	// read the subtree list
	var (
		hashBytes   [32]byte
		subtreeHash *chainhash.Hash
	)

	block.Subtrees = make([]*chainhash.Hash, 0, block.subtreeLength)

	for i := uint64(0); i < block.subtreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, errors.NewBlockInvalidError("error reading subtree hash %d/%d", i, block.subtreeLength, err)
		}

		subtreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, errors.NewBlockInvalidError("error creating subtree hash", err)
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	var coinbaseTx bt.Tx
	if _, err = coinbaseTx.ReadFrom(buf); err != nil {
		return nil, errors.NewBlockInvalidError("error reading coinbase tx", err)
	}

	// If the coinbaseTx is all zeros (empty), then we should not set it
	if !coinbaseTx.TxIDChainHash().Equal(*emptyTX.TxIDChainHash()) {
		block.CoinbaseTx = &coinbaseTx
	}

	// Read in the block height
	blockHeight64, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading block height", err)
	}

	block.Height, err = util.SafeUint64ToUint32(blockHeight64)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error converting block height to uint32", err)
	}

	return block, nil
}

func (b *Block) Hash() *chainhash.Hash {
	if b.hash != nil {
		return b.hash
	}

	b.hash = b.Header.Hash()

	return b.hash
}

// MinedBlockStore
// TODO This should be compatible with the normal txmetastore.Store, but was implemented now just as a test
type MinedBlockStore interface {
	SetMultiKeysSingleValueAppended(keys []byte, value []byte, keySize int) error
	SetMulti(keys [][]byte, values [][]byte) error
	Get(dst *[]byte, k []byte) error
}

func (b *Block) String() string {
	return fmt.Sprintf("Block %s (height: %d, txCount: %d, size: %d", b.Hash().String(), b.Height, b.TransactionCount, b.SizeInBytes)
}

func (b *Block) Valid(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, txMetaStore utxo.Store, oldBlockIDsMap *sync.Map,
	recentBlocksBloomFilters []*BlockBloomFilter, currentChain []*BlockHeader, currentBlockHeaderIDs []uint32, bloomStats *BloomStats) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "Block:Valid",
		tracing.WithHistogram(prometheusBlockValid),
		tracing.WithLogMessage(logger, "[Block:Valid] called for %s", b.Header.String()),
	)
	defer deferFn()

	// 1. Check that the block header hash is less than the target difficulty.
	headerValid, _, err := b.Header.HasMetTargetDifficulty()
	if err != nil {
		return false, errors.NewProcessingError("[BLOCK][%s] error checking target difficulty", b.Hash().String(), err)
	}

	if !headerValid {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block header hash is not less than the target difficulty", b.Hash().String())
	}

	// 2. Check that the block timestamp is not more than two hours in the future.
	twoHoursToTheFutureTimestampUint32, err := util.SafeInt64ToUint32(time.Now().Add(2 * time.Hour).Unix())
	if err != nil {
		return false, errors.NewBlockInvalidError("failed to convert two hours to the future timestamp to uint32", err)
	}

	if b.Header.Timestamp > twoHoursToTheFutureTimestampUint32 {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block timestamp is more than two hours in the future", b.Hash().String())
	}

	// 3. Check that the median time past of the block is after the median time past of the last 11 blocks.
	// if we don't have 11 blocks then use what we have
	pruneLength := 11
	currentChainLength := len(currentChain)
	// if the current chain length is 0 skip this test
	if currentChainLength > 0 {
		if currentChainLength < pruneLength {
			pruneLength = currentChainLength
		}

		// prune the last few timestamps from the current chain
		lastTimeStamps := currentChain[currentChainLength-pruneLength:]
		prevTimeStamps := make([]time.Time, pruneLength)

		for i, bh := range lastTimeStamps {
			prevTimeStamps[i] = time.Unix(int64(bh.Timestamp), 0)
		}

		// calculate the median timestamp
		medianTimestamp, err := CalculateMedianTimestamp(prevTimeStamps)
		if err != nil {
			return false, err
		}

		b.medianTimestamp, err = util.SafeInt64ToUint32(medianTimestamp.Unix())
		if err != nil {
			return false, err
		}

		// validate that the block's timestamp is after the median timestamp
		if b.Header.Timestamp <= b.medianTimestamp {
			// TODO fix this for test mode when generating lots of blocks quickly
			// return false, errors.NewProcessingError("block timestamp %d is not after median time past of last %d blocks %d", b.Header.Timestamp, pruneLength, medianTimestamp.Unix())
			logger.Warnf("block timestamp %d is not after median time past of last %d blocks %d", b.Header.Timestamp, pruneLength, medianTimestamp.Unix())
		}
	}
	// 4. Check that the coinbase transaction is valid (reward checked later).
	if b.CoinbaseTx == nil {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block has no coinbase tx", b.Hash().String())
	}

	if !b.CoinbaseTx.IsCoinbase() {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block coinbase tx is not a valid coinbase tx", b.Hash().String())
	}

	// We can only calculate the height from coinbase transactions in block versions 2 and higher

	// https://en.bitcoin.it/wiki/BIP_0034
	// BIP-34 was created to force miners to add the block height to the coinbase tx.
	// This BIP came into effect at block 227,835, which is after the first halving
	// at block 210,000.  Therefore, until this happened, we do not know the actual
	// height of the block we are checking for.

	// TODO - do this another way, if necessary

	// 5. Check that the coinbase transaction includes the correct block height.
	if b.Header.Version > 1 && b.Height > LastV1Block {
		height, err := b.ExtractCoinbaseHeight()
		if err != nil {
			return false, errors.NewBlockInvalidError("[BLOCK][%s] error extracting coinbase height", b.Hash().String(), err)
		}

		if height != b.Height {
			return false, errors.NewBlockInvalidError("[BLOCK][%s] block height in coinbase tx (%d) does not match block height in block header (%d)", b.Hash().String(), height, b.Height)
		}
	}

	// only do the subtree checks if we have a subtree store
	// missing the subtreeStore should only happen when we are validating an internal block
	if subtreeStore != nil && len(b.Subtrees) > 0 {
		// 6. Get and validate any missing subtrees.
		if err = b.GetAndValidateSubtrees(ctx, logger, subtreeStore, nil); err != nil {
			return false, err
		}

		// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
		// if !b.SubtreeSlices[0].Nodes[0].Hash.Equal(CoinbasePlaceholder) {
		// 	return false, errors.NewBlockInvalidError("[BLOCK][%s] first transaction in first subtree is not a coinbase placeholder: %s", b.Hash().String(), b.SubtreeSlices[0].Nodes[0].Hash.String())
		// }

		// 8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
		//    making sure to replace the coinbase placeholder with the coinbase tx hash in the first subtree
		if err = b.CheckMerkleRoot(ctx); err != nil {
			return false, err
		}
	}

	// 9. Check that the total fees of the block are less than or equal to the block reward.
	// 10. Check that the coinbase transaction includes the correct block reward.
	if b.Height > 0 {
		err = b.checkBlockRewardAndFees(b.Height)
		if err != nil {
			return false, err
		}
	}

	// 11. Check that there are no duplicate transactions in the block.
	// we only check when we have a subtree store passed in, otherwise this check cannot / should not be done
	if subtreeStore != nil {
		// this creates the txMap for the block that is also used in the validOrderAndBlessed check
		err = b.checkDuplicateTransactions(ctx)
		if err != nil {
			return false, err
		}
	}

	// 12. Check that all transactions are in the valid order and blessed
	//     Can only be done with a valid texMetaStore passed in
	if txMetaStore != nil {
		legacyLimitedBlockValidation := b.settings.Legacy.LimitedBlockValidation
		if b.settings.Legacy.LimitedBlockValidation {
			logger.Warnf("WARNING: legacyLimitedBlockValidation env: %v", legacyLimitedBlockValidation)
		}

		if !legacyLimitedBlockValidation {
			err = b.validOrderAndBlessed(ctx, logger, txMetaStore, subtreeStore, recentBlocksBloomFilters, currentChain, currentBlockHeaderIDs, bloomStats, oldBlockIDsMap)
			if err != nil {
				return false, err
			}
		}

		if err = b.checkConflictingTransactions(ctx, logger, txMetaStore); err != nil {
			return false, err
		}
	}

	// reset the txMap and release the memory
	b.txMap = nil

	return true, nil
}

// https://en.bitcoin.it/wiki/BIP_0034
// BIP-34 was created to force miners to add the block height to the coinbase tx.
// This BIP came into effect at block 227,835, which is after the first halving
// at block 210,000.  Therefore, until this happened, we do not know the actual
// height of the block we are checking for.

// TODO - do this another way, if necessary
func (b *Block) checkBlockRewardAndFees(height uint32) error {
	if height == 0 {
		return nil // Skip this check
	}

	coinbaseOutputSatoshis := uint64(0)
	for _, tx := range b.CoinbaseTx.Outputs {
		coinbaseOutputSatoshis += tx.Satoshis
	}

	subtreeFees := uint64(0)

	for i := 0; i < len(b.SubtreeSlices); i++ {
		subtree := b.SubtreeSlices[i]
		subtreeFees += subtree.Fees
	}

	coinbaseReward := util.GetBlockSubsidyForHeight(height, b.settings.ChainCfgParams)

	if coinbaseOutputSatoshis > subtreeFees+coinbaseReward {
		return errors.NewBlockInvalidError("[BLOCK][%s] coinbase output (%d) is greater than the fees + block subsidy (%d)", b.Hash().String(), coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
	}

	return nil
}

func (b *Block) checkDuplicateTransactions(ctx context.Context) error {
	_, _, deferFn := tracing.StartTracing(ctx, "Block:checkDuplicateTransactions")
	defer deferFn()

	concurrency := b.settings.Block.CheckDuplicateTransactionsConcurrency
	if concurrency <= 0 {
		concurrency = util.Max(4, runtime.NumCPU()/2)
	}

	g := errgroup.Group{}
	g.SetLimit(concurrency)

	transactionCountUint32, err := util.SafeUint64ToUint32(b.TransactionCount)
	if err != nil {
		return errors.NewBlockInvalidError("failed to convert transaction count to int", err)
	}

	b.txMap = util.NewSplitSwissMapUint64(transactionCountUint32)
	for subIdx := 0; subIdx < len(b.SubtreeSlices); subIdx++ {
		subIdx := subIdx
		subtree := b.SubtreeSlices[subIdx]

		g.Go(func() (err error) {
			for txIdx := 0; txIdx < len(subtree.Nodes); txIdx++ {
				if subIdx == 0 && txIdx == 0 {
					continue
				}

				subtreeNode := subtree.Nodes[txIdx]

				// in a tx map, Put is mutually exclusive, can only be called once per key
				err = b.txMap.Put(subtreeNode.Hash, uint64((subIdx*len(subtree.Nodes))+txIdx))
				if err != nil {
					if errors.Is(err, errors.ErrTxExists) {
						return errors.NewBlockInvalidError("[BLOCK][%s] duplicate transaction %s", b.Hash().String(), subtreeNode.Hash.String())
					}

					return errors.NewStorageError("[BLOCK][%s] error adding transaction %s to txMap", b.Hash().String(), subtreeNode.Hash.String(), err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		// just return the error from above
		return err
	}

	return nil
}

func (b *Block) validOrderAndBlessed(ctx context.Context, logger ulogger.Logger, txMetaStore utxo.Store, subtreeStore blob.Store,
	recentBlocksBloomFilters []*BlockBloomFilter, currentChain []*BlockHeader, currentBlockHeaderIDs []uint32, bloomStats *BloomStats, oldBlockIDsMap *sync.Map) error {
	if b.txMap == nil {
		return errors.NewStorageError("[BLOCK][%s] txMap is nil, cannot check transaction order", b.Hash().String())
	}

	currentBlockHeaderHashesMap := make(map[chainhash.Hash]struct{}, len(currentChain))
	for _, blockHeader := range currentChain {
		currentBlockHeaderHashesMap[*blockHeader.Hash()] = struct{}{}
	}

	currentBlockHeaderIDsMap := make(map[uint32]struct{}, len(currentBlockHeaderIDs))
	for _, id := range currentBlockHeaderIDs {
		currentBlockHeaderIDsMap[id] = struct{}{}
	}

	concurrency := b.settings.Block.ValidOrderAndBlessedConcurrency
	if concurrency <= 0 {
		concurrency = util.Max(4, runtime.NumCPU()) // block validation runs on its own box, so we can use all cores
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		sIdx := sIdx

		g.Go(func() (err error) {
			subtreeHash := subtree.RootHash()
			checkParentTxHashes := make([]missingParentTx, 0, len(subtree.Nodes))

			// if the subtree meta slice is loaded, we can use that instead of the txMetaStore
			var subtreeMetaSlice *util.SubtreeMeta

			subtreeMetaSlice, err = b.getSubtreeMetaSlice(ctx, subtreeStore, *subtreeHash, subtree)
			if err != nil {
				subtreeMetaSlice, err = retry.Retry(ctx, logger, func() (*util.SubtreeMeta, error) {
					return b.getSubtreeMetaSlice(gCtx, subtreeStore, *subtreeHash, subtree)
				}, retry.WithMessage(fmt.Sprintf("[BLOCK][%s][%s:%d] error getting subtree meta slice", b.Hash().String(), subtreeHash.String(), sIdx)))

				if err != nil {
					logger.Errorf("[BLOCK][%s][%s:%d] error getting subtree meta slice: %v", b.Hash().String(), subtreeHash.String(), sIdx, err)
				}
			}

			var parentTxHashes []chainhash.Hash

			bloomStats.mu.Lock()
			bloomStats.QueryCounter += uint64(len(subtree.Nodes))
			bloomStats.mu.Unlock()

			for snIdx := 0; snIdx < len(subtree.Nodes); snIdx++ {
				// ignore the very first transaction, is coinbase
				if sIdx == 0 && snIdx == 0 {
					continue
				}

				subtreeNode := subtree.Nodes[snIdx]

				txIdx, ok := b.txMap.Get(subtreeNode.Hash)
				if !ok {
					return errors.NewNotFoundError("[BLOCK][%s][%s:%d]:%d transaction %s is not in the txMap", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())
				}

				if subtreeMetaSlice != nil {
					parentTxHashes = subtreeMetaSlice.ParentTxHashes[snIdx]
				} else {
					// get from the txMetaStore
					var txMeta *meta.Data

					txMeta, err = txMetaStore.GetMeta(gCtx, &subtreeNode.Hash)
					if err != nil {
						txMeta, err = retry.Retry(ctx, logger, func() (*meta.Data, error) {
							return txMetaStore.GetMeta(gCtx, &subtreeNode.Hash)
						}, retry.WithMessage(fmt.Sprintf("[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())))

						if err != nil {
							if errors.Is(err, errors.ErrTxNotFound) {
								return errors.NewNotFoundError("[BLOCK][%s][%s:%d]:%d transaction %s could not be found in tx txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
							}

							return errors.NewStorageError("[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
						}
					}

					if txMeta == nil {
						return errors.NewProcessingError("transaction %s is not blessed", subtreeNode.Hash.String())
					}

					parentTxHashes = txMeta.ParentTxHashes

					if txMeta.LockTime > 0 {
						// A transaction must be final, meaning that, if exists, the lock time is: Equal to zero, or <500000000 and smaller than block height, or >=500000000 and SMALLER THAN TIMESTAMP
						// Any transaction that does not adhere to this consensus rule is to be rejected. See Consensus Rules - TNJ-13
						if err := util.ValidLockTime(txMeta.LockTime, b.Height, b.medianTimestamp); err != nil {
							return errors.NewTxLockTimeError("[BLOCK][%s][%s:%d]:%d transaction %s has an invalid locktime: %d", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), txMeta.LockTime, err)
						}
					}
				}

				if parentTxHashes == nil {
					return errors.NewBlockInvalidError("[BLOCK][%s][%s:%d]:%d transaction %s could not be found in tx meta data", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())
				}

				// TODO add all the parentTxHashes + idxs to a map to check that they are unique (not duplicates)

				// check whether the transaction has recently been mined in a block on our chain
				// for all transactions, we go over all bloom filters, we collect the transactions that are in the bloom filter
				// collected transactions will be checked in the txMetaStore, as they can be false positives.
				// get first 8 bytes of the subtreeNode hash
				n64 := binary.BigEndian.Uint64(subtreeNode.Hash[:])

				for _, filter := range recentBlocksBloomFilters {
					// check whether this bloom filter is on our chain
					if _, found := currentBlockHeaderHashesMap[*filter.BlockHash]; !found {
						continue
					}

					if filter.Filter.Has(n64) {
						// we have a match, check the txMetaStore
						bloomStats.mu.Lock()
						bloomStats.PositiveCounter++
						bloomStats.mu.Unlock()

						// there is a chance that the bloom filter has a false positive, but the txMetaStore has pruned
						// the transaction. This will cause the block to be incorrectly invalidated, but this is the safe
						// option for now.
						txMeta, err := txMetaStore.GetMeta(gCtx, &subtreeNode.Hash)
						if err != nil {
							if errors.Is(err, errors.ErrTxNotFound) {
								continue
							}

							return errors.NewStorageError("[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
						}

						for _, blockID := range txMeta.BlockIDs {
							if _, found := currentBlockHeaderIDsMap[blockID]; found {
								return errors.NewBlockInvalidError("[BLOCK][%s][%s:%d]:%d transaction %s has already been mined in block %d", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), blockID)
							}
						}

						bloomStats.mu.Lock()
						bloomStats.FalsePositiveCounter++
						bloomStats.mu.Unlock()
					}
				}

				for _, parentTxHash := range parentTxHashes {
					parentTxIdx, foundInSameBlock := b.txMap.Get(parentTxHash)
					if foundInSameBlock {
						// parent tx was found in the same block as our tx, check idx
						if parentTxIdx > txIdx {
							return errors.NewBlockInvalidError("[BLOCK][%s][%s:%d]:%d transaction %s comes before parent transaction %s in block", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), parentTxHash.String())
						}

						// if the parent is in the same block, we have already checked whether it is on the same chain
						// in a previous block here above. No need to check again
						continue
					}

					checkParentTxHashes = append(checkParentTxHashes, missingParentTx{parentTxHash, subtreeNode.Hash})
				}
			}

			if len(checkParentTxHashes) > 0 {
				// check all the parent transactions in parallel, this allows us to batch read from the txMetaStore
				parentG := errgroup.Group{}
				parentG.SetLimit(1024 * 32)

				for _, parentTxStruct := range checkParentTxHashes {
					parentTxStruct := parentTxStruct

					parentG.Go(func() error {
						oldParentBlockIDs, err := b.checkParentExistsOnChain(gCtx, logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)

						// there are old blocks we need to return to the validator
						if err == nil && len(oldParentBlockIDs) > 0 {
							// insert tx id and old parent block ids (i.e. tx's parent block ids) into the map.
							// Each tx id and its block ids will be checked by the validator separately.
							oldBlockIDsMap.Store(parentTxStruct.txHash, oldParentBlockIDs)
						}

						return err
					})
				}

				if err = parentG.Wait(); err != nil {
					// just return the error from above
					return err
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewBlockInvalidError("[BLOCK][%s] error validating transaction order", b.Hash().String(), err)
	}

	return nil
}

// checkConflictingTransactions checks whether the transactions in the block have conflicting transactions on the chain
// - 0: get the counter conflicting transactions, check they are not mined on our chain
func (b *Block) checkConflictingTransactions(ctx context.Context, _ ulogger.Logger, txMetaStore utxo.Store) error {
	// get all conflicting transaction from all subtrees
	conflictingTxs, err := b.GetCounterConflictingTxs(ctx, txMetaStore)
	if err != nil {
		return err
	}

	_ = conflictingTxs

	return nil
}

func (b *Block) GetCounterConflictingTxs(ctx context.Context, txMetaStore utxo.Store) ([]chainhash.Hash, error) {
	conflictingTxs := make([]chainhash.Hash, 0, 1024)

	for _, subtree := range b.SubtreeSlices {
		for _, conflictingNode := range subtree.ConflictingNodes {
			tx, err := txMetaStore.Get(ctx, &conflictingNode, fields.Tx)
			if err != nil {
				return nil, errors.NewStorageError("error getting transaction %s from txMetaStore", conflictingNode.String(), err)
			}

			// for _, parentTxHash := range txMeta.ParentTxHashes {
			// 	// get the parent tx from the txMetaStore
			// 	parentTxMeta, err := txMetaStore.GetMeta(ctx, &parentTxHash)
			// 	if err != nil {
			// 		return nil, errors.NewStorageError("error getting parent transaction %s from txMetaStore", parentTxHash.String(), err)
			// 	}
			// }

			_ = tx
		}
	}

	return conflictingTxs, nil
}

func (b *Block) checkParentExistsOnChain(gCtx context.Context, logger ulogger.Logger, txMetaStore utxo.Store, parentTxStruct missingParentTx, currentBlockHeaderIDsMap map[uint32]struct{}) ([]uint32, error) {
	// check whether the parent transaction has already been mined in a block on our chain
	// we need to get back to the txMetaStore for this, to make sure we have the latest data
	// two options: 1- parent is currently under validation, 2- parent is from forked chain.
	// for the first situation we don't start validating the current block until the parent is validated.
	// parent tx meta was not found, must be old, ignore | it is a coinbase, which obviously is mined in a block
	parentTxMeta, err := getParentTxMeta(gCtx, txMetaStore, parentTxStruct)

	var oldBlockIDs []uint32

	if err != nil {
		return oldBlockIDs, err
	}

	if parentTxMeta == nil || parentTxMeta.IsCoinbase {
		return oldBlockIDs, nil
	}

	if len(parentTxMeta.BlockIDs) > 0 && parentTxMeta.BlockIDs[0] == GenesisBlockID {
		// when blockIds[0] is GenesisBlockID, it means the transaction was imported from a restore and is on a valid chain
		return oldBlockIDs, nil
	}

	// check whether the parent is on our current chain (of 100 blocks), it should be, because the tx meta is still in the store
	foundInPreviousBlocks, minBlockID := filterCurrentBlockHeaderIDsMap(parentTxMeta, currentBlockHeaderIDsMap)

	if len(foundInPreviousBlocks) == 0 && minBlockID > 0 {
		var minSetBlockID uint32
		for blockID := range currentBlockHeaderIDsMap {
			if minSetBlockID == 0 || blockID < minSetBlockID {
				minSetBlockID = blockID
			}
		}

		if minBlockID < minSetBlockID {
			// parent is from a block that is older than the blocks we have in the current chain
			logger.Debugf("[BLOCK][%s] parent transaction %s of tx %s is over %d blocks ago - checking later in validator", b.Hash().String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(currentBlockHeaderIDsMap))

			// we need to return parentTxMeta.BlockIDs back to validator, which can check if those blocks are part of our chain
			oldBlockIDs = append(oldBlockIDs, parentTxMeta.BlockIDs...)

			return oldBlockIDs, nil
		}
	}

	if len(foundInPreviousBlocks) != 1 {
		return oldBlockIDs, ErrCheckParentExistsOnChain(gCtx, currentBlockHeaderIDsMap, parentTxMeta, txMetaStore, parentTxStruct, b, foundInPreviousBlocks)
	}

	return oldBlockIDs, nil
}

func ErrCheckParentExistsOnChain(gCtx context.Context, currentBlockHeaderIDsMap map[uint32]struct{}, parentTxMeta *meta.Data, txMetaStore utxo.Store, parentTxStruct missingParentTx, b *Block, foundInPreviousBlocks map[uint32]struct{}) error {
	headerErr := errors.NewBlockError("currentBlockHeaderIDs: %v", currentBlockHeaderIDsMap)
	headerErr = errors.NewBlockError("parent TxMeta: %v", parentTxMeta, headerErr)

	txMeta, err := txMetaStore.GetMeta(gCtx, &parentTxStruct.txHash)
	if err != nil {
		headerErr = errors.NewProcessingError("txMetaStore error getting transaction %s: %v", parentTxStruct.txHash.String(), err, headerErr)
	} else {
		headerErr = errors.NewProcessingError("tx TxMeta: %v", txMeta, headerErr)
	}

	return errors.NewBlockInvalidError("[BLOCK][%s] parent transaction %s of tx %s is not valid on our current chain, found %d times", b.Hash().String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(foundInPreviousBlocks), headerErr)
}

func filterCurrentBlockHeaderIDsMap(parentTxMeta *meta.Data, currentBlockHeaderIDsMap map[uint32]struct{}) (map[uint32]struct{}, uint32) {
	foundInPreviousBlocks := make(map[uint32]struct{}, len(parentTxMeta.BlockIDs))

	var minBlockID uint32

	for _, blockID := range parentTxMeta.BlockIDs {
		if minBlockID == 0 || blockID < minBlockID {
			minBlockID = blockID
		}

		if _, found := currentBlockHeaderIDsMap[blockID]; found {
			foundInPreviousBlocks[blockID] = struct{}{}
		}
	}

	return foundInPreviousBlocks, minBlockID
}

func getParentTxMeta(gCtx context.Context, txMetaStore utxo.Store, parentTxStruct missingParentTx) (*meta.Data, error) {
	parentTxMeta, err := txMetaStore.GetMeta(gCtx, &parentTxStruct.parentTxHash)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return nil, nil
		}

		return nil, errors.NewStorageError("error getting parent transaction %s from txMetaStore", parentTxStruct.parentTxHash.String(), err)
	}

	if parentTxMeta.BlockIDs == nil || len(parentTxMeta.BlockIDs) == 0 {
		return nil, errors.NewBlockInvalidError("parent transaction %s of tx %s has no block IDs", parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String())
	}

	return parentTxMeta, nil
}

// nolint:unused
func (b *Block) getFromAerospike(logger ulogger.Logger, parentTxStruct missingParentTx) error {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Printf("Recovered in getFromAerospike: %v\n", err)
		}
	}()

	aeroURL := b.settings.Block.TxMetaStore
	if aeroURL == nil {
		return errors.NewConfigurationError("aerospike get URL (settings.Block.TxMetaStore) is nil")
	}

	portStr := aeroURL.Port()

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return errors.NewConfigurationError("aerospike port error", err)
	}

	client, aErr := uaerospike.NewClient(aeroURL.Host, port)
	if aErr != nil {
		return errors.NewServiceError("aerospike error", aErr)
	}

	key, aeroErr := aerospike.NewKey(aeroURL.Path[1:], aeroURL.Query().Get("set"), parentTxStruct.txHash.CloneBytes())
	if aeroErr != nil {
		return errors.NewProcessingError("aerospike error", aeroErr)
	}

	readPolicy := aerospike.NewPolicy()
	readPolicy.SocketTimeout = 30 * time.Second
	readPolicy.TotalTimeout = 30 * time.Second
	start := time.Now()
	response, aErr := client.Get(readPolicy, key)

	logger.Warnf("Aerospike get [%s]took %v", parentTxStruct.txHash.String(), time.Since(start))

	if aErr != nil {
		return errors.NewServiceError("aerospike error", aErr)
	}

	return errors.NewServiceError("aerospike response: %v", response)
}

func (b *Block) GetSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, fallbackGetFunc func(subtreeHash chainhash.Hash) error) ([]*util.Subtree, error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockGetSubtrees.Observe(time.Since(startTime).Seconds())
	}()

	// get the subtree slices from the subtree store
	if err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore, fallbackGetFunc); err != nil {
		return nil, err
	}

	return b.SubtreeSlices, nil
}

func (b *Block) GetAndValidateSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, fallbackGetFunc func(subtreeHash chainhash.Hash) error) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "Block:GetAndValidateSubtrees",
		tracing.WithHistogram(prometheusBlockGetAndValidateSubtrees),
	)
	defer deferFn()

	b.subtreeSlicesMu.Lock()
	defer func() {
		b.subtreeSlicesMu.Unlock()
	}()

	if len(b.Subtrees) == len(b.SubtreeSlices) {
		// already loaded
		return nil
	}

	b.SubtreeSlices = make([]*util.Subtree, len(b.Subtrees))

	var (
		sizeInBytes atomic.Uint64
		txCount     atomic.Uint64
	)

	concurrency := b.settings.Block.GetAndValidateSubtreesConcurrency
	if concurrency <= 0 {
		concurrency = util.Max(4, runtime.NumCPU()/2)
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	// we have the hashes. Get the actual subtrees from the subtree store
	for i, subtreeHash := range b.Subtrees {
		i := i
		if b.SubtreeSlices[i] == nil {
			blockHash := b.Hash()
			blockID := b.ID
			subtreeHash := subtreeHash

			g.Go(func() error {
				// retry to get the subtree from the store 3 times, there are instances when we get an EOF error,
				// probably when being moved to permanent storage in another service
				subtree := &util.Subtree{}

				findSubtree := func() (io.ReadCloser, error) {
					readCloser, err := subtreeStore.GetIoReader(gCtx, subtreeHash[:], options.WithFileExtension("subtree"))
					if err != nil {
						if errors.Is(err, errors.ErrNotFound) && fallbackGetFunc != nil {
							if err := fallbackGetFunc(*subtreeHash); err != nil {
								return nil, errors.NewSubtreeNotFoundError("failed to get subtree via fallback method", err)
							}

							return subtreeStore.GetIoReader(gCtx, subtreeHash[:], options.WithFileExtension("subtree"))
						}
					}

					return readCloser, err
				}
				subtreeReader, err := retry.Retry(
					gCtx,
					logger,
					findSubtree,
					retry.WithMessage(fmt.Sprintf("[BLOCK][%s][ID %d] failed to get subtree %s", blockHash, blockID, subtreeHash)),
				)

				if err != nil {
					return errors.NewStorageError("[BLOCK][%s][ID %d] failed to get subtree %s", blockHash, blockID, subtreeHash, err)
				}

				err = subtree.DeserializeFromReader(subtreeReader)
				if err != nil {
					_, err = retry.Retry(gCtx, logger, func() (struct{}, error) {
						return struct{}{}, subtree.DeserializeFromReader(subtreeReader)
					}, retry.WithMessage(fmt.Sprintf("[BLOCK][%s][ID %d] failed to deserialize subtree %s", blockHash, blockID, subtreeHash)))

					if err != nil {
						return errors.NewStorageError("[BLOCK][%s][ID %d] failed to deserialize subtree %s", blockHash, blockID, subtreeHash, err)
					}
				}

				b.SubtreeSlices[i] = subtree

				sizeInBytes.Add(subtree.SizeInBytes)
				txCount.Add(uint64(subtree.Length()))

				_ = subtreeReader.Close()

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		// just return the error from above
		return err
	}

	// check that the size of all subtrees is the same
	var subtreeSize int

	nrOfSubtrees := len(b.Subtrees)

	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		if sIdx == 0 {
			subtreeSize = subtree.Length()
		} else if subtree.Length() != subtreeSize && sIdx != nrOfSubtrees-1 {
			// all subtrees need to be the same size as the first tree, except the last one
			return errors.NewBlockInvalidError("[BLOCK][%s][ID %d] subtree %d has length %d, expected %d", b.Hash().String(), b.ID, sIdx, subtree.Length(), subtreeSize)
		}
	}

	b.TransactionCount = txCount.Load()
	// header + transaction count + size in bytes + coinbase tx size
	b.SizeInBytes = sizeInBytes.Load() + 80 + util.VarintSize(b.TransactionCount) + uint64(b.CoinbaseTx.Size())

	// TODO something with conflicts

	return nil
}

func (b *Block) getSubtreeMetaSlice(ctx context.Context, subtreeStore blob.Store, subtreeHash chainhash.Hash, subtree *util.Subtree) (*util.SubtreeMeta, error) {
	// get subtree meta
	subtreeMetaReader, err := subtreeStore.GetIoReader(ctx, subtreeHash[:], options.WithFileExtension("meta"))
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s][%s] failed to get subtree meta", b.Hash().String(), subtreeHash.String(), err)
	}

	defer func() {
		_ = subtreeMetaReader.Close()
	}()

	// no need to check whether this fails or not, it's just a cache file and not critical
	subtreeMetaSlice, err := util.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s][%s] failed to deserialize subtree meta", b.Hash().String(), subtreeHash.String(), err)
	}

	return subtreeMetaSlice, nil
}

func (b *Block) CheckMerkleRoot(ctx context.Context) (err error) {
	if len(b.Subtrees) != len(b.SubtreeSlices) {
		return errors.NewStorageError("[BLOCK][%s] number of subtrees does not match number of subtree slices, have you called block.GetAndValidateSubtrees()?", b.Hash().String())
	}

	_, _, deferFn := tracing.StartTracing(ctx, "Block:CheckMerkleRoot",
		tracing.WithHistogram(prometheusBlockCheckMerkleRoot),
	)
	defer deferFn()

	hashes := make([]chainhash.Hash, len(b.Subtrees))

	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		if sIdx == 0 {
			// We need to inject the coinbase tx id into the first position of the first subtree
			rootHash, err := subtree.RootHashWithReplaceRootNode(b.CoinbaseTx.TxIDChainHash(), 0, uint64(b.CoinbaseTx.Size()))
			if err != nil {
				return errors.NewProcessingError("[BLOCK][%s] error replacing root node in subtree", b.Hash().String(), err)
			}

			hashes[sIdx] = *rootHash
		} else {
			hashes[sIdx] = *subtree.RootHash()
		}
	}

	var calculatedMerkleRootHash *chainhash.Hash

	switch {
	case len(hashes) == 1:
		calculatedMerkleRootHash = &hashes[0]
	case len(hashes) > 0:
		// Create a new subtree with the hashes of the subtrees
		st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(b.Subtrees)))
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating new root tree", b.Hash().String(), err)
		}

		for _, hash := range hashes {
			err = st.AddNode(hash, 1, 0)
			if err != nil {
				return errors.NewProcessingError("[BLOCK][%s] error adding node to root tree", b.Hash().String(), err)
			}
		}

		calculatedMerkleRoot := st.RootHash()

		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating calculated merkle root hash", b.Hash().String(), err)
		}
	default:
		calculatedMerkleRootHash = b.CoinbaseTx.TxIDChainHash()
	}

	if !b.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		return errors.NewBlockInvalidError("[BLOCK][%s] merkle root does not match", b.Hash().String())
	}

	return nil
}

// ExtractCoinbaseHeight attempts to extract the height of the block from the
// scriptSig of a coinbase transaction.  Coinbase's heights are only present in
// blocks of version 2 or later.  This was added as part of BIP0034.
func (b *Block) ExtractCoinbaseHeight() (uint32, error) {
	if b.CoinbaseTx == nil {
		return 0, errors.NewBlockInvalidError("[BLOCK][%s] missing coinbase transaction", b.Hash().String())
	}

	if len(b.CoinbaseTx.Inputs) != 1 {
		return 0, errors.NewBlockInvalidError("[BLOCK][%s] multiple coinbase transactions", b.Hash().String())
	}

	return util.ExtractCoinbaseHeight(b.CoinbaseTx)
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)

	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree length", b.Hash().String(), err)
	}

	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree hash", b.Hash().String(), err)
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)

	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("[BLOCK][%s] error reading subtree length", b.Hash().String(), err)
	}

	var (
		subtreeBytes [32]byte
		subtreeHash  *chainhash.Hash
	)

	for i := uint64(0); i < subTreeCount; i++ {
		_, err = io.ReadFull(buf, subtreeBytes[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error reading subtree hash", b.Hash().String(), err)
		}

		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating subtree hash", b.Hash().String(), err)
		}

		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	return nil
}

func (b *Block) Bytes() ([]byte, error) {
	if b.Header == nil {
		return nil, errors.NewBlockInvalidError("[BLOCK][%s] block has no header", b.Hash().String())
	}

	// write the header
	buf := bytes.NewBuffer(b.Header.Bytes())

	// write the transaction count
	err := wire.WriteVarInt(buf, 0, b.TransactionCount)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing transaction count", b.Hash().String(), err)
	}

	// write the size in bytes
	err = wire.WriteVarInt(buf, 0, b.SizeInBytes)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing size in bytes", b.Hash().String(), err)
	}

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree list", b.Hash().String(), err)
	}

	if _, err := buf.Write(subtreeBytes); err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree list", b.Hash().String(), err)
	}

	coinbaseTX := b.CoinbaseTx
	if coinbaseTX == nil {
		coinbaseTX = emptyTX
	}

	// write the coinbase tx
	_, err = buf.Write(coinbaseTX.Bytes())
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing coinbase tx", b.Hash().String(), err)
	}

	err = wire.WriteVarInt(buf, 0, uint64(b.Height))
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing height", b.Hash().String(), err)
	}

	return buf.Bytes(), nil
}

func (b *Block) NewOptimizedBloomFilter(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store) (*blobloom.Filter, error) {
	err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore, nil)
	if err != nil {
		// just return the error from the call above
		return nil, err
	}

	filter := blobloom.NewOptimized(blobloom.Config{
		Capacity: b.TransactionCount, // Expected number of keys.
		FPRate:   1e-6,               // Accept one false positive per 100,000 lookups.
	})

	var n64 uint64
	// insert all transaction ids first 8 bytes to the filter
	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		if subtree == nil {
			return nil, errors.NewProcessingError("[BLOCK][%s] missing subtree %d", b.Hash().String(), sIdx)
		}

		for nodeIdx := 0; nodeIdx < len(subtree.Nodes); nodeIdx++ {
			if sIdx == 0 && nodeIdx == 0 {
				// skip coinbase
				continue
			}

			n64 = binary.BigEndian.Uint64(subtree.Nodes[nodeIdx].Hash[:])
			filter.Add(n64)
		}
	}

	return filter, nil
}

func CalculateMedianTimestamp(timestamps []time.Time) (*time.Time, error) {
	n := len(timestamps)

	if n == 0 {
		return nil, errors.NewInvalidArgumentError("no timestamps provided")
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	mid := n / 2
	// NOTE: The consensus rules incorrectly calculate the median for even
	// numbers of blocks.  A true median averages the middle two elements
	// for a set with an even number of elements in it.   Since the constant
	// for the previous number of blocks to be used is odd, this is only an
	// issue for a few blocks near the beginning of the chain.  I suspect
	// this is an optimization even though the result is slightly wrong for
	// a few of the first blocks since after the first few blocks, there
	// will always be an odd number of blocks in the set per the constant.
	//
	// This code follows suit to ensure the same rules are used, however, be
	// aware that should the medianTimeBlocks constant ever be changed to an
	// even number, this code will be wrong.
	return &timestamps[mid], nil
}
