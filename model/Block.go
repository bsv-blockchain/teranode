package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances for subtree deserialization.
// Using 32KB buffers provides excellent I/O performance for sequential reads
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 1024*1024) // Temp changed to 1MB buffer for scaling env - 32KB buffer - optimized for sequential I/O
	},
}

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

type OutPoint struct {
	Hash  chainhash.Hash
	Index uint32
}

type Block struct {
	Header           *BlockHeader          `json:"header"`
	CoinbaseTx       *bt.Tx                `json:"coinbase_tx"`
	TransactionCount uint64                `json:"transaction_count"`
	SizeInBytes      uint64                `json:"size_in_bytes"`
	Subtrees         []*chainhash.Hash     `json:"subtrees"`
	SubtreeSlices    []*subtreepkg.Subtree `json:"-"`
	Height           uint32                `json:"height"` // SAO - This can be left empty (i.e 0) as it is only used in legacy before the height was encoded in the coinbase tx (BIP-34)
	ID               uint32                `json:"id"`

	// local
	hash            atomic.Pointer[chainhash.Hash]
	subtreeLength   uint64
	subtreeSlicesMu sync.RWMutex
	txMap           txmap.TxMap
	medianTimestamp uint32
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash, transactionCount uint64, sizeInBytes uint64, blockHeight uint32, id uint32) (*Block, error) {
	return &Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Subtrees:         subtrees,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		subtreeLength:    uint64(len(subtrees)),
		Height:           blockHeight,
		ID:               id,
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

	versionUint32, err := safeconversion.Int32ToUint32(msgBlock.Header.Version)
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to convert version to uint32", err)
	}

	timestampUint32, err := safeconversion.Int64ToUint32(msgBlock.Header.Timestamp.Unix())
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

	sizeInBytes, err := safeconversion.IntToUint64(msgBlock.SerializeSize())
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to convert msgBlock size to uint64", err)
	}

	subtrees := make([]*chainhash.Hash, 0)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(msgBlock.Transactions))
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
	return NewBlock(header, coinbaseTx, subtrees, txCount, sizeInBytes, 0, 0)
}

func NewBlockFromBytes(blockBytes []byte) (block *Block, err error) {
	startTime := time.Now()

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

	block = &Block{}

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]

	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, errors.NewBlockInvalidError("invalid block header", err)
	}

	return readBlockFromReader(block, bytes.NewReader(blockBytes[80:]))
}

func NewBlockFromReader(blockReader io.Reader) (block *Block, err error) {
	startTime := time.Now()

	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())

		if r := recover(); r != nil {
			err = errors.NewBlockInvalidError("error creating block from reader", r)
			fmt.Println("Recovered in NewBlockFromReader", r)
		}
	}()

	block = &Block{}

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

		if subtreeHash.Equal(chainhash.Hash{}) {
			return nil, errors.NewBlockInvalidError("unexpected empty subtree hash %d of %d", i, block.subtreeLength)
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	if uint64(len(block.Subtrees)) != block.subtreeLength {
		return nil, errors.NewBlockInvalidError("block subtree length mismatch, expected %d, actual %d", block.subtreeLength, block.Subtrees)
	}

	coinbaseTx := new(bt.Tx)
	if _, err = coinbaseTx.ReadFrom(buf); err != nil {
		return nil, errors.NewBlockInvalidError("error reading coinbase tx", err)
	}

	// Always set the coinbase tx (even if empty) to avoid nil pointer issues during validation
	block.CoinbaseTx = coinbaseTx

	// Read in the block height
	blockHeight64, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error reading block height", err)
	}

	block.Height, err = safeconversion.Uint64ToUint32(blockHeight64)
	if err != nil {
		return nil, errors.NewBlockInvalidError("error converting block height to uint32", err)
	}

	return block, nil
}

// GetHash calculates the hash of the block header and caches it.
// It returns the cached hash.
//
// Parameters:
// - None
//
// Returns:
// - *chainhash.Hash: The hash of the block header
func (b *Block) GetHash() *chainhash.Hash {
	calculatedHash := b.Header.Hash()

	b.hash.Store(calculatedHash)

	return calculatedHash
}

// Hash returns the cached hash of the block, or calculates it if not cached.
//
// Parameters:
// - None
//
// Returns:
// - *chainhash.Hash: The hash of the block header
func (b *Block) Hash() *chainhash.Hash {
	cachedHash := b.hash.Load()
	if cachedHash != nil {
		return cachedHash
	}

	calculatedHash := b.Header.Hash()

	b.hash.Store(calculatedHash)

	return calculatedHash
}

// MinedBlockStore
// TODO This should be compatible with the normal txmetastore.Store, but was implemented now just as a test
type MinedBlockStore interface {
	SetMultiKeysSingleValueAppended(keys []byte, value []byte, keySize int) error
	SetMulti(keys [][]byte, values [][]byte) error
	Get(dst *[]byte, k []byte) error
}

func (b *Block) String() string {
	b.subtreeSlicesMu.Lock()
	defer func() {
		b.subtreeSlicesMu.Unlock()
	}()

	return fmt.Sprintf("Block %s (height: %d, id: %d, txCount: %d, size: %d)", b.Hash().String(), b.Height, b.ID, b.TransactionCount, b.SizeInBytes)
}

type SubtreeStore interface {
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
}

func (b *Block) Valid(ctx context.Context, logger ulogger.Logger, subtreeStore SubtreeStore, txMetaStore utxo.Store, oldBlockIDsMap *txmap.SyncedMap[chainhash.Hash, []uint32],
	currentChain []*BlockHeader, currentBlockHeaderIDs []uint32, settings *settings.Settings, metaRegenerator SubtreeMetaRegeneratorI) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("block").Start(ctx, "Valid",
		tracing.WithHistogram(prometheusBlockValid),
		tracing.WithLogMessage(logger, "[Block:Valid] called for %s", b.Header.String()),
	)
	defer deferFn()

	// 1. Check that the block header hash is less than the target difficulty.
	headerValid, _, err := b.Header.HasMetTargetDifficulty()
	if err != nil {
		return false, errors.NewProcessingError("[BLOCK][%s] error checking target difficulty", b.String(), err)
	}

	if !headerValid {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block header hash is not less than the target difficulty", b.String())
	}

	// 2. Check that the block timestamp is not more than two hours in the future.
	twoHoursToTheFutureTimestampUint32, err := safeconversion.Int64ToUint32(time.Now().Add(2 * time.Hour).Unix())
	if err != nil {
		return false, errors.NewProcessingError("[BLOCK][%s] failed to convert two hours to the future timestamp to uint32", b.String(), err)
	}

	if b.Header.Timestamp > twoHoursToTheFutureTimestampUint32 {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block timestamp is more than two hours in the future", b.String())
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

		b.medianTimestamp, err = safeconversion.Int64ToUint32(medianTimestamp.Unix())
		if err != nil {
			return false, err
		}

		// validate that the block's timestamp is after the median timestamp
		if b.Header.Timestamp <= b.medianTimestamp {
			// if we're not on a chain that allows blocks to be generated quickly then return an error
			if !settings.ChainCfgParams.GenerateSupported {
				return false, errors.NewBlockInvalidError("block timestamp %d is not after median time past of last %d blocks %d", b.Header.Timestamp, pruneLength, medianTimestamp.Unix())
			}
			// otherwise just warn
			logger.Warnf("block timestamp %d is not after median time past of last %d blocks %d", b.Header.Timestamp, pruneLength, medianTimestamp.Unix())
		}
	}

	// 4. Check that the coinbase transaction is valid (reward checked later).
	if b.CoinbaseTx == nil {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block has no coinbase tx", b.String())
	}

	if !b.CoinbaseTx.IsCoinbase() {
		return false, errors.NewBlockInvalidError("[BLOCK][%s] block coinbase tx is not a valid coinbase tx", b.String())
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
			return false, errors.NewBlockInvalidError("[BLOCK][%s] error extracting coinbase height", b.String(), err)
		}

		if height != b.Height {
			return false, errors.NewBlockInvalidError("[BLOCK][%s] block height in coinbase tx (%d) does not match block height in block header (%d)", b.String(), height, b.Height)
		}
	}

	// only do the subtree checks if we have a subtree store
	// missing the subtreeStore should only happen when we are validating an internal block
	if subtreeStore != nil && len(b.Subtrees) > 0 {
		// 6. Get and validate any missing subtrees.
		if err = b.GetAndValidateSubtrees(ctx, logger, subtreeStore, settings.Block.GetAndValidateSubtreesConcurrency); err != nil {
			return false, err
		}

		// Verify that we have at least one subtree and that it has at least one node
		if len(b.SubtreeSlices) == 0 || len(b.SubtreeSlices[0].Nodes) == 0 {
			return false, errors.NewBlockInvalidError("[BLOCK][%s] first subtree has no nodes", b.String())
		}

		// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
		if !b.SubtreeSlices[0].Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholder) {
			return false, errors.NewBlockInvalidError("[BLOCK][%s] first transaction in first subtree is not a coinbase placeholder: %s", b.String(), b.SubtreeSlices[0].Nodes[0].Hash.String())
		}

		// 8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
		//    making sure to replace the coinbase placeholder with the coinbase tx hash in the first subtree
		if err = b.CheckMerkleRoot(ctx); err != nil {
			return false, err
		}
	}

	// 9. Check that the total fees of the block are less than or equal to the block reward.
	// 10. Check that the coinbase transaction includes the correct block reward.
	if b.Height > 0 {
		err = b.checkBlockRewardAndFees(settings.ChainCfgParams)
		if err != nil {
			return false, err
		}
	}

	// 11. Check that there are no duplicate transactions in the block.
	// we only check when we have a subtree store passed in, otherwise this check cannot / should not be done
	if subtreeStore != nil {
		// this creates the txMap for the block that is also used in the validOrderAndBlessed check
		err = b.checkDuplicateTransactions(ctx, logger, settings.Block.CheckDuplicateTransactionsConcurrency)
		if err != nil {
			return false, err
		}
	}

	// 12. Check that all transactions are in the valid order and blessed
	//     Can only be done with a valid texMetaStore passed in
	if txMetaStore != nil {
		deps := &validationDependencies{
			txMetaStore:           txMetaStore,
			subtreeStore:          subtreeStore,
			currentChain:          currentChain,
			currentBlockHeaderIDs: currentBlockHeaderIDs,
			oldBlockIDsMap:        oldBlockIDsMap,
			metaRegenerator:       metaRegenerator,
		}
		err = b.validOrderAndBlessed(ctx, logger, deps, settings.Block.ValidOrderAndBlessedConcurrency)
		if err != nil {
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
func (b *Block) checkBlockRewardAndFees(params *chaincfg.Params) error {
	if b.Height == 0 {
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

	coinbaseReward := util.GetBlockSubsidyForHeight(b.Height, params)

	if coinbaseOutputSatoshis > subtreeFees+coinbaseReward {
		return errors.NewBlockInvalidError("[checkBlockRewardAndFees][%s] coinbase output (%d) is greater than the fees + block subsidy (%d)", b.String(), coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
	}

	return nil
}

// checkDuplicateTransactions checks for duplicate transactions in all the subtrees in the block.
// It uses a concurrent approach to check for duplicates in each subtree.
// If a duplicate transaction is found, it returns an error.
//
// Parameters:
// - ctx: the context to use for tracing and cancellation
//
// Returns:
// - error: if a duplicate transaction is found or if there is an error adding the transaction to the txMap
func (b *Block) checkDuplicateTransactions(ctx context.Context, logger ulogger.Logger, checkDuplicateTransactionsConcurrency int) error {
	_, _, deferFn := tracing.Tracer("block").Start(ctx, "checkDuplicateTransactions",
		tracing.WithLogMessage(logger, "[checkDuplicateTransactions][%s] called", b.String()),
	)
	defer deferFn()

	concurrency := checkDuplicateTransactionsConcurrency
	if concurrency <= 0 {
		concurrency = subtreepkg.Max(4, runtime.NumCPU()/2)
	}

	g := new(errgroup.Group)
	util.SafeSetLimit(g, concurrency)

	transactionCountUint32, err := safeconversion.Uint64ToUint32(b.TransactionCount)
	if err != nil {
		return errors.NewProcessingError("[checkDuplicateTransactions][%s] failed to convert transaction count to int", b.String(), err)
	}

	// set the expected subtree size based on the first subtree in the block
	subtreeSize := 0
	if len(b.SubtreeSlices) > 0 {
		subtreeSize = b.SubtreeSlices[0].Size()
	}

	b.txMap = txmap.NewSplitSwissMapUint64(transactionCountUint32)
	for subIdx := 0; subIdx < len(b.SubtreeSlices); subIdx++ {
		subIdx := subIdx
		subtree := b.SubtreeSlices[subIdx]

		g.Go(func() (err error) {
			return b.checkDuplicateTransactionsInSubtree(subtree, subIdx, subtreeSize)
		})
	}

	if err = g.Wait(); err != nil {
		// return the error from above without wrapping it
		return err
	}

	return nil
}

// checkDuplicateTransactionsInSubtree checks for duplicate transactions in a subtree.
// It uses the block txMap to store the transactions and check for duplicates.
// If a duplicate transaction is found, it returns an error.
//
// Parameters:
// - subtree: the subtree to check for duplicate transactions
// - subIdx: the index of the subtree in the block
// - subtreeSize: the cap of the subtree (number of transactions in the subtree) - this is based on the first subtree in the block
//
// Returns:
// - error: if a duplicate transaction is found or if there is an error adding the transaction to the txMap
func (b *Block) checkDuplicateTransactionsInSubtree(subtree *subtreepkg.Subtree, subIdx, subtreeSize int) (err error) {
	var idx64 uint64

	// Calculate the base index for this subtree by summing sizes of all previous subtrees.
	// We cannot use (subIdx * subtreeSize) because the last subtree may be smaller.
	// Per Block.go:1192: "all subtrees need to be the same size as the first tree, except the last one"
	baseIdx := 0
	for i := 0; i < subIdx; i++ {
		baseIdx += b.SubtreeSlices[i].Size()
	}

	for txIdx := 0; txIdx < len(subtree.Nodes); txIdx++ {
		if subIdx == 0 && txIdx == 0 && subtree.Nodes[txIdx].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		subtreeNode := subtree.Nodes[txIdx]

		// Calculate the global transaction index as baseIdx + txIdx
		idx64, err = safeconversion.IntToUint64(baseIdx + txIdx)
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] failed to convert index to uint64", b.String(), err)
		}

		// in a tx map, Put is mutually exclusive, can only be called once per key
		if err = b.txMap.Put(subtreeNode.Hash, idx64); err != nil {
			if errors.Is(err, errors.ErrTxExists) || strings.Contains(err.Error(), "hash already exists in map") {
				return errors.NewBlockInvalidError("[BLOCK][%s] block contains duplicate transaction %s", b.String(), subtreeNode.Hash.String())
			}

			return errors.NewStorageError("[BLOCK][%s] error adding transaction %s to txMap", b.String(), subtreeNode.Hash.String(), err)
		}
	}

	return nil
}

type validationDependencies struct {
	txMetaStore           utxo.Store
	subtreeStore          SubtreeStore
	currentChain          []*BlockHeader
	currentBlockHeaderIDs []uint32
	oldBlockIDsMap        *txmap.SyncedMap[chainhash.Hash, []uint32]
	metaRegenerator       SubtreeMetaRegeneratorI // optional: nil means no regeneration
}

func (b *Block) validOrderAndBlessed(ctx context.Context, logger ulogger.Logger, deps *validationDependencies, validOrderAndBlessedConcurrency int) error {
	ctx, _, deferFn := tracing.Tracer("block").Start(ctx, "validOrderAndBlessed",
		tracing.WithLogMessage(logger, "[validOrderAndBlessed][%s] called", b.String()),
	)
	defer deferFn()

	if b.txMap == nil {
		return errors.NewStorageError("[validOrderAndBlessed][%s] txMap is nil, cannot check transaction order", b.String())
	}

	validationCtx := &validationContext{
		currentBlockHeaderHashesMap: b.buildBlockHeaderHashesMap(deps.currentChain),
		currentBlockHeaderIDsMap:    b.buildBlockHeaderIDsMap(deps.currentBlockHeaderIDs),
		parentSpendsMap:             NewSplitSyncedParentMap(4096),
	}

	concurrency := b.getValidationConcurrency(validOrderAndBlessedConcurrency)
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		sIdx := sIdx

		g.Go(func() error {
			return b.validateSubtree(gCtx, logger, deps, validationCtx, subtree, sIdx)
		})
	}

	// do not wrap the error again, the error is already wrapped
	return g.Wait()
}

func (b *Block) validateSubtree(ctx context.Context, logger ulogger.Logger, deps *validationDependencies,
	validationCtx *validationContext, subtree *subtreepkg.Subtree, sIdx int) error {
	ctx, _, deferFn := tracing.Tracer("block").Start(ctx, "validateSubtree",
		tracing.WithLogMessage(logger, "[validateSubtree][%s][%s:%d] called", b.String(), subtree.RootHash().String(), sIdx),
	)
	defer deferFn()

	var (
		subtreeMetaSlice    *subtreepkg.Meta
		subtreeHash         = subtree.RootHash()
		checkParentTxHashes = make([]missingParentTx, 0, len(subtree.Nodes))
		err                 error
	)

	subtreeMetaSlice, err = b.getSubtreeMetaSlice(ctx, deps.subtreeStore, *subtreeHash, subtree)

	// Attempt regeneration if meta not found and regenerator is available
	if err != nil && deps.metaRegenerator != nil {
		logger.Warnf("[validateSubtree][%s][%s:%d] subtree meta not found, attempting regeneration", b.String(), subtreeHash.String(), sIdx)

		subtreeMetaSlice, err = deps.metaRegenerator.RegenerateMeta(ctx, subtreeHash, subtree)
		if err == nil {
			logger.Warnf("[validateSubtree][%s][%s:%d] successfully regenerated subtree meta", b.String(), subtreeHash.String(), sIdx)
		}
	}

	// a subtreeMetaSlice is required for further block validation, so if we cannot get it, we return an error
	if err != nil {
		return errors.NewProcessingError("[validOrderAndBlessed][%s][%s:%d] error getting subtree meta slice: %v", b.String(), subtreeHash.String(), sIdx, err)
	}

	for snIdx := 0; snIdx < len(subtree.Nodes); snIdx++ {
		// ignore the very first transaction, is coinbase
		if sIdx == 0 && snIdx == 0 && subtree.Nodes[snIdx].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		subtreeNode := subtree.Nodes[snIdx]

		missingParents, err := b.validateTransaction(ctx, deps, validationCtx, &transactionValidationParams{
			subtreeMetaSlice: subtreeMetaSlice,
			subtreeHash:      subtreeHash,
			sIdx:             sIdx,
			snIdx:            snIdx,
			subtreeNode:      subtreeNode,
		})
		if err != nil {
			return err
		}

		checkParentTxHashes = append(checkParentTxHashes, missingParents...)
	}

	if len(checkParentTxHashes) > 0 {
		return b.checkParentsExistOnChain(ctx, logger, deps, validationCtx, checkParentTxHashes)
	}

	return nil
}

func (b *Block) checkParentsExistOnChain(ctx context.Context, logger ulogger.Logger, deps *validationDependencies,
	validationCtx *validationContext, checkParentTxHashes []missingParentTx) error {
	ctx, _, deferFn := tracing.Tracer("block").Start(ctx, "checkParentsExistOnChain",
		tracing.WithLogMessage(logger, "[validateSubtree][%s] called to check %d parent tx hashes", b.String(), len(checkParentTxHashes)),
	)
	defer deferFn()

	// check all the parent transactions in parallel, this allows us to batch read from the txMetaStore
	parentG := new(errgroup.Group)
	util.SafeSetLimit(parentG, 1024*32)

	for _, parentTxStruct := range checkParentTxHashes {
		parentTxStruct := parentTxStruct

		parentG.Go(func() error {
			oldParentBlockIDs, err := b.checkParentExistsOnChain(ctx, logger, deps.txMetaStore, parentTxStruct, validationCtx.currentBlockHeaderIDsMap)

			// there are old blocks we need to return to the validator
			if err == nil && len(oldParentBlockIDs) > 0 {
				// insert tx id and old parent block ids (i.e. tx's parent block ids) into the map.
				// Each tx id and its block ids will be checked by the validator separately.
				deps.oldBlockIDsMap.Set(parentTxStruct.txHash, oldParentBlockIDs)
			}

			return err
		})
	}

	return parentG.Wait()
}

type validationContext struct {
	currentBlockHeaderHashesMap map[chainhash.Hash]struct{}
	currentBlockHeaderIDsMap    map[uint32]struct{}
	parentSpendsMap             *SplitSyncedParentMap
}

func (b *Block) buildBlockHeaderHashesMap(currentChain []*BlockHeader) map[chainhash.Hash]struct{} {
	currentBlockHeaderHashesMap := make(map[chainhash.Hash]struct{}, len(currentChain))
	for _, blockHeader := range currentChain {
		currentBlockHeaderHashesMap[*blockHeader.Hash()] = struct{}{}
	}

	return currentBlockHeaderHashesMap
}

func (b *Block) buildBlockHeaderIDsMap(currentBlockHeaderIDs []uint32) map[uint32]struct{} {
	currentBlockHeaderIDsMap := make(map[uint32]struct{}, len(currentBlockHeaderIDs))
	for _, id := range currentBlockHeaderIDs {
		currentBlockHeaderIDsMap[id] = struct{}{}
	}

	return currentBlockHeaderIDsMap
}

func (b *Block) getValidationConcurrency(validOrderAndBlessedConcurrency int) int {
	concurrency := validOrderAndBlessedConcurrency
	if concurrency <= 0 {
		concurrency = subtreepkg.Max(4, runtime.NumCPU()) // block validation runs on its own box, so we can use all cores
	}

	return concurrency
}

func (b *Block) checkParentExistsOnChain(gCtx context.Context, logger ulogger.Logger, txMetaStore utxo.Store, parentTxStruct missingParentTx, currentBlockHeaderIDsMap map[uint32]struct{}) ([]uint32, error) {
	var oldBlockIDs []uint32

	// check whether the parent transaction has already been mined in a block on our chain
	// we need to get back to the txMetaStore for this, to make sure we have the latest data
	// two options: 1- parent is currently under validation, 2- parent is from forked chain.
	// for the first situation we don't start validating the current block until the parent is validated.
	// parent tx meta was not found, must be old, ignore | it is a coinbase, which obviously is mined in a block
	parentTxMeta, err := getParentTxMetaBlockIDs(gCtx, txMetaStore, parentTxStruct)
	if err != nil {
		return oldBlockIDs, err
	}

	if parentTxMeta == nil {
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
			logger.Debugf("[BLOCK][%s] parent transaction %s of tx %s is over %d blocks ago - checking later in validator", b.String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(currentBlockHeaderIDsMap))

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

	txMeta := &meta.Data{}
	if err := txMetaStore.GetMeta(gCtx, &parentTxStruct.txHash, txMeta); err != nil {
		headerErr = errors.NewProcessingError("txMetaStore error getting transaction %s: %v", parentTxStruct.txHash.String(), err, headerErr)
	} else {
		headerErr = errors.NewProcessingError("tx TxMeta: %v", txMeta, headerErr)
	}

	return errors.NewBlockInvalidError("[BLOCK][%s] parent transaction %s of tx %s is not valid on our current chain, found %d times", b.String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(foundInPreviousBlocks), headerErr)
}

type transactionValidationParams struct {
	subtreeMetaSlice *subtreepkg.Meta
	subtreeHash      *chainhash.Hash
	sIdx, snIdx      int
	subtreeNode      subtreepkg.Node
}

func (b *Block) validateTransaction(ctx context.Context, deps *validationDependencies, validationCtx *validationContext,
	params *transactionValidationParams) ([]missingParentTx, error) {
	txIdx, ok := b.txMap.Get(params.subtreeNode.Hash)
	if !ok {
		return nil, errors.NewNotFoundError("[validOrderAndBlessed][%s][%s:%d]:%d transaction %s is not in the txMap",
			b.String(), params.subtreeHash.String(), params.sIdx, params.snIdx, params.subtreeNode.Hash.String())
	}

	parentTxHashes, err := params.subtreeMetaSlice.GetParentTxHashes(params.snIdx)
	if err != nil {
		return nil, errors.NewStorageError("[validOrderAndBlessed][%s][%s:%d]:%d error getting parent tx hashes from subtree meta slice",
			b.String(), params.subtreeHash.String(), params.sIdx, params.snIdx, err)
	}

	if parentTxHashes == nil {
		return nil, errors.NewBlockInvalidError("[validOrderAndBlessed][%s][%s:%d]:%d transaction %s could not be found in tx meta data",
			b.String(), params.subtreeHash.String(), params.sIdx, params.snIdx, params.subtreeNode.Hash.String())
	}

	// Check for duplicate inputs
	err = b.checkDuplicateInputs(params.subtreeMetaSlice, validationCtx, params.subtreeHash, params.sIdx, params.snIdx, params.subtreeNode)
	if err != nil {
		return nil, err
	}

	// Check if transaction has been mined in recent blocks
	//
	// We do not have to do this anymore, since we are checking for this in setTXMined, which gets the blockIDs for all transactions
	// returned and checks them against the recent block IDs.
	// model/update-tx-mined.go:278
	//
	// err = b.checkTxInRecentBlocks(ctx, deps, validationCtx, params.subtreeNode, params.subtreeHash, params.sIdx, params.snIdx)
	// if err != nil {
	// 	return nil, err
	// }

	// Check parent transactions
	return b.checkParentTransactions(parentTxHashes, txIdx, params.subtreeNode, params.subtreeHash, params.sIdx, params.snIdx)
}

func (b *Block) checkDuplicateInputs(subtreeMetaSlice *subtreepkg.Meta, validationCtx *validationContext,
	subtreeHash *chainhash.Hash, sIdx, snIdx int, subtreeNode subtreepkg.Node) error {
	txInpoints, err := subtreeMetaSlice.GetTxInpoints(snIdx)
	if err != nil {
		return errors.NewStorageError("[validOrderAndBlessed][%s][%s:%d]:%d error getting tx inpoints from subtree meta slice",
			b.String(), subtreeHash.String(), sIdx, snIdx, err)
	}

	for _, txInpoint := range txInpoints {
		if valueSet := validationCtx.parentSpendsMap.SetIfNotExists(txInpoint); !valueSet {
			return errors.NewBlockInvalidError("[validOrderAndBlessed][%s][%s:%d]:%d transaction %s has duplicate inputs",
				b.String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())
		}
	}

	return nil
}

func (b *Block) checkParentTransactions(parentTxHashes []chainhash.Hash, txIdx uint64,
	subtreeNode subtreepkg.Node, subtreeHash *chainhash.Hash, sIdx, snIdx int) ([]missingParentTx, error) {
	checkParentTxHashes := make([]missingParentTx, 0, len(parentTxHashes)/16) // assume 1 in 16 parents will be missing

	for _, parentTxHash := range parentTxHashes {
		parentTxIdx, foundInSameBlock := b.txMap.Get(parentTxHash)
		if foundInSameBlock {
			// parent tx was found in the same block as our tx, check idx
			if parentTxIdx > txIdx {
				return nil, errors.NewBlockInvalidError("[validOrderAndBlessed][%s][%s:%d]:%d transaction %s comes before parent transaction %s in block",
					b.String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), parentTxHash.String())
			}
			// if the parent is in the same block, we have already checked whether it is on the same chain
			// in a previous block here above. No need to check again
			continue
		}

		checkParentTxHashes = append(checkParentTxHashes, missingParentTx{parentTxHash, subtreeNode.Hash})
	}

	return checkParentTxHashes, nil
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

func getParentTxMetaBlockIDs(gCtx context.Context, txMetaStore utxo.Store, parentTxStruct missingParentTx) (*meta.Data, error) {
	parentTxMeta, err := txMetaStore.Get(gCtx, &parentTxStruct.parentTxHash, fields.BlockIDs)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return nil, errors.NewBlockInvalidError("parent transaction %s of tx %s not found in txMetaStore", parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String())
		}

		return nil, errors.NewStorageError("error getting parent transaction %s from txMetaStore", parentTxStruct.parentTxHash.String(), err)
	}

	if len(parentTxMeta.BlockIDs) == 0 {
		return nil, errors.NewBlockInvalidError("parent transaction %s of tx %s has no block IDs", parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String())
	}

	return parentTxMeta, nil
}

func (b *Block) GetSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore SubtreeStore, getAndValidateSubtreesConcurrency int) ([]*subtreepkg.Subtree, error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockGetSubtrees.Observe(time.Since(startTime).Seconds())
	}()

	// get the subtree slices from the subtree store
	if err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore, getAndValidateSubtreesConcurrency); err != nil {
		return nil, err
	}

	return b.SubtreeSlices, nil
}

func (b *Block) GetAndValidateSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore SubtreeStore, getAndValidateSubtreesConcurrency int) error {
	ctx, _, deferFn := tracing.Tracer("block").Start(ctx, "GetAndValidateSubtrees",
		tracing.WithHistogram(prometheusBlockGetAndValidateSubtrees),
		tracing.WithLogMessage(logger, "[GetAndValidateSubtrees][%s] fetching and validating subtrees", b.String()),
	)
	defer deferFn()

	b.subtreeSlicesMu.Lock()
	defer func() {
		b.subtreeSlicesMu.Unlock()
	}()

	if len(b.Subtrees) == len(b.SubtreeSlices) {
		missing := false

		for i := range b.Subtrees {
			if b.SubtreeSlices[i] == nil {
				missing = true
				break
			}
		}

		if !missing {
			// already loaded
			return nil
		}
	}

	b.SubtreeSlices = make([]*subtreepkg.Subtree, len(b.Subtrees))

	var (
		sizeInBytes atomic.Uint64
		txCount     atomic.Uint64
	)

	concurrency := getAndValidateSubtreesConcurrency
	if concurrency <= 0 {
		concurrency = subtreepkg.Max(4, runtime.NumCPU()/2)
	}

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)
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
				subtree := &subtreepkg.Subtree{}

				findSubtree := func() (io.ReadCloser, error) {
					readCloser, err := subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtree)
					if err != nil {
						if errors.Is(err, errors.ErrNotFound) {
							// Try fallback to SubtreeToCheck - this file exists when CheckBlockSubtrees downloaded
							// the subtree but couldn't fully validate it (e.g., due to missing parent transactions)
							readCloser, err = subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
							if err == nil {
								logger.Debugf("[BLOCK][%s][ID %d] using FileTypeSubtreeToCheck for subtree %s", blockHash, blockID, subtreeHash)
								return readCloser, nil
							}
							// Still not found, return the original NOT_FOUND error
							return nil, errors.ErrNotFound
						}

						return nil, err
					}

					return readCloser, err
				}

				subtreeReader, err := retry.Retry(
					gCtx,
					logger,
					findSubtree,
					retry.WithMessage(fmt.Sprintf("[BLOCK][%s][ID %d] failed to get subtree %s", blockHash, blockID, subtreeHash)),
					retry.WithRetryCount(3),
					retry.WithBackoffDurationType(100*time.Millisecond),
				)
				if err != nil {
					return errors.NewStorageError("[BLOCK][%s][ID %d] failed to get subtree %s", blockHash, blockID, subtreeHash, err)
				}

				defer func() {
					_ = subtreeReader.Close()
				}()

				// Use pooled bufio.Reader to reduce GC pressure
				bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
				bufferedReader.Reset(subtreeReader)
				defer func() {
					bufferedReader.Reset(nil)
					bufioReaderPool.Put(bufferedReader)
				}()

				if err = subtree.DeserializeFromReader(bufferedReader); err != nil {
					_, err = retry.Retry(gCtx, logger, func() (struct{}, error) {
						return struct{}{}, subtree.DeserializeFromReader(bufferedReader)
					}, retry.WithMessage(fmt.Sprintf("[BLOCK][%s][ID %d] failed to deserialize subtree %s", blockHash, blockID, subtreeHash)))

					if err != nil {
						return errors.NewStorageError("[BLOCK][%s][ID %d] failed to deserialize subtree %s", blockHash, blockID, subtreeHash, err)
					}
				}

				b.SubtreeSlices[i] = subtree

				sizeInBytes.Add(subtree.SizeInBytes)
				txCount.Add(uint64(subtree.Length())) // nolint: gosec

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
		if subtree == nil {
			return errors.NewBlockInvalidError("[BLOCK][%s][ID %d] subtree %d of %d was loaded but is nil", b.String(), b.ID, sIdx, nrOfSubtrees)
		}
		if sIdx == 0 {
			subtreeSize = subtree.Length()
		} else if subtree.Length() != subtreeSize && sIdx != nrOfSubtrees-1 {
			// all subtrees need to be the same size as the first tree, except the last one
			return errors.NewBlockInvalidError("[BLOCK][%s][ID %d] subtree %d has length %d, expected %d", b.String(), b.ID, sIdx, subtree.Length(), subtreeSize)
		}
	}

	b.TransactionCount = txCount.Load()
	// header + transaction count + size in bytes + coinbase tx size
	b.SizeInBytes = sizeInBytes.Load() + 80 + util.VarintSize(b.TransactionCount) + uint64(b.CoinbaseTx.Size()) // nolint: gosec

	// TODO something with conflicts

	return nil
}

// SubtreesLoaded checks if subtrees are loaded and valid.
// This method is safe for concurrent use.
func (b *Block) SubtreesLoaded() bool {
	b.subtreeSlicesMu.RLock()
	defer b.subtreeSlicesMu.RUnlock()

	if len(b.SubtreeSlices) != len(b.Subtrees) || len(b.SubtreeSlices) == 0 {
		return false
	}

	for _, subtree := range b.SubtreeSlices {
		if subtree == nil {
			return false
		}
	}

	return true
}

// GetSubtreeSlicesCount returns the number of loaded subtree slices.
// This method is safe for concurrent use.
func (b *Block) GetSubtreeSlicesCount() int {
	b.subtreeSlicesMu.RLock()
	defer b.subtreeSlicesMu.RUnlock()
	return len(b.SubtreeSlices)
}

func (b *Block) getSubtreeMetaSlice(ctx context.Context, subtreeStore SubtreeStore, subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Meta, error) {
	// get subtree meta
	subtreeMetaReader, err := subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s][%s] failed to get subtree meta", b.String(), subtreeHash.String(), err)
	}

	defer func() {
		_ = subtreeMetaReader.Close()
	}()

	// no need to check whether this fails or not, it's just a cache file and not critical
	subtreeMetaSlice, err := subtreepkg.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s][%s] failed to deserialize subtree meta", b.String(), subtreeHash.String(), err)
	}

	return subtreeMetaSlice, nil
}

func (b *Block) CheckMerkleRoot(ctx context.Context) (err error) {
	if len(b.Subtrees) != len(b.SubtreeSlices) {
		return errors.NewStorageError("[BLOCK][%s] number of subtrees does not match number of subtree slices, have you called block.GetAndValidateSubtrees()?", b.String())
	}

	_, _, deferFn := tracing.Tracer("block").Start(ctx, "CheckMerkleRoot",
		tracing.WithHistogram(prometheusBlockCheckMerkleRoot),
	)
	defer deferFn()

	hashes := make([]chainhash.Hash, len(b.Subtrees))

	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		if subtree == nil {
			return errors.NewProcessingError("[BLOCK][%s] missing subtree %d of %d", b.String(), sIdx, len(b.Subtrees))
		}

		if sIdx == 0 {
			// We need to inject the coinbase tx id into the first position of the first subtree
			rootHash, err := subtree.RootHashWithReplaceRootNode(b.CoinbaseTx.TxIDChainHash(), 0, uint64(b.CoinbaseTx.Size())) // nolint: gosec
			if err != nil {
				return errors.NewProcessingError("[BLOCK][%s] error replacing root node in subtree", b.String(), err)
			}

			hashes[sIdx] = *rootHash
		} else {
			rootHash := subtree.RootHash()
			if rootHash == nil {
				return errors.NewProcessingError("[BLOCK][%s] subtree %d returned nil root hash", b.String(), sIdx)
			}

			hashes[sIdx] = *rootHash
		}
	}

	var calculatedMerkleRootHash *chainhash.Hash

	switch {
	case len(hashes) == 1:
		calculatedMerkleRootHash = &hashes[0]
	case len(hashes) > 0:
		// Create a new subtree with the hashes of the subtrees
		st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(b.Subtrees))
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating new root tree", b.String(), err)
		}

		for _, hash := range hashes {
			err = st.AddNode(hash, 1, 0)
			if err != nil {
				return errors.NewProcessingError("[BLOCK][%s] error adding node to root tree", b.String(), err)
			}
		}

		calculatedMerkleRoot := st.RootHash()

		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating calculated merkle root hash", b.String(), err)
		}
	default:
		calculatedMerkleRootHash = b.CoinbaseTx.TxIDChainHash()
	}

	if !b.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		return errors.NewBlockInvalidError("[BLOCK][%s] merkle root does not match", b.String())
	}

	return nil
}

// ExtractCoinbaseHeight attempts to extract the height of the block from the
// scriptSig of a coinbase transaction.  Coinbase's heights are only present in
// blocks of version 2 or later.  This was added as part of BIP0034.
func (b *Block) ExtractCoinbaseHeight() (uint32, error) {
	if b.CoinbaseTx == nil {
		return 0, errors.NewBlockInvalidError("[BLOCK][%s] missing coinbase transaction", b.String())
	}

	if len(b.CoinbaseTx.Inputs) != 1 {
		return 0, errors.NewBlockInvalidError("[BLOCK][%s] multiple coinbase transactions", b.String())
	}

	return util.ExtractCoinbaseHeight(b.CoinbaseTx)
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)

	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree length", b.String(), err)
	}

	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree hash", b.String(), err)
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)

	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.NewProcessingError("[BLOCK][%s] error reading subtree length", b.String(), err)
	}

	var (
		subtreeBytes [32]byte
		subtreeHash  *chainhash.Hash
	)

	for i := uint64(0); i < subTreeCount; i++ {
		_, err = io.ReadFull(buf, subtreeBytes[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error reading subtree hash", b.String(), err)
		}

		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return errors.NewProcessingError("[BLOCK][%s] error creating subtree hash", b.String(), err)
		}

		if subtreeHash.Equal(chainhash.Hash{}) {
			return errors.NewProcessingError("[BLOCK][%s] unexpected empty subtree hash %d of %d", b.String(), i, subTreeCount)
		}

		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	if b.subtreeLength != uint64(len(b.Subtrees)) {
		return errors.NewProcessingError("[BLOCK][%s] subtree size mismatch, expected %d, actual %d", b.String(), b.subtreeLength, len(b.Subtrees), err)
	}

	return nil
}

func (b *Block) Bytes() ([]byte, error) {
	if b.Header == nil {
		return nil, errors.NewBlockInvalidError("[BLOCK][%s] block has no header", b.String())
	}

	// write the header
	buf := bytes.NewBuffer(b.Header.Bytes())

	// write the transaction count
	err := wire.WriteVarInt(buf, 0, b.TransactionCount)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing transaction count", b.String(), err)
	}

	// write the size in bytes
	err = wire.WriteVarInt(buf, 0, b.SizeInBytes)
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing size in bytes", b.String(), err)
	}

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree list", b.String(), err)
	}

	if _, err := buf.Write(subtreeBytes); err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing subtree list", b.String(), err)
	}

	coinbaseTX := b.CoinbaseTx
	if coinbaseTX == nil {
		coinbaseTX = emptyTX
	}

	// write the coinbase tx
	_, err = buf.Write(coinbaseTX.Bytes())
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing coinbase tx", b.String(), err)
	}

	err = wire.WriteVarInt(buf, 0, uint64(b.Height))
	if err != nil {
		return nil, errors.NewProcessingError("[BLOCK][%s] error writing height", b.String(), err)
	}

	return buf.Bytes(), nil
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
