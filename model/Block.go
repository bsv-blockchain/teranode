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

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/greatroar/blobloom"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
)

var (
	prometheusBlockFromBytes              prometheus.Histogram
	prometheusBlockValid                  prometheus.Histogram
	prometheusBlockCheckMerkleRoot        prometheus.Histogram
	prometheusBlockGetSubtrees            prometheus.Histogram
	prometheusBlockGetAndValidateSubtrees prometheus.Histogram
	prometheusBloomQueryCounter           prometheus.Gauge
	prometheusBloomPositiveCounter        prometheus.Gauge
	prometheusBloomFalsePositiveCounter   prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

func init() {
	initPrometheusMetrics()
}

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockFromBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "block",
			Name:      "from_bytes",
			Help:      "Duration of BlockFromBytes",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockValid = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "block",
			Name:      "valid",
			Help:      "Duration of Block.Valid",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockCheckMerkleRoot = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "block",
			Name:      "check_merkle_root",
			Help:      "Duration of Block.CheckMerkleRoot",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "block",
			Name:      "get_subtrees",
			Help:      "Duration of Block.GetSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetAndValidateSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "block",
			Name:      "get_and_validate_subtrees",
			Help:      "Duration of Block.GetAndValidateSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBloomQueryCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "block",
			Name:      "bloom_filter_query_counter",
			Help:      "Number of queries to the bloom filter",
		},
	)

	prometheusBloomPositiveCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "block",
			Name:      "bloom_filter_positive_counter",
			Help:      "Number of positive from the bloom filter",
		},
	)

	prometheusBloomFalsePositiveCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "block",
			Name:      "bloom_filter_false_positive_counter",
			Help:      "Number of false positives from the bloom filter",
		},
	)

}

type missingParentTx struct {
	parentTxHash chainhash.Hash
	txHash       chainhash.Hash
}

type BloomStats struct {
	QueryCounter         uint64
	PositiveCounter      uint64
	FalsePositiveCounter uint64
	mu                   sync.Mutex
}

func NewBloomStats() *BloomStats {
	return &BloomStats{
		QueryCounter:         0,
		PositiveCounter:      0,
		FalsePositiveCounter: 0,
	}
}

func (bs *BloomStats) BloomFilterStatsProcessor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if prometheusBloomQueryCounter != nil {
					bs.mu.Lock()
					prometheusBloomQueryCounter.Set(float64(bs.QueryCounter))
					prometheusBloomPositiveCounter.Set(float64(bs.PositiveCounter))
					prometheusBloomFalsePositiveCounter.Set(float64(bs.FalsePositiveCounter))
					bs.mu.Unlock()
				}

			}
		}
	}()
}

type Block struct {
	Header           *BlockHeader      `json:"header"`
	CoinbaseTx       *bt.Tx            `json:"coinbase_tx"`
	TransactionCount uint64            `json:"transaction_count"`
	SizeInBytes      uint64            `json:"size_in_bytes"`
	Subtrees         []*chainhash.Hash `json:"subtrees"`
	SubtreeSlices    []*util.Subtree   `json:"-"`
	Height           uint32            `json:"height"` // SAO - This can be left empty (i.e 0) as it is only used in legacy before the height was encoded in the coinbase tx (BIP-34)

	// local
	hash            *chainhash.Hash
	subtreeLength   uint64
	subtreeSlicesMu sync.RWMutex
	txMap           util.TxMap
}

type BlockBloomFilter struct {
	Filter       *blobloom.Filter
	BlockHash    *chainhash.Hash
	CreationTime time.Time
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash, transactionCount uint64, sizeInBytes uint64, blockHeight uint32) (*Block, error) {

	return &Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Subtrees:         subtrees,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		subtreeLength:    uint64(len(subtrees)),
		Height:           blockHeight,
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (block *Block, err error) {
	startTime := time.Now()

	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())
		if r := recover(); r != nil {
			err = errors.New(errors.ERR_BLOCK_INVALID, "error creating block from bytes", r)
			fmt.Println("Recovered in NewBlockFromBytes", r)
		}
	}()

	// check minimal block size
	// 92 bytes is the bare minimum, but will not be valid
	if len(blockBytes) < 92 {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "block is too small")
	}

	block = &Block{}

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "invalid block header", err)
	}

	return readBlockFromReader(block, bytes.NewReader(blockBytes[80:]))
}

func NewBlockFromReader(blockReader io.Reader) (block *Block, err error) {
	startTime := time.Now()

	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())
		if r := recover(); r != nil {
			err = errors.New(errors.ERR_BLOCK_INVALID, "error creating block from reader", r)
			fmt.Println("Recovered in NewBlockFromReader", r)
		}
	}()

	block = &Block{}

	blockHeaderBytes := make([]byte, 80)
	// read the first 80 bytes as the block header
	_, err = io.ReadFull(blockReader, blockHeaderBytes)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading block header", err)
	}

	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "invalid block header", err)
	}

	return readBlockFromReader(block, blockReader)
}

func readBlockFromReader(block *Block, buf io.Reader) (*Block, error) {
	var err error

	// read the transaction count
	block.TransactionCount, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading transaction count", err)
	}

	// read the size in bytes
	block.SizeInBytes, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading size in bytes", err)
	}

	// read the length of the subtree list
	block.subtreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading subtree length", err)
	}

	// read the subtree list
	var hashBytes [32]byte
	var subtreeHash *chainhash.Hash
	block.Subtrees = make([]*chainhash.Hash, 0, block.subtreeLength)
	for i := uint64(0); i < block.subtreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading subtree hash", err)
		}

		subtreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, errors.New(errors.ERR_BLOCK_INVALID, "error creating subtree hash", err)
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	var coinbaseTx bt.Tx
	if _, err := coinbaseTx.ReadFrom(buf); err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading coinbase tx", err)
	}
	block.CoinbaseTx = &coinbaseTx

	// Read in the block height
	blockHeight64, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "error reading block height", err)
	}

	block.Height = uint32(blockHeight64)

	// If the height is also stored in the coinbase, we should use that instead
	if block.Header.Version > 1 {
		block.Height, err = block.ExtractCoinbaseHeight()
		if err != nil {
			return nil, errors.New(errors.ERR_BLOCK_INVALID, "error extracting coinbase height", err)
		}
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

func (b *Block) Valid(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, txMetaStore txmetastore.Store, recentBlocksBloomFilters []*BlockBloomFilter, currentChain []*BlockHeader, currentBlockHeaderIDs []uint32, bloomStats *BloomStats) (bool, error) {
	startTime := time.Now()
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "Block:Valid")
	defer func() {
		span.Finish()
		prometheusBlockValid.Observe(time.Since(startTime).Seconds())
	}()

	// 1. Check that the block header hash is less than the target difficulty.
	headerValid, _, err := b.Header.HasMetTargetDifficulty()
	if !headerValid {
		return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] invalid block header: %s - %v", b.Hash().String(), b.Header.Hash().String(), err)
	}

	// 2. Check that the block timestamp is not more than two hours in the future.
	if b.Header.Timestamp > uint32(time.Now().Add(2*time.Hour).Unix()) {
		return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] block timestamp is more than two hours in the future", b.Hash().String())
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

		// TODO fix this for test mode when generating lots of blocks quickly
		// calculate the median timestamp
		//ts, err := medianTimestamp(prevTimeStamps)
		//if err != nil {
		//	return false, err
		//}

		// validate that the block's timestamp is after the median timestamp
		//if b.Header.Timestamp <= uint32(ts.Unix()) {
		//return false, fmt.Errorf("block timestamp %d is not after median time past of last %d blocks %d", b.Header.Timestamp, pruneLength, ts.Unix())
		//}
	}
	// 4. Check that the coinbase transaction is valid (reward checked later).
	if b.CoinbaseTx == nil {
		return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] block has no coinbase tx", b.Hash().String())
	}
	if !b.CoinbaseTx.IsCoinbase() {
		return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] block coinbase tx is not a valid coinbase tx", b.Hash().String())
	}

	if b.Header.Version > 1 {
		// We can only calculate the height from coinbase transactions in block versions 2 and higher

		// https://en.bitcoin.it/wiki/BIP_0034
		// BIP-34 was created to force miners to add the block height to the coinbase tx.
		// This BIP came into effect at block 227,835, which is after the first halving
		// at block 210,000.  Therefore, until this happened, we do not know the actual
		// height of the block we are checking for.

		// TODO - do this another way, if necessary

		// 5. Check that the coinbase transaction includes the correct block height.
		_, err := b.ExtractCoinbaseHeight()
		if err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] error extracting coinbase height: %v", b.Hash().String(), err)
		}
	}

	// only do the subtree checks if we have a subtree store
	// missing the subtreeStore should only happen when we are validating an internal block
	if subtreeStore != nil && len(b.Subtrees) > 0 {
		// 6. Get and validate any missing subtrees.
		if err = b.GetAndValidateSubtrees(spanCtx, logger, subtreeStore); err != nil {
			return false, err
		}

		// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
		//if !b.SubtreeSlices[0].Nodes[0].Hash.Equal(CoinbasePlaceholder) {
		//	return false, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] first transaction in first subtree is not a coinbase placeholder: %s", b.Hash().String(), b.SubtreeSlices[0].Nodes[0].Hash.String())
		//}

		//8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
		//    making sure to replace the coinbase placeholder with the coinbase tx hash in the first subtree
		if err = b.CheckMerkleRoot(spanCtx); err != nil {
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
		err = b.checkDuplicateTransactions(spanCtx)
		if err != nil {
			return false, err
		}
	}

	// 12. Check that all transactions are in the valid order and blessed
	//     Can only be done with a valid texMetaStore passed in

	// TODO - Re-enable order checking in all cases
	if !gocore.Config().GetBool("startLegacy", false) {
		if txMetaStore != nil {
			err = b.validOrderAndBlessed(spanCtx, logger, txMetaStore, subtreeStore, recentBlocksBloomFilters, currentChain, currentBlockHeaderIDs, bloomStats)
			if err != nil {
				return false, err
			}
		}
	}

	// reset the txMap and release the memory
	b.txMap = nil

	return true, nil
}

func (b *Block) checkBlockRewardAndFees(height uint32) error {
	// https://en.bitcoin.it/wiki/BIP_0034
	// BIP-34 was created to force miners to add the block height to the coinbase tx.
	// This BIP came into effect at block 227,835, which is after the first halving
	// at block 210,000.  Therefore, until this happened, we do not know the actual
	// height of the block we are checking for.

	// TODO - do this another way, if necessary
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

	coinbaseReward := util.GetBlockSubsidyForHeight(height)

	if height < 800_000 { // TODO - this is an arbitary number, we need to find the correct one
		if coinbaseOutputSatoshis > subtreeFees+coinbaseReward {
			return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] coinbase output (%d) is greated than fees + block subsidy (%d)", b.Hash().String(), coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
		}
	} else {
		// TODO should this be != instead of > ?
		if coinbaseOutputSatoshis != subtreeFees+coinbaseReward {
			return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] coinbase output (%d) is not equal to fees + block subsidy (%d)", b.Hash().String(), coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
		}
	}

	return nil
}

func (b *Block) checkDuplicateTransactions(ctx context.Context) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "Block:checkDuplicateTransactions")
	defer span.Finish()

	concurrency, _ := gocore.Config().GetInt("block_checkDuplicateTransactionsConcurrency", -1)
	if concurrency <= 0 {
		concurrency = util.Max(4, runtime.NumCPU()/2)
	}

	g := errgroup.Group{}
	g.SetLimit(concurrency)

	b.txMap = util.NewSplitSwissMapUint64(int(b.TransactionCount))
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
					if errors.Is(err, errors.ErrTxAlreadyExists) {
						return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] duplicate transaction %s", b.Hash().String(), subtreeNode.Hash.String())
					}
					return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] error adding transaction %s to txMap: %v", b.Hash().String(), subtreeNode.Hash.String(), err)
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

func (b *Block) validOrderAndBlessed(ctx context.Context, logger ulogger.Logger, txMetaStore txmetastore.Store, subtreeStore blob.Store,
	recentBlocksBloomFilters []*BlockBloomFilter, currentChain []*BlockHeader, currentBlockHeaderIDs []uint32, bloomStats *BloomStats) error {

	if b.txMap == nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] txMap is nil, cannot check transaction order", b.Hash().String())
	}

	currentBlockHeaderHashesMap := make(map[chainhash.Hash]struct{}, len(currentChain))
	for _, blockHeader := range currentChain {
		currentBlockHeaderHashesMap[*blockHeader.Hash()] = struct{}{}
	}

	currentBlockHeaderIDsMap := make(map[uint32]struct{}, len(currentBlockHeaderIDs))
	for _, id := range currentBlockHeaderIDs {
		currentBlockHeaderIDsMap[id] = struct{}{}
	}

	concurrency, _ := gocore.Config().GetInt("block_validOrderAndBlessedConcurrency", -1)
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
			retries := 0
			for {
				subtreeMetaSlice, err = b.getSubtreeMetaSlice(ctx, subtreeStore, *subtreeHash, subtree)
				if err != nil {
					if retries < 3 {
						retries++
						logger.Errorf("[BLOCK][%s][%s:%d] error getting subtree meta slice, retrying: %v", b.Hash().String(), subtreeHash.String(), sIdx, err)
						time.Sleep(1 * time.Second)
						continue
					}
					logger.Errorf("[BLOCK][%s][%s:%d] error getting subtree meta slice: %v", b.Hash().String(), subtreeHash.String(), sIdx, err)
				}

				break
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
					return errors.New(errors.ERR_NOT_FOUND, "[BLOCK][%s][%s:%d]:%d transaction %s is not in the txMap", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())
				}

				if subtreeMetaSlice != nil {
					parentTxHashes = subtreeMetaSlice.ParentTxHashes[snIdx]
				} else {
					// get from the txMetaStore
					var txMeta *txmetastore.Data
					retries = 0
					for {
						txMeta, err = txMetaStore.GetMeta(gCtx, &subtreeNode.Hash)
						if err != nil {
							if retries < 3 {
								retries++
								logger.Errorf("[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore, retrying: %v", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
								time.Sleep(1 * time.Second)
								continue
							}

							if errors.Is(err, txmetastore.NewErrTxmetaNotFound(&subtreeNode.Hash)) {
								return errors.New(errors.ERR_NOT_FOUND, "[BLOCK][%s][%s:%d]:%d transaction %s could not be found in tx txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
							}
							return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
						}

						break
					}

					if txMeta == nil {
						return fmt.Errorf("transaction %s is not blessed", subtreeNode.Hash.String())
					}
					parentTxHashes = txMeta.ParentTxHashes
				}
				if parentTxHashes == nil {
					return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s][%s:%d]:%d transaction %s could not be found in tx meta data", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String())
				}

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
							if errors.Is(err, txmetastore.NewErrTxmetaNotFound(&subtreeNode.Hash)) {
								continue
							}
							return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s][%s:%d]:%d error getting transaction %s from txMetaStore", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), err)
						}

						for _, blockID := range txMeta.BlockIDs {
							if _, found := currentBlockHeaderIDsMap[blockID]; found {
								return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s][%s:%d]:%d transaction %s has already been mined in block %d", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), blockID)
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
							return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s][%s:%d]:%d transaction %s comes before parent transaction %s in block", b.Hash().String(), subtreeHash.String(), sIdx, snIdx, subtreeNode.Hash.String(), parentTxHash.String())
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
						return b.checkParentExistsOnChain(gCtx, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
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
		return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] error validating transaction order: %v", b.Hash().String(), err)
	}

	return nil
}

func (b *Block) checkParentExistsOnChain(gCtx context.Context, txMetaStore txmetastore.Store, parentTxStruct missingParentTx, currentBlockHeaderIDsMap map[uint32]struct{}) error {
	// check whether the parent transaction has already been mined in a block on our chain
	// we need to get back to the txMetaStore for this, to make sure we have the latest data
	// two options: 1- parent is currently under validation, 2- parent is from forked chain.
	// for the first situation we don't start validating the current block until the parent is validated.
	parentTxMeta, err := txMetaStore.GetMeta(gCtx, &parentTxStruct.parentTxHash)
	if err != nil && !errors.Is(err, txmetastore.NewErrTxmetaNotFound(&parentTxStruct.parentTxHash)) {
		return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] error getting parent transaction %s from txMetaStore", b.Hash().String(), parentTxStruct.parentTxHash.String(), err)
	}
	// parent tx meta was not found, must be old, ignore | it is a coinbase, which obviously is mined in a block
	if parentTxMeta == nil || parentTxMeta.IsCoinbase {
		return nil
	}

	// check whether the parent is on our current chain (of 100 blocks), it should be, because the tx meta is still in the store
	foundInPreviousBlocks := make(map[uint32]struct{}, len(parentTxMeta.BlockIDs))
	for _, blockID := range parentTxMeta.BlockIDs {
		// TODO it is possible that the parent is much mich older, and does not exist on the current chain of last 100 blocks
		//      maybe check whether the block ID in the parent is older (lower number) than the lowest blockID in the current chain map?
		if _, found := currentBlockHeaderIDsMap[blockID]; found {
			foundInPreviousBlocks[blockID] = struct{}{}
		}
	}
	if len(foundInPreviousBlocks) != 1 {
		// log out the block header IDs map
		headerErr := fmt.Errorf("currentBlockHeaderIDs: %v", currentBlockHeaderIDsMap)
		headerErr = errors.Join(headerErr, fmt.Errorf("parent TxMeta: %v", parentTxMeta))

		// TODO TEMP code, remove this when we are sure the parent tx is in the store
		headerErr = errors.Join(headerErr, b.getFromAerospike(parentTxStruct))

		txMeta, err := txMetaStore.GetMeta(gCtx, &parentTxStruct.txHash)
		if err != nil {
			headerErr = errors.Join(headerErr, fmt.Errorf("txMetaStore error getting transaction %s: %v", parentTxStruct.txHash.String(), err))
		} else {
			headerErr = errors.Join(headerErr, fmt.Errorf("tx TxMeta: %v", txMeta))
		}

		// logger.Errorf("[BLOCK][%s] parent transaction %s of tx %s is not valid on our current chain, found %d times: %v", b.Hash().String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(foundInPreviousBlocks), headerErr)
		return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] parent transaction %s of tx %s is not valid on our current chain, found %d times", b.Hash().String(), parentTxStruct.parentTxHash.String(), parentTxStruct.txHash.String(), len(foundInPreviousBlocks), headerErr)
	}

	return nil
}

func (b *Block) getFromAerospike(parentTxStruct missingParentTx) error {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Printf("Recovered in getFromAerospike: %v\n", err)
		}
	}()

	aeroURL, err, _ := gocore.Config().GetURL("txmeta_store")
	if err != nil {
		return fmt.Errorf("aerospike get URL error: %w", err)
	}

	portStr := aeroURL.Port()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("aerospike port error: %w", err)
	}

	client, aErr := aerospike.NewClient(aeroURL.Host, port)
	if aErr != nil {
		return fmt.Errorf("aerospike error: %w", aErr)
	}

	key, aeroErr := aerospike.NewKey(aeroURL.Path[1:], aeroURL.Query().Get("set"), parentTxStruct.txHash.CloneBytes())
	if aeroErr != nil {
		return fmt.Errorf("aerospike error: %w", aeroErr)
	}

	response, aErr := client.Get(nil, key)
	if aErr != nil {
		return fmt.Errorf("aerospike error: %w", aErr)
	}

	return fmt.Errorf("aerospike response: %v", response)
}

func (b *Block) GetSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store) ([]*util.Subtree, error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockGetSubtrees.Observe(time.Since(startTime).Seconds())
	}()

	// get the subtree slices from the subtree store
	if err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore); err != nil {
		return nil, err
	}

	return b.SubtreeSlices, nil
}

func (b *Block) GetAndValidateSubtrees(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store) error {
	startTime := time.Now()
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "Block:GetAndValidateSubtrees")
	defer func() {
		span.Finish()
		prometheusBlockGetAndValidateSubtrees.Observe(time.Since(startTime).Seconds())
	}()

	b.subtreeSlicesMu.Lock()
	defer func() {
		b.subtreeSlicesMu.Unlock()
	}()

	if len(b.Subtrees) == len(b.SubtreeSlices) {
		// already loaded
		return nil
	}

	b.SubtreeSlices = make([]*util.Subtree, len(b.Subtrees))

	var sizeInBytes atomic.Uint64
	var txCount atomic.Uint64

	concurrency, _ := gocore.Config().GetInt("block_getAndValidateSubtreesConcurrency", -1)
	if concurrency <= 0 {
		concurrency = util.Max(4, runtime.NumCPU()/2)
	}

	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(concurrency)
	// we have the hashes. Get the actual subtrees from the subtree store
	for i, subtreeHash := range b.Subtrees {
		i := i
		if b.SubtreeSlices[i] == nil {
			subtreeHash := subtreeHash

			g.Go(func() error {
				// retry to get the subtree from the store 3 times, there are instances when we get an EOF error,
				// probably when being moved to permanent storage in another service
				retries := 0
				subtree := &util.Subtree{}
				for { // retry for loop
					select {
					case <-gCtx.Done():
						return gCtx.Err()
					default:

						subtreeReader, err := subtreeStore.GetIoReader(gCtx, subtreeHash[:])
						if err != nil {
							if retries < 3 {
								retries++
								backoff := time.Duration(2^retries) * time.Second
								logger.Warnf("[BLOCK][%s] failed to get subtree %s, retrying %d in %s", b.Hash().String(), subtreeHash.String(), retries, backoff.String())
								time.Sleep(backoff)
								continue
							}
							return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] failed to get subtree %s", b.Hash().String(), subtreeHash.String(), err)
						}

						err = subtree.DeserializeFromReader(subtreeReader)
						if err != nil {
							_ = subtreeReader.Close()
							if retries < 3 {
								retries++
								backoff := time.Duration(2^retries) * time.Second
								logger.Warnf("[BLOCK][%s] failed to deserialize subtree %s, retrying %d in %s", b.Hash().String(), subtreeHash.String(), retries, backoff)
								time.Sleep(backoff)
								continue
							}
							return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] failed to deserialize subtree %s", b.Hash().String(), subtreeHash.String(), err)
						}

						b.SubtreeSlices[i] = subtree

						sizeInBytes.Add(subtree.SizeInBytes)
						txCount.Add(uint64(subtree.Length()))

						_ = subtreeReader.Close()
						break

					}

					return nil
				}
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
		} else {
			// all subtrees need to be the same size as the first tree, except the last one
			if subtree.Length() != subtreeSize && sIdx != nrOfSubtrees-1 {
				return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] subtree %d has length %d, expected %d", b.Hash().String(), sIdx, subtree.Length(), subtreeSize)
			}
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
		return nil, fmt.Errorf("[BLOCK][%s][%s] failed to get subtree meta: %w", b.Hash().String(), subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeMetaReader.Close()
	}()

	// no need to check whether this fails or not, it's just a cache file and not critical
	subtreeMetaSlice, err := util.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
	if err != nil {
		return nil, fmt.Errorf("[BLOCK][%s][%s] failed to deserialize subtree meta: %w", b.Hash().String(), subtreeHash.String(), err)
	}

	return subtreeMetaSlice, nil
}

func (b *Block) CheckMerkleRoot(ctx context.Context) (err error) {
	if len(b.Subtrees) != len(b.SubtreeSlices) {
		return errors.New(errors.ERR_STORAGE_ERROR, "[BLOCK][%s] number of subtrees does not match number of subtree slices", b.Hash().String())
	}

	startTime := time.Now()
	span, _ := opentracing.StartSpanFromContext(ctx, "Block:CheckMerkleRoot")
	defer func() {
		span.Finish()
		prometheusBlockCheckMerkleRoot.Observe(time.Since(startTime).Seconds())
	}()

	hashes := make([]chainhash.Hash, len(b.Subtrees))
	for sIdx := 0; sIdx < len(b.SubtreeSlices); sIdx++ {
		subtree := b.SubtreeSlices[sIdx]
		if sIdx == 0 {
			// We need to inject the coinbase tx id into the first position of the first subtree
			rootHash, err := subtree.RootHashWithReplaceRootNode(b.CoinbaseTx.TxIDChainHash(), 0, uint64(b.CoinbaseTx.Size()))
			if err != nil {
				return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error replacing root node in subtree", b.Hash().String(), err)
			}
			hashes[sIdx] = *rootHash
		} else {
			hashes[sIdx] = *subtree.RootHash()
		}
	}

	var calculatedMerkleRootHash *chainhash.Hash
	if len(hashes) == 1 {
		calculatedMerkleRootHash = &hashes[0]
	} else if len(hashes) > 0 {
		// Create a new subtree with the hashes of the subtrees
		st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(b.Subtrees)))
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error creating new root tree", b.Hash().String(), err)
		}

		for _, hash := range hashes {
			err = st.AddNode(hash, 1, 0)
			if err != nil {
				return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error adding node to root tree", b.Hash().String(), err)
			}
		}

		calculatedMerkleRoot := st.RootHash()
		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error creating calculated merkle root hash", b.Hash().String(), err)
		}
	} else {
		calculatedMerkleRootHash = b.CoinbaseTx.TxIDChainHash()
	}

	if !b.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		return errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] merkle root does not match", b.Hash().String())
	}

	return nil
}

// ExtractCoinbaseHeight attempts to extract the height of the block from the
// scriptSig of a coinbase transaction.  Coinbase's heights are only present in
// blocks of version 2 or later.  This was added as part of BIP0034.
func (b *Block) ExtractCoinbaseHeight() (uint32, error) {
	if b.CoinbaseTx == nil {
		return 0, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] missing coinbase transaction", b.Hash().String())
	}

	if len(b.CoinbaseTx.Inputs) != 1 {
		return 0, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] multiple coinbase transactions", b.Hash().String())
	}

	return util.ExtractCoinbaseHeight(b.CoinbaseTx)
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing subtree length", b.Hash().String(), err)
	}
	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing subtree hash", b.Hash().String(), err)
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)
	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error reading subtree length", b.Hash().String(), err)
	}

	var subtreeBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < subTreeCount; i++ {
		_, err = io.ReadFull(buf, subtreeBytes[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error reading subtree hash", b.Hash().String(), err)
		}
		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error creating subtree hash", b.Hash().String(), err)
		}
		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	return nil
}

func (b *Block) Bytes() ([]byte, error) {
	if b.Header == nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] block has no header", b.Hash().String())
	}

	if b.CoinbaseTx == nil {
		return nil, errors.New(errors.ERR_BLOCK_INVALID, "[BLOCK][%s] block has no coinbase tx", b.Hash().String())
	}

	// write the header
	buf := bytes.NewBuffer(b.Header.Bytes())

	// write the transaction count
	err := wire.WriteVarInt(buf, 0, b.TransactionCount)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing transaction count", b.Hash().String(), err)
	}

	// write the size in bytes
	err = wire.WriteVarInt(buf, 0, b.SizeInBytes)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing size in bytes", b.Hash().String(), err)
	}

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing subtree list", b.Hash().String(), err)
	}
	buf.Write(subtreeBytes)

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing coinbase tx", b.Hash().String(), err)
	}

	err = wire.WriteVarInt(buf, 0, uint64(b.Height))
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] error writing height", b.Hash().String(), err)
	}

	return buf.Bytes(), nil
}

func (b *Block) NewOptimizedBloomFilter(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store) (*blobloom.Filter, error) {
	err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore)
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
			return nil, errors.New(errors.ERR_PROCESSING, "[BLOCK][%s] missing subtree %d", b.Hash().String(), sIdx)
		}
		for nodeIdx := 0; nodeIdx < len(subtree.Nodes); nodeIdx++ {
			if sIdx == 0 && nodeIdx == 0 {
				// skip coinbase
				continue
			}
			binary.BigEndian.PutUint64(subtree.Nodes[nodeIdx].Hash.CloneBytes(), n64)
			filter.Add(n64)
		}
	}

	return filter, nil
}

func medianTimestamp(timestamps []time.Time) (*time.Time, error) {
	n := len(timestamps)

	if n == 0 {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "no timestamps provided")
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
