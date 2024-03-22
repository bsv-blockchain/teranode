package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/greatroar/blobloom"

	"github.com/ordishs/gocore"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
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
	Header            *BlockHeader        `json:"header"`
	CoinbaseTx        *bt.Tx              `json:"coinbase_tx"`
	TransactionCount  uint64              `json:"transaction_count"`
	SizeInBytes       uint64              `json:"size_in_bytes"`
	Subtrees          []*chainhash.Hash   `json:"subtrees"`
	SubtreeSlices     []*util.Subtree     `json:"-"`
	SubtreeMetaSlices []*util.SubtreeMeta `json:"-"`

	// local
	hash            *chainhash.Hash
	subtreeLength   uint64
	subtreeSlicesMu sync.RWMutex
	txMap           util.TxMap
}

type BlockBloomFilter struct {
	Filter       *blobloom.Filter
	CreationTime time.Time
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash, transactionCount uint64, sizeInBytes uint64) (*Block, error) {

	return &Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Subtrees:         subtrees,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		subtreeLength:    uint64(len(subtrees)),
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (*Block, error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockFromBytes.Observe(time.Since(startTime).Seconds())
		if r := recover(); r != nil {
			fmt.Println("Recovered in NewBlockFromBytes", r)
		}
	}()

	// check minimal block size
	// 92 bytes is the bare minimum, but will not be valid
	if len(blockBytes) < 92 {
		return nil, errors.New("block is too small")
	}

	block := &Block{}

	var err error

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, err
	}

	// create new buffer reader for the block bytes
	buf := bytes.NewReader(blockBytes[80:])

	// read the transaction count
	block.TransactionCount, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the size in bytes
	block.SizeInBytes, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the length of the subtree list
	block.subtreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the subtree list
	var hashBytes [32]byte
	var subtreeHash *chainhash.Hash
	block.Subtrees = make([]*chainhash.Hash, 0, block.subtreeLength)
	for i := uint64(0); i < block.subtreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}

		subtreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	coinbaseTxBytes, _ := io.ReadAll(buf) // read the rest of the bytes as the coinbase tx
	block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTxBytes)
	if err != nil {
		return nil, err
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
	height, err := util.ExtractCoinbaseHeight(b.CoinbaseTx)
	if err != nil {
		height = 0
	}

	return fmt.Sprintf("Block %s (height: %d, txCount: %d, size: %d", b.Hash().String(), height, b.TransactionCount, b.SizeInBytes)
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
		return false, fmt.Errorf("invalid block header: %s - %v", b.Header.Hash().String(), err)
	}

	// 2. Check that the block timestamp is not more than two hours in the future.
	if b.Header.Timestamp > uint32(time.Now().Add(2*time.Hour).Unix()) {
		return false, fmt.Errorf("block timestamp is more than two hours in the future")
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
		return false, fmt.Errorf("block has no coinbase tx")
	}
	if !b.CoinbaseTx.IsCoinbase() {
		return false, fmt.Errorf("block coinbase tx is not a valid coinbase tx")
	}

	var height uint32
	if b.Header.Version > 1 {
		// We can only calculate the height from coinbase transactions in block versions 2 and higher

		// https://en.bitcoin.it/wiki/BIP_0034
		// BIP-34 was created to force miners to add the block height to the coinbase tx.
		// This BIP came into effect at block 227,835, which is after the first halving
		// at block 210,000.  Therefore, until this happened, we do not know the actual
		// height of the block we are checking for.

		// TODO - do this another way, if necessary

		// 5. Check that the coinbase transaction includes the correct block height.
		height, err = b.ExtractCoinbaseHeight()
		if err != nil {
			return false, err
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
		if !b.SubtreeSlices[0].Nodes[0].Hash.Equal(CoinbasePlaceholder) {
			return false, fmt.Errorf("first transaction in first subtree is not a coinbase placeholder")
		}

		//8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
		//    making sure to replace the coinbase placeholder with the coinbase tx hash in the first subtree
		if err = b.CheckMerkleRoot(spanCtx); err != nil {
			return false, err
		}
	}

	// 9. Check that the total fees of the block are less than or equal to the block reward.
	// 10. Check that the coinbase transaction includes the correct block reward.
	if height > 0 {
		err = b.checkBlockRewardAndFees(height)
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
	if txMetaStore != nil {
		err = b.validOrderAndBlessed(spanCtx, logger, txMetaStore, recentBlocksBloomFilters, currentBlockHeaderIDs, bloomStats)
		if err != nil {
			return false, err
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
	for _, subtree := range b.SubtreeSlices {
		subtreeFees += subtree.Fees
	}

	coinbaseReward := util.GetBlockSubsidyForHeight(height)
	// TODO should this be != instead of > ?
	if coinbaseOutputSatoshis != subtreeFees+coinbaseReward {
		return fmt.Errorf("fees paid (%d) are not equal to fees + block reward (%d)", coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
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
	for subIdx, subtree := range b.SubtreeSlices {
		subIdx := subIdx
		subtree := subtree
		g.Go(func() (err error) {
			for txIdx, subtreeNode := range subtree.Nodes {
				// in a tx map, Put is mutually exclusive, can only be called once per key
				err = b.txMap.Put(subtreeNode.Hash, uint64((subIdx*len(subtree.Nodes))+txIdx))
				if err != nil {
					return fmt.Errorf("error adding transaction %s to txMap: %v", subtreeNode.Hash.String(), err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error checking for duplicate transactions: %v", err)
	}

	return nil
}

func (b *Block) validOrderAndBlessed(ctx context.Context, logger ulogger.Logger, txMetaStore txmetastore.Store, recentBlocksBloomFilters []*BlockBloomFilter, currentBlockHeaderIDs []uint32, bloomStats *BloomStats) error {
	if b.txMap == nil {
		return fmt.Errorf("txMap is nil, cannot check transaction order")
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

	for sIdx, subtree := range b.SubtreeSlices {
		sIdx := sIdx
		subtree := subtree
		g.Go(func() error {
			var parentTxHashes []chainhash.Hash
			bloomStats.mu.Lock()
			bloomStats.QueryCounter += uint64(len(subtree.Nodes))
			bloomStats.mu.Unlock()
			for snIdx, subtreeNode := range subtree.Nodes {
				if subtreeNode.Hash.IsEqual(CoinbasePlaceholderHash) {
					continue
				}

				// ignore the very first transaction, is coinbase
				if sIdx == 0 && snIdx == 0 {
					continue
				}

				// if the subtree meta slice is loaded, we can use that instead of the txMetaStore
				subtreeMetaSlice := b.SubtreeMetaSlices[sIdx]

				txIdx, ok := b.txMap.Get(subtreeNode.Hash)
				if !ok {
					return fmt.Errorf("transaction %s is not in the txMap", subtreeNode.Hash.String())
				}

				if subtreeMetaSlice != nil {
					parentTxHashes = subtreeMetaSlice.ParentTxHashes[snIdx]
				} else {
					// get from the txMetaStore
					txMeta, err := txMetaStore.Get(gCtx, &subtreeNode.Hash)
					if err != nil {
						return fmt.Errorf("error getting transaction %s from txMetaStore: %v", subtreeNode.Hash.String(), err)
					} else if txMeta == nil {
						return fmt.Errorf("transaction %s is not blessed", subtreeNode.Hash.String())
					}
					parentTxHashes = txMeta.ParentTxHashes
				}
				if parentTxHashes == nil {
					return fmt.Errorf("transaction %s could not be found in tx meta data", subtreeNode.Hash.String())
				}

				// check whether the transaction has recently been mined in a block on our chain
				// for all transactions, we go over all bloom filters, we collect the transactions that are in the bloom filter
				// collected transactions will be checked in the txMetaStore, as they can be false positives.
				// get first 8 bytes of the subtreeNode hash
				n64 := binary.BigEndian.Uint64(subtreeNode.Hash[:])

				for _, filter := range recentBlocksBloomFilters {
					if filter.Filter.Has(n64) {
						// we have a match, check the txMetaStore
						bloomStats.mu.Lock()
						bloomStats.PositiveCounter++
						bloomStats.mu.Unlock()
						txMeta, err := txMetaStore.Get(gCtx, &subtreeNode.Hash)
						if err != nil {
							return fmt.Errorf("error getting transaction %s from txMetaStore: %v", subtreeNode.Hash.String(), err)
						}

						for _, blockID := range txMeta.BlockIDs {
							if _, found := currentBlockHeaderIDsMap[blockID]; found {
								return fmt.Errorf("transaction %s has already been mined in block %d", subtreeNode.Hash.String(), blockID)
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
							return fmt.Errorf("transaction %s comes before parent transaction %s in block", subtreeNode.Hash.String(), parentTxHash.String())
						}

						// if the parent is in the same block, we have already checked whether it is on the same chain
						// in a previous block here above. No need to check again
						continue
					}

					// check whether the parent transaction has already been mined in a block on our chain
					// we need to get back to the txMetaStore for this, to make sure we have the latest data
					// two options: 1- parent is currently under validation, 2- parent is from forked chain.
					// for the first situation we don't start validating the current block until the parent is validated.
					parentTxMeta, err := txMetaStore.Get(gCtx, &parentTxHash)
					if err != nil && !errors.Is(err, txmetastore.NewErrTxmetaNotFound(&parentTxHash)) {
						return fmt.Errorf("error getting parent transaction %s of %s from txMetaStore: %v", parentTxHash.String(), subtreeNode.Hash.String(), err)
					}
					// parent tx meta was not found, must be old, ignore
					if parentTxMeta == nil {
						continue
					}

					// check whether the parent is on our current chain (of 100 blocks), it should be, because the tx meta is still in the store
					foundInPreviousBlocks := 0
					for _, blockID := range parentTxMeta.BlockIDs {
						if _, found := currentBlockHeaderIDsMap[blockID]; found {
							foundInPreviousBlocks++
						}
					}
					if foundInPreviousBlocks != 1 {
						return fmt.Errorf("parent transaction %s of %s is not valid on our current chain", parentTxHash.String(), subtreeNode.Hash.String())
					}
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error validating transaction order: %v", err)
	}

	return nil
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
	b.SubtreeMetaSlices = make([]*util.SubtreeMeta, len(b.Subtrees))

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
					subtreeReader, err := subtreeStore.GetIoReader(gCtx, subtreeHash[:])
					if err != nil {
						if retries < 3 {
							retries++
							logger.Warnf("failed to get subtree %s, retrying %d", subtreeHash.String(), retries)
							continue
						}
						return errors.Join(fmt.Errorf("failed to get subtree %s", subtreeHash.String()), err)
					}

					err = subtree.DeserializeFromReader(subtreeReader)
					if err != nil {
						_ = subtreeReader.Close()
						if retries < 3 {
							retries++
							logger.Warnf("failed to deserialize subtree %s, retrying %d", subtreeHash.String(), retries)
							continue
						}
						return errors.Join(fmt.Errorf("failed to deserialize subtree %s", subtreeHash.String()), err)
					}

					b.SubtreeSlices[i] = subtree

					sizeInBytes.Add(subtree.SizeInBytes)
					txCount.Add(uint64(subtree.Length()))

					_ = subtreeReader.Close()
					break
				}

				// get subtree meta
				subtreeMetaReader, err := subtreeStore.GetIoReader(gCtx, subtreeHash[:], options.WithFileExtension("meta"))
				if err != nil {
					logger.Warnf("failed to get subtree meta %s: %w", subtreeHash.String(), err)
					return nil
				}
				defer func() {
					_ = subtreeMetaReader.Close()
				}()

				b.SubtreeMetaSlices[i], err = util.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
				if err != nil {
					logger.Warnf("failed to deserialize subtree meta %s: %w", subtreeHash.String(), err)
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to get and validate subtrees: %v", err)
	}

	// check that the size of all subtrees is the same
	var subtreeSize int
	nrOfSubtrees := len(b.Subtrees)
	for i, subtree := range b.SubtreeSlices {
		if i == 0 {
			subtreeSize = subtree.Length()
		} else {
			// all subtrees need to be the same size as the first tree, except the last one
			if subtree.Length() != subtreeSize && i != nrOfSubtrees-1 {
				return fmt.Errorf("subtree %d has length %d, expected %d", i, subtree.Length(), subtreeSize)
			}
		}
	}

	b.TransactionCount = txCount.Load()
	// header + transaction count + size in bytes + coinbase tx size
	b.SizeInBytes = sizeInBytes.Load() + 80 + util.VarintSize(b.TransactionCount) + uint64(b.CoinbaseTx.Size())

	// TODO something with conflicts

	return nil
}

func (b *Block) CheckMerkleRoot(ctx context.Context) (err error) {
	if len(b.Subtrees) != len(b.SubtreeSlices) {
		return fmt.Errorf("number of subtrees does not match number of subtree slices")
	}

	startTime := time.Now()
	span, _ := opentracing.StartSpanFromContext(ctx, "Block:CheckMerkleRoot")
	defer func() {
		span.Finish()
		prometheusBlockCheckMerkleRoot.Observe(time.Since(startTime).Seconds())
	}()

	hashes := make([]chainhash.Hash, len(b.Subtrees))
	for i, subtree := range b.SubtreeSlices {
		if i == 0 {
			// We need to inject the coinbase txid into the first position of the first subtree
			subtree.ReplaceRootNode(b.CoinbaseTx.TxIDChainHash(), 0, uint64(b.CoinbaseTx.Size()))
		}

		hashes[i] = *subtree.RootHash()
	}

	var calculatedMerkleRootHash *chainhash.Hash
	if len(hashes) == 1 {
		calculatedMerkleRootHash = &hashes[0]
	} else if len(hashes) > 0 {
		// Create a new subtree with the hashes of the subtrees
		st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(b.Subtrees)))
		if err != nil {
			return err
		}

		for _, hash := range hashes {
			err = st.AddNode(hash, 1, 0)
			if err != nil {
				return err
			}
		}

		calculatedMerkleRoot := st.RootHash()
		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return err
		}
	} else {
		calculatedMerkleRootHash = b.CoinbaseTx.TxIDChainHash()
	}

	if !b.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		return errors.New("merkle root does not match")
	}

	return nil
}

// ExtractCoinbaseHeight attempts to extract the height of the block from the
// scriptSig of a coinbase transaction.  Coinbase's heights are only present in
// blocks of version 2 or later.  This was added as part of BIP0034.
func (b *Block) ExtractCoinbaseHeight() (uint32, error) {
	if b.CoinbaseTx == nil {
		return 0, fmt.Errorf("ErrMissingCoinbase")
	}

	if len(b.CoinbaseTx.Inputs) != 1 {
		return 0, fmt.Errorf("ErrMultipleCoinbase")
	}

	return util.ExtractCoinbaseHeight(b.CoinbaseTx)
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, err
	}
	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)
	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return err
	}

	var subtreeBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < subTreeCount; i++ {
		_, err = buf.Read(subtreeBytes[:])
		if err != nil {
			return err
		}
		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return err
		}
		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	return err
}

func (b *Block) Bytes() ([]byte, error) {
	if b.Header == nil {
		return nil, fmt.Errorf("block has no header")
	}

	if b.CoinbaseTx == nil {
		return nil, fmt.Errorf("block has no coinbase tx")
	}

	// write the header
	buf := bytes.NewBuffer(b.Header.Bytes())

	// write the transaction count
	err := wire.WriteVarInt(buf, 0, b.TransactionCount)
	if err != nil {
		return nil, err
	}

	// write the size in bytes
	err = wire.WriteVarInt(buf, 0, b.SizeInBytes)
	if err != nil {
		return nil, err
	}

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, err
	}
	buf.Write(subtreeBytes)

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b *Block) NewOptimizedBloomFilter(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store) (*blobloom.Filter, error) {
	err := b.GetAndValidateSubtrees(ctx, logger, subtreeStore)
	if err != nil {
		return nil, err
	}

	filter := blobloom.NewOptimized(blobloom.Config{
		Capacity: b.TransactionCount, // Expected number of keys.
		FPRate:   1e-5,               // Accept one false positive per 100,000 lookups.
	})

	var n64 uint64
	// insert all transaction ids first 8 bytes to the filter
	for idx, subtree := range b.SubtreeSlices {
		if subtree == nil {
			return nil, fmt.Errorf("subtree %d is nil: %s", idx, b.hash.String())
		}
		for _, node := range subtree.Nodes {
			binary.BigEndian.PutUint64(node.Hash[:], n64)
			filter.Add(n64)
		}
	}

	return filter, nil
}

func medianTimestamp(timestamps []time.Time) (*time.Time, error) {
	n := len(timestamps)

	if n == 0 {
		return nil, errors.New("no timestamps provided")
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
