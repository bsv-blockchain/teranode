package subtreevalidation

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type missingTx struct {
	tx  *bt.Tx
	idx int
}

func (u *Server) SetSubtreeExists(hash *chainhash.Hash) error {
	// TODO: implement for local storage
	return nil
}

func (u *Server) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	// TODO: implement for local storage
	start, stat, ctx := util.StartStatFromContext(ctx, "GetSubtreeExists")
	defer func() {
		stat.AddTime(start)
	}()
	_ = ctx
	return false, nil
}

func (u *Server) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *txmeta.Data) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCache")
		defer func() {
			span.Finish()
		}()

		return cache.SetCache(hash, txMeta)
	}

	return nil
}

func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

func (u *Server) SetTxMetaCacheMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMinedMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetMinedMulti(ctx, hashes, blockID)
	}

	return nil
}

func (u *Server) SetTxMetaCacheMulti(ctx context.Context, keys [][]byte, values [][]byte) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetCacheMulti(keys, values)
	}

	return nil
}

func (u *Server) DelTxMetaCache(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:DelTxMetaCache")
		defer func() {
			span.Finish()
		}()

		return cache.Delete(ctx, hash)
	}

	return nil
}

func (u *Server) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:DelTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.Delete(ctx, hash)
	}

	return nil
}

// getMissingTransactionsBatch gets a batch of transactions from the network
// NOTE: it does not return the transactions in the same order as the txHashes
func (u *Server) getMissingTransactionsBatch(ctx context.Context, txHashes []txmeta.MissingTxHash, baseUrl string) ([]*bt.Tx, error) {
	txIDBytes := make([]byte, 32*len(txHashes))
	for idx, txHash := range txHashes {
		copy(txIDBytes[idx*32:(idx+1)*32], txHash.Hash[:])
	}

	// do http request to baseUrl + txHash.String()
	u.logger.Debugf("[getMissingTransactionsBatch] getting %d txs from other miner %s", len(txHashes), baseUrl)
	url := fmt.Sprintf("%s/txs", baseUrl)
	body, err := util.DoHTTPRequestBodyReader(ctx, url, txIDBytes)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to do http request"), err)
	}
	defer body.Close()

	// read the body into transactions using go-bt
	missingTxs := make([]*bt.Tx, 0, len(txHashes))
	var tx *bt.Tx
	for {
		tx, err = u.readTxFromReader(body)
		if err != nil || tx == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to read transaction from body"), err)
		}

		missingTxs = append(missingTxs, tx)
	}

	return missingTxs, nil
}

func (u *Server) readTxFromReader(body io.ReadCloser) (tx *bt.Tx, err error) {
	defer func() {
		// there is a bug in go-bt, that does not check input and throws a runtime error in
		// github.com/libsv/go-bt/v2@v2.2.2/input.go:76 +0x16b
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(errors.ERR_UNKNOWN, x)
			case error:
				err = x
			default:
				err = fmt.Errorf("unknown panic: %v", r)
			}
		}
	}()

	tx = &bt.Tx{}
	_, err = tx.ReadFrom(body)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// func (u *Server) getMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*bt.Tx, error) {
// 	//startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
// 	defer func() {
// 		//stat.AddTime(startTotal)
// 	}()

// 	// get transaction from network over http using the baseUrl
// 	if baseUrl == "" {
// 		return nil, fmt.Errorf("[getMissingTransaction][%s] baseUrl for transaction is empty", txHash.String())
// 	}

// 	//start := gocore.CurrentTime()
// 	alreadyHaveTransaction := true
// 	txBytes, err := u.txStore.Get(ctx, txHash[:])
// 	//stat.NewStat("getTxFromStore").AddTime(start)
// 	if txBytes == nil || err != nil {
// 		alreadyHaveTransaction = false

// 		// do http request to baseUrl + txHash.String()
// 		u.logger.Infof("[getMissingTransaction][%s] getting tx from other miner", txHash.String(), baseUrl)
// 		url := fmt.Sprintf("%s/tx/%s", baseUrl, txHash.String())
// 		//startM := gocore.CurrentTime()
// 		//statM := stat.NewStat("http fetch missing tx")
// 		txBytes, err = util.DoHTTPRequest(ctx, url)
// 		//statM.AddTime(startM)
// 		if err != nil {
// 			return nil, errors.Join(fmt.Errorf("[getMissingTransaction][%s] failed to do http request", txHash.String()), err)
// 		}
// 	}

// 	// validate the transaction by creating a transaction object
// 	tx, err := bt.NewTxFromBytes(txBytes)
// 	if err != nil {
// 		return nil, fmt.Errorf("[getMissingTransaction][%s] failed to create transaction from bytes [%s]", txHash.String(), err.Error())
// 	}

// 	if !alreadyHaveTransaction {
// 		//start = gocore.CurrentTime()
// 		// store the transaction, we did not get it via propagation
// 		err = u.txStore.Set(ctx, txHash[:], txBytes)
// 		//stat.NewStat("storeTx").AddTime(start)
// 		if err != nil {
// 			return nil, fmt.Errorf("[getMissingTransaction][%s] failed to store transaction [%s]", txHash.String(), err.Error())
// 		}
// 	}

// 	return tx, nil
// }

func (u *Server) blessMissingTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32) (txMeta *txmeta.Data, err error) {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
	defer func() {
		stat.AddTime(startTotal)
		prometheusSubtreeValidationBlessMissingTransaction.Inc()
		prometheusSubtreeValidationBlessMissingTransactionDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)
	}()

	if tx == nil {
		return nil, fmt.Errorf("[blessMissingTransaction] tx is nil")
	}
	u.logger.Debugf("[blessMissingTransaction][%s] called", tx.TxID())

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] transaction is coinbase", tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	// todo return tx meta data
	err = u.validatorClient.Validate(ctx, tx, blockHeight)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to validate transaction [%s]", tx.TxID(), err.Error())
	}

	start := gocore.CurrentTime()
	txMeta, err = u.txMetaStore.GetMeta(ctx, tx.TxIDChainHash())
	stat.NewStat("getTxMeta").AddTime(start)
	if err != nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to get tx meta [%s]", tx.TxID(), err.Error())
	}

	if txMeta == nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] tx meta is nil", tx.TxID())
	}

	return txMeta, nil
}

type ValidateSubtree struct {
	SubtreeHash   chainhash.Hash
	BaseUrl       string
	SubtreeHashes []chainhash.Hash
	AllowFailFast bool
}

func (u *Server) validateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "validateSubtreeBlobInternal")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateSubtree")
	span.LogKV("subtree", v.SubtreeHash.String())
	defer func() {
		span.Finish()
		stat.AddTime(startTotal)
		prometheusSubtreeValidationValidateSubtree.Inc()
	}()

	u.logger.Infof("[validateSubtreeInternal][%s] called", v.SubtreeHash.String())

	start := gocore.CurrentTime()

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	txHashes := v.SubtreeHashes

	if txHashes == nil {
		subtreeExists, err := u.GetSubtreeExists(spanCtx, &v.SubtreeHash)
		stat.NewStat("1. subtreeExists").AddTime(start)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to check if subtree exists in store", v.SubtreeHash.String()), err)
		}
		if subtreeExists {
			// subtree already exists in store, which means it's valid
			// TODO is this true?
			return nil
		}

		// The function was called by BlockFound, and we had not already blessed the subtree, so we load the subtree from the store to get the hashes
		// get subtree from network over http using the baseUrl
		for retries := 0; retries < 3; retries++ {
			txHashes, err = u.getSubtreeTxHashes(spanCtx, stat, &v.SubtreeHash, v.BaseUrl)
			if err != nil {
				if retries < 2 {
					backoff := time.Duration(2^retries) * time.Second
					u.logger.Warnf("[validateSubtreeInternal][%s] failed to get subtree from network (try %d), will retry in %s", v.SubtreeHash.String(), retries, backoff.String())
					time.Sleep(backoff)
				} else {
					return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to get subtree from network", v.SubtreeHash.String()), err)
				}
			} else {
				break
			}
		}
	}

	// create the empty subtree
	height := math.Ceil(math.Log2(float64(len(txHashes))))
	subtree, err := util.NewTree(int(height))
	if err != nil {
		return err
	}

	subtreeMeta := util.NewSubtreeMeta(subtree)

	failFastValidation := gocore.Config().GetBool("blockvalidation_fail_fast_validation", false)
	abandonTxThreshold, _ := gocore.Config().GetInt("blockvalidation_subtree_validation_abandon_threshold", 10000)
	maxRetries, _ := gocore.Config().GetInt("blockvalidation_validation_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("blockvalidation_validation_retry_sleep", 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("invalid value for blockvalidation_fail_fast_validation_retry_sleep: %v", err))
	}

	// TODO document, what does this do?
	subtreeWarmupCount, _ := gocore.Config().GetInt("blockvalidation_validation_warmup_count", 128)

	// TODO document, what is the logic here?
	failFast := v.AllowFailFast && failFastValidation && u.subtreeCount.Add(1) > int32(subtreeWarmupCount)

	// txMetaSlice will be populated with the txMeta data for each txHash
	// in the retry attempts, only the tx hashes that are missing will be retried, not the whole subtree
	txMetaSlice := make([]*txmeta.Data, len(txHashes))
	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		prometheusSubtreeValidationValidateSubtreeRetry.Inc()

		if attempt > maxRetries {
			failFast = false
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] final attempt to process subtree, this time with full checks enabled", v.SubtreeHash.String(), attempt)
		} else {
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] (fail fast=%v) process %d txs from subtree", v.SubtreeHash.String(), attempt, failFast, len(txHashes))
		}

		// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

		// 1. First attempt to load the txMeta from the cache...
		missed, err := u.processTxMetaUsingCache(spanCtx, txHashes, txMetaSlice, failFast)
		if err != nil {
			if errors.Is(err, errors.ErrThresholdExceeded) {
				u.logger.Warnf("[validateSubtreeInternal][%s] [attempt #%d] too many missing txmeta entries in cache (fail fast check only, will retry)", v.SubtreeHash.String(), attempt)
				time.Sleep(retrySleepDuration)
				continue
			}
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from cache", v.SubtreeHash.String(), attempt), err)
		}

		if failFast && abandonTxThreshold > 0 && missed > abandonTxThreshold {
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] abandoned - too many missing txmeta entries", v.SubtreeHash.String(), attempt), err)
		}

		if missed > 0 {
			batched := gocore.Config().GetBool("blockvalidation_batchMissingTransactions", true)

			// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
			missed, err = u.processTxMetaUsingStore(spanCtx, txHashes, txMetaSlice, batched, failFast)
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from store", v.SubtreeHash.String(), attempt), err)
			}
		}

		if missed > 0 {
			// 3. ...then attempt to load the txMeta from the network
			start, stat5, ctx5 := util.StartStatFromContext(spanCtx, "5. processMissingTransactions")
			// missingTxHashes is a slice if all txHashes in the subtree, but only the missing ones are not nil
			// this is done to make sure the order is preserved when getting them in parallel
			// compact the missingTxHashes to only a list of the missing ones
			missingTxHashesCompacted := make([]txmeta.MissingTxHash, 0, missed)
			for idx, txHash := range txHashes {
				if txMetaSlice[idx] == nil && !txHash.IsEqual(model.CoinbasePlaceholderHash) {
					missingTxHashesCompacted = append(missingTxHashesCompacted, txmeta.MissingTxHash{
						Hash: txHash,
						Idx:  idx,
					})
				}
			}

			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] processing %d missing tx for subtree instance", v.SubtreeHash.String(), attempt, len(missingTxHashesCompacted))

			err = u.processMissingTransactions(ctx5, &v.SubtreeHash, missingTxHashesCompacted, v.BaseUrl, txMetaSlice, blockHeight)
			if err != nil {
				return err
			}
			stat5.AddTime(start)
		}

		break
	}

	start = gocore.CurrentTime()
	var txMeta *txmeta.Data
	u.logger.Infof("[validateSubtreeInternal][%s] adding %d nodes to subtree instance", v.SubtreeHash.String(), len(txHashes))
	for idx, txHash := range txHashes {
		// if placeholder just add it and continue
		if idx == 0 && txHash.Equal(*model.CoinbasePlaceholderHash) {
			err = subtree.AddNode(txHash, 0, 0)
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to add coinbase placeholder node to subtree", v.SubtreeHash.String()), err)
			}
			continue
		}

		// finally add the transaction hash and fee to the subtree
		txMeta = txMetaSlice[idx]
		if txMeta == nil {
			return fmt.Errorf("[validateSubtreeInternal][%s] tx meta not found in txMetaSlice at index %d: %s", v.SubtreeHash.String(), idx, txHash.String())
		}

		err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to add node to subtree / subtreeMeta", v.SubtreeHash.String()), err)
		}

		// add the txMeta data we need for block validation
		subtreeIdx := subtree.Length() - 1
		err = subtreeMeta.SetParentTxHashes(subtreeIdx, txMeta.ParentTxHashes)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to set parent tx hash in subtreeMeta", v.SubtreeHash.String()), err)
		}
	}
	stat.NewStat("6. addAllTxHashFeeSizesToSubtree").AddTime(start)

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(&v.SubtreeHash) {
		return fmt.Errorf("[validateSubtreeInternal][%s] subtree root hash does not match [%s]", v.SubtreeHash.String(), merkleRoot.String())
	}

	//
	// store subtree meta in store
	//
	u.logger.Infof("[validateSubtreeInternal][%s] serialize subtree meta", v.SubtreeHash.String())
	completeSubtreeMetaBytes, err := subtreeMeta.Serialize()
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to serialize subtree meta", v.SubtreeHash.String()), err)
	}

	start = gocore.CurrentTime()
	u.logger.Infof("[validateSubtreeInternal][%s] store subtree meta", v.SubtreeHash.String())
	err = u.subtreeStore.Set(spanCtx, merkleRoot[:], completeSubtreeMetaBytes, options.WithTTL(u.subtreeTTL), options.WithFileExtension("meta"))
	stat.NewStat("7. storeSubtreeMeta").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to store subtree meta", v.SubtreeHash.String()), err)
	}

	//
	// store subtree in store
	//
	u.logger.Infof("[validateSubtreeInternal][%s] serialize subtree", v.SubtreeHash.String())
	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to serialize subtree", v.SubtreeHash.String()), err)
	}

	start = gocore.CurrentTime()
	u.logger.Infof("[validateSubtreeInternal][%s] store subtree", v.SubtreeHash.String())
	err = u.subtreeStore.Set(spanCtx, merkleRoot[:], completeSubtreeBytes, options.WithTTL(u.subtreeTTL))
	stat.NewStat("8. storeSubtree").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to store subtree", v.SubtreeHash.String()), err)
	}

	_ = u.SetSubtreeExists(&v.SubtreeHash)

	// only set this on no errors
	prometheusSubtreeValidationValidateSubtreeDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)

	return nil
}

func (u *Server) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseUrl string) ([]chainhash.Hash, error) {
	if baseUrl == "" {
		return nil, fmt.Errorf("[getSubtreeTxHashes][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	start := gocore.CurrentTime()
	// do http request to baseUrl + subtreeHash.String()
	u.logger.Infof("[getSubtreeTxHashes][%s] getting subtree from %s", subtreeHash.String(), baseUrl)
	url := fmt.Sprintf("%s/subtree/%s", baseUrl, subtreeHash.String())
	body, err := util.DoHTTPRequestBodyReader(spanCtx, url)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("[getSubtreeTxHashes][%s] failed to do http request", subtreeHash.String()), err)
	}
	defer body.Close()

	stat.NewStat("2. http fetch subtree").AddTime(start)

	start = gocore.CurrentTime()
	txHashes := make([]chainhash.Hash, 0, u.maxMerkleItemsPerSubtree)
	buffer := make([]byte, chainhash.HashSize)
	bufferedReader := bufio.NewReaderSize(body, 1024*1024*4) // 4MB buffer

	u.logger.Debugf("[getSubtreeTxHashes][%s] processing subtree response into tx hashes", subtreeHash.String())
	for {
		n, err := io.ReadFull(bufferedReader, buffer)
		if n > 0 {
			txHashes = append(txHashes, chainhash.Hash(buffer))
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("[getSubtreeTxHashes][%s] unexpected EOF: partial hash read", subtreeHash.String())
			}
			return nil, fmt.Errorf("[getSubtreeTxHashes][%s] error reading stream: %v", subtreeHash.String(), err)
		}
	}

	stat.NewStat("3. createTxHashes").AddTime(start)

	u.logger.Debugf("[getSubtreeTxHashes][%s] done with subtree response", subtreeHash.String())

	return txHashes, nil
}

func (u *Server) processMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash,
	missingTxHashes []txmeta.MissingTxHash, baseUrl string, txMetaSlice []*txmeta.Data, blockHeight uint32) error {

	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:processMissingTransactions")
	defer func() {
		span.Finish()
	}()

	u.logger.Infof("[validateSubtree][%s] fetching %d missing txs", subtreeHash.String(), len(missingTxHashes))
	missingTxs, err := u.getMissingTransactions(spanCtx, missingTxHashes, baseUrl)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get missing transactions", subtreeHash.String()), err)
	}

	u.logger.Infof("[validateSubtree][%s] blessing %d missing txs", subtreeHash.String(), len(missingTxs))

	var txMeta *txmeta.Data
	var mTx missingTx
	var missingCount int
	missed := make([]*chainhash.Hash, 0, len(txMetaSlice))

	for _, mTx = range missingTxs {
		if mTx.tx == nil {
			return fmt.Errorf("[validateSubtree][%s] missing transaction is nil", subtreeHash.String())
		}

		txMeta, err = u.blessMissingTransaction(spanCtx, mTx.tx, blockHeight)
		if err != nil {
			return fmt.Errorf("[validateSubtree][%s] failed to bless missing transaction: %s: %w", subtreeHash.String(), mTx.tx.TxIDChainHash().String(), err)
		}

		if txMeta == nil {
			missingCount++
			missed = append(missed, mTx.tx.TxIDChainHash())
			u.logger.Infof("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), mTx.tx.TxIDChainHash().String())
		} else {
			u.logger.Debugf("[validateSubtree][%s] adding missing tx to txMetaSlice: %s", subtreeHash.String(), mTx.tx.TxIDChainHash().String())
			txMetaSlice[mTx.idx] = txMeta
		}
	}

	if missingCount > 0 {
		u.logger.Errorf("[validateSubtree][%s] %d missing entries in txMetaSlice (%d requested)", subtreeHash.String(), missingCount, len(txMetaSlice))
		for _, m := range missed {
			u.logger.Debugf("\t txid: %s", m)
		}
	}

	return nil
}

func (u *Server) getMissingTransactions(ctx context.Context, missingTxHashes []txmeta.MissingTxHash, baseUrl string) (missingTxs []missingTx, err error) {
	// transactions have to be returned in the same order as they were requested
	missingTxsMap := make(map[chainhash.Hash]*bt.Tx, len(missingTxHashes))
	missingTxsMu := sync.Mutex{}

	getMissingTransactionsConcurrency, _ := gocore.Config().GetInt("blockvalidation_getMissingTransactions", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(getMissingTransactionsConcurrency) // keep 32 cores free for other tasks

	// get the transactions in batches of 500
	batchSize, _ := gocore.Config().GetInt("blockvalidation_missingTransactionsBatchSize", 100_000)

	for i := 0; i < len(missingTxHashes); i += batchSize {
		missingTxHashesBatch := missingTxHashes[i:util.Min(i+batchSize, len(missingTxHashes))]

		g.Go(func() error {
			missingTxsBatch, err := u.getMissingTransactionsBatch(gCtx, missingTxHashesBatch, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("[getMissingTransactions] failed to get missing transactions batch"), err)
			}

			missingTxsMu.Lock()
			for _, tx := range missingTxsBatch {
				if tx == nil {
					missingTxsMu.Unlock()
					return fmt.Errorf("[getMissingTransactions] #1 missing transaction is nil")
				}
				missingTxsMap[*tx.TxIDChainHash()] = tx
			}
			missingTxsMu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.Join(fmt.Errorf("[blessMissingTransaction] failed to get all transactions"), err)
	}

	// populate the missingTx slice with the tx data
	missingTxs = make([]missingTx, 0, len(missingTxHashes))
	for _, mTx := range missingTxHashes {
		tx, ok := missingTxsMap[mTx.Hash]
		if !ok {
			return nil, fmt.Errorf("[blessMissingTransaction] missing transaction [%s]", mTx.Hash.String())
		}
		if tx == nil {
			return nil, fmt.Errorf("[blessMissingTransaction] #3 missing transaction is nil [%s]", mTx.Hash.String())
		}
		missingTxs = append(missingTxs, missingTx{tx: tx, idx: mTx.Idx})
	}

	return missingTxs, nil
}
