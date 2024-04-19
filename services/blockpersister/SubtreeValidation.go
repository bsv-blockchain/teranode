package blockpersister

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	utxo_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
)

func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
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

func (u *Server) processSubtree(ctx context.Context, subtreeHash chainhash.Hash, w io.Writer, utxoDiff *utxo_model.UTXODiff) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "validateSubtreeBlobInternal")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateSubtree")
	span.LogKV("subtree", subtreeHash.String())
	defer func() {
		span.Finish()
		stat.AddTime(startTotal)
		prometheusBlockPersisterValidateSubtree.Inc()
	}()

	u.logger.Infof("[validateSubtreeInternal][%s] called", subtreeHash.String())

	// 1. get the subtree from the subtree store
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return fmt.Errorf("[BlockPersister] error getting subtree %s from store: %w", subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error deserializing subtree: %w", err)
	}

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	// 2. create a slice of MissingTxHashes for all the txs in the subtree
	txHashes := make([]chainhash.Hash, len(subtree.Nodes))

	for i := 0; i < len(subtree.Nodes); i++ {
		txHashes[i] = subtree.Nodes[i].Hash
	}

	abandonTxThreshold, _ := gocore.Config().GetInt("blockvalidation_subtree_validation_abandon_threshold", 10000)
	maxRetries, _ := gocore.Config().GetInt("blockvalidation_validation_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("blockvalidation_validation_retry_sleep", 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("invalid value for blockvalidation_fail_fast_validation_retry_sleep: %v", err))
	}

	// txMetaSlice will be populated with the txMeta data for each txHash
	// in the retry attempts, only the tx hashes that are missing will be retried, not the whole subtree
	txMetaSlice := make([]*txmeta.Data, len(txHashes))

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		prometheusBlockPersisterValidateSubtreeRetry.Inc()

		if attempt > maxRetries {
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] final attempt to process subtree, this time with full checks enabled", subtreeHash.String(), attempt)
		} else {
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] (fail fast=false) process %d txs from subtree", subtreeHash.String(), attempt, len(txHashes))
		}

		// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

		// 1. First attempt to load the txMeta from the cache...
		missed, err := u.processTxMetaUsingCache(spanCtx, txHashes, txMetaSlice, false)
		if err != nil {
			if errors.Is(err, errors.ErrThresholdExceeded) {
				u.logger.Warnf("[validateSubtreeInternal][%s] [attempt #%d] too many missing txmeta entries in cache (fail fast check only, will retry)", subtreeHash.String(), attempt)
				time.Sleep(retrySleepDuration)
				continue
			}
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from cache", subtreeHash.String(), attempt), err)
		}

		if abandonTxThreshold > 0 && missed > abandonTxThreshold {
			return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] abandoned - too many missing txmeta entries", subtreeHash.String(), attempt), err)
		}

		if missed > 0 {
			batched := gocore.Config().GetBool("blockvalidation_batchMissingTransactions", true)

			// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
			missed, err = u.processTxMetaUsingStore(spanCtx, txHashes, txMetaSlice, batched, false)
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from store", subtreeHash.String(), attempt), err)
			}
		}

		if missed > 0 {
			return fmt.Errorf("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from store", subtreeHash.String(), attempt)
		}

		break
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if model.CoinbasePlaceholderHash.Equal(txHashes[i]) {
			if i != 0 {
				return fmt.Errorf("[BlockPersister] coinbase tx is not first in subtree (%d)", i)
			}
			// The coinbase tx is not in the txmeta store and has been added to the block already
			continue
		}

		if w != nil {
			if _, err := w.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
				return fmt.Errorf("[BlockPersister] error writing tx to file: %w", err)
			}
		}

		if utxoDiff != nil {
			// Process the utxo diff...
			utxoDiff.ProcessTx(txMetaSlice[i].Tx)
		}
	}

	return nil
}
