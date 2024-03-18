package blockpersister

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var stats = gocore.NewStat("blockpersister")

type blockPersister struct {
	l     ulogger.Logger
	r     reader
	d     decorator
	store blob.Store
}

func newBlockPersister(l ulogger.Logger, storeUrl *url.URL, r reader, d decorator) *blockPersister {

	bp := &blockPersister{l: l, r: r, d: d}

	// Create a new block persister
	if storeUrl != nil {
		store, err := blob.NewStore(l, storeUrl)
		if err != nil {
			l.Fatalf("Error creating blob store: %v", err)
		}

		bp.store = store
	}

	return bp
}

func (bp *blockPersister) blockFinalHandler(ctx context.Context, _ []byte, blockBytes []byte) error {
	startTime, stat, ctx := util.NewStatFromContext(ctx, "blockFinalHandler", stats)
	defer func() {
		stat.AddTime(startTime)
		prometheusBlockPersisterBlocks.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error creating block from bytes: %w", err)
	}

	bp.l.Infof("[BlockPersister] Processing block %s (%d subtrees)...", block.Header.Hash().String(), len(block.Subtrees))

	dir, _ := gocore.Config().Get("blockPersister_workingDir", os.TempDir())

	// Open file
	filename := path.Join(dir, block.Header.Hash().String()+".dat")
	tmpFilename := filename + ".tmp"

	f, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error creating file: %w", err)
	}

	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpFilename)
	}()

	w := bufio.NewWriter(f)

	// Write 80 byte block header
	n, err := w.Write(block.Header.Bytes())
	if err != nil {
		return fmt.Errorf("[BlockPersister] error writing block header: %w", err)
	}
	if n != 80 {
		return fmt.Errorf("[BlockPersister] error writing block header: wrote %d bytes, expected 80", n)
	}

	// Write varint of number of tx's
	if err := wire.WriteVarInt(w, 0, block.TransactionCount); err != nil {
		return fmt.Errorf("[BlockPersister] error writing transaction count: %w", err)
	}

	if _, err := w.Write(block.CoinbaseTx.Bytes()); err != nil {
		return fmt.Errorf("[BlockPersister] error writing coinbase tx: %w", err)
	}

	for i, subtreeHash := range block.Subtrees {
		bp.l.Infof("[BlockPersister] Processing subtree %s (%d / %d)", subtreeHash.String(), i+1, len(block.Subtrees))

		if err := bp.processSubtree(ctx, *subtreeHash, w); err != nil {
			return fmt.Errorf("[BlockPersister] error processing subtree %d [%s]: %w", i, subtreeHash.String(), err)
		}
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("[BlockPersister] error flushing writer: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("[BlockPersister] error closing file: %w", err)
	}

	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("[BlockPersister] error renaming file: %w", err)
	}

	bp.l.Infof("[BlockPersister] Wrote block %s to file %s", block.Header.Hash().String(), filename)

	// Write the file to a store (e.g. S3)
	if bp.store != nil {
		r, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("[BlockPersister] error opening file %s: %w", filename, err)
		}
		defer r.Close()

		// Write the file to the store
		hash := block.Header.Hash()
		key := utils.ReverseAndHexEncodeHash(*hash)

		if err := bp.store.SetFromReader(ctx, []byte(key), r, options.WithFileName(hash.String()), options.WithSubDirectory("blocks")); err != nil {
			return fmt.Errorf("[BlockPersister] error writing file to store: %w", err)
		}

		bp.l.Infof("[BlockPersister] Wrote block %s to store", block.Header.Hash().String())

		// Remove the file
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("[BlockPersister] error removing file %s: %w", filename, err)
		}

		bp.l.Infof("[BlockPersister] Removed file %s", filename)
	}

	bp.l.Infof("[BlockPersister] Finished processing block %s", block.Header.Hash().String())

	return nil
}

func (bp *blockPersister) processSubtree(ctx context.Context, subtreeHash chainhash.Hash, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var txCount int

	startTime, groupStat, ctx := util.NewStatFromContext(ctx, "processSubtree", stats)
	defer func() {
		if txCount > 1_000_000 {
			groupStat.NewStat("> 1,000,000 txs").AddTime(startTime)
		} else if txCount > 100_000 {
			groupStat.NewStat("> 100,000 txs").AddTime(startTime)
		} else if txCount > 10_000 {
			groupStat.NewStat("> 10,000 txs").AddTime(startTime)
		} else if txCount > 1_000 {
			groupStat.NewStat("> 1,000 txs").AddTime(startTime)
		} else if txCount > 100 {
			groupStat.NewStat("> 100 txs").AddTime(startTime)
		} else if txCount > 10 {
			groupStat.NewStat("> 10 txs").AddTime(startTime)
		} else if txCount > 0 {
			groupStat.NewStat("> 0 txs").AddTime(startTime)
		} else {
			groupStat.NewStat("0 txs").AddTime(startTime)
		}

		prometheusBlockPersisterSubtrees.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	// 1. get the subtree from the subtree store
	subtreeReader, err := bp.r.GetIoReader(ctx, subtreeHash.CloneBytes())
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

	// 2. create a slice of MissingTxHashes for all the txs in the subtree
	txHashes := make([]*txmeta.MissingTxHash, len(subtree.Nodes))

	for i := 0; i < len(subtree.Nodes); i++ {
		txHashes[i] = &txmeta.MissingTxHash{
			Hash: &subtree.Nodes[i].Hash,
			Idx:  i,
		}
	}

	// 3. get the txs in parallel from the txmeta store in batches of 1024
	batchSize := 1024

	g, ctx := errgroup.WithContext(ctx)
	limit, _ := gocore.Config().GetInt("blockPersister_groupLimit", 4)
	g.SetLimit(limit)

	for i := 0; i < len(txHashes); i += batchSize {
		i := i                  // capture the value of i
		var startTime time.Time // declare here so it can be reused saving garbage collection

		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Proceed with the operation if not cancelled.
				startTime = gocore.CurrentTime()
				defer func() {
					stats.NewStat("MetaBatchDecorate").AddTime(startTime)
					prometheusBlockPersisterSubtreeBatch.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
				}()

				end := util.Min(i+batchSize, len(txHashes))
				bp.l.Debugf("[BlockPersister] Getting txmetas from store for subtree %s [%d:%d]", subtreeHash.String(), i, end)

				if err := bp.d.MetaBatchDecorate(ctx, txHashes[i:end], "tx"); err != nil {
					return fmt.Errorf("[BlockPersister] error getting txmetas from store: %w", err)
				}

				return nil
			}
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[BlockPersister] error getting txmetas from store: %w", err)
	}

	for i, data := range txHashes {
		if data.Data == nil {
			if model.CoinbasePlaceholderHash.IsEqual(data.Hash) {
				if i != 0 {
					return errors.New("[BlockPersister] coinbase tx is not first in subtree")
				}
				// The coinbase tx is not in the txmeta store and has been added to the block already
				continue
			}
			return fmt.Errorf("[BlockPersister] error getting tx meta from store: %s", data.Hash.String())
		}

		if _, err := w.Write(data.Data.Tx.Bytes()); err != nil {
			return fmt.Errorf("[BlockPersister] error writing tx to file: %w", err)
		}
	}

	txCount = len(txHashes)

	return nil
}
