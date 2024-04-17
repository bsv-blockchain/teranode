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
	utxo_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type blockPersisterUpload struct {
	blockHeader *model.BlockHeader
	filename    string
}

type blockPersister struct {
	logger       ulogger.Logger
	subtreeStore reader
	txStore      decorator
	blockStore   blob.Store
	uploadCh     chan blockPersisterUpload
	stats        *gocore.Stat
}

func newBlockPersister(ctx context.Context, logger ulogger.Logger, storeUrl *url.URL, subtreeStore reader, txStore decorator) *blockPersister {

	bp := &blockPersister{
		logger:       logger,
		subtreeStore: subtreeStore,
		txStore:      txStore,
		uploadCh:     make(chan blockPersisterUpload, 100),
		stats:        gocore.NewStat("blockpersister"),
	}

	// Create a new block persister
	if storeUrl != nil {
		store, err := blob.NewStore(logger, storeUrl)
		if err != nil {
			logger.Fatalf("Error creating blob store: %v", err)
		}

		bp.blockStore = store
	}

	// clean old files from working dir
	dir, ok := gocore.Config().Get("blockPersister_workingDir")
	if ok {
		logger.Infof("[BlockPersister] Cleaning old files from working dir: %s", dir)
		files, err := os.ReadDir(dir)
		if err != nil {
			logger.Fatalf("error reading working dir: %v", err)
		}
		for _, file := range files {
			fileInfo, err := file.Info()
			if err != nil {
				logger.Errorf("error reading file info: %v", err)
			}
			if time.Since(fileInfo.ModTime()) > 30*time.Minute {
				logger.Infof("removing old file: %s", file.Name())
				if err = os.Remove(path.Join(dir, file.Name())); err != nil {
					logger.Errorf("error removing old file: %v", err)
				}
			}
		}
	}

	// Start the upload worker
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case upload := <-bp.uploadCh:
				r, err := os.Open(upload.filename)
				if err != nil {
					bp.logger.Errorf("[BlockPersister] error opening file %s: %v", upload.filename, err)
				}
				defer func() {
					_ = r.Close()
				}()

				// create buffered reader for uploading
				bufferedReader := io.NopCloser(bufio.NewReaderSize(r, 1024*1024*16)) // 16MB buffer

				hash := upload.blockHeader.Hash()

				// Write the file to the store
				if err = bp.blockStore.SetFromReader(ctx,
					hash.CloneBytes(),
					bufferedReader,
					options.WithSubDirectory("blocks"),
				); err != nil {
					bp.logger.Errorf("[BlockPersister] error writing file to store, retrying: %v", err)
					bp.uploadCh <- upload
					continue
				}

				bp.logger.Infof("[BlockPersister] Wrote block %s to store", upload.blockHeader.Hash().String())

				// Remove the file
				if err = os.Remove(upload.filename); err != nil {
					bp.logger.Errorf("[BlockPersister] error removing file %s: %v", upload.filename, err)
				}

				bp.logger.Infof("[BlockPersister] Removed file %s", upload.filename)
			}
		}
	}()

	return bp
}

func (bp *blockPersister) blockFinalHandler(ctx context.Context, _ []byte, blockBytes []byte) error {
	startTime, stat, ctx := util.NewStatFromContext(ctx, "blockFinalHandler", bp.stats)
	defer func() {
		stat.AddTime(startTime)
		prometheusBlockPersisterBlocks.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error creating block from bytes: %w", err)
	}

	bp.logger.Infof("[BlockPersister] Processing block %s (%d subtrees)...", block.Header.Hash().String(), len(block.Subtrees))

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
	if err = wire.WriteVarInt(w, 0, block.TransactionCount); err != nil {
		return fmt.Errorf("[BlockPersister] error writing transaction count: %w", err)
	}

	if _, err = w.Write(block.CoinbaseTx.Bytes()); err != nil {
		return fmt.Errorf("[BlockPersister] error writing coinbase tx: %w", err)
	}

	// Create a new UTXO diff
	utxoDiff := utxo_model.NewUTXODiff(block.Header.Hash(), block.Height)

	// Add coinbase utxos to the utxo diff
	utxoDiff.ProcessTx(block.CoinbaseTx)

	for i, subtreeHash := range block.Subtrees {
		bp.logger.Infof("[BlockPersister] Processing subtree %s (%d / %d)", subtreeHash.String(), i+1, len(block.Subtrees))

		if err = bp.processSubtree(ctx, *subtreeHash, w, utxoDiff); err != nil {
			return fmt.Errorf("[BlockPersister] error processing subtree %d [%s]: %w", i, subtreeHash.String(), err)
		}
	}

	if err = w.Flush(); err != nil {
		return fmt.Errorf("[BlockPersister] error flushing writer: %w", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("[BlockPersister] error closing file: %w", err)
	}

	if err = os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("[BlockPersister] error renaming file: %w", err)
	}

	bp.logger.Infof("[BlockPersister] Wrote block %s to file %s", block.Header.Hash().String(), filename)

	// Write the file to a store (e.g. S3)
	if bp.blockStore != nil {
		bp.uploadCh <- blockPersisterUpload{
			blockHeader: block.Header,
			filename:    filename,
		}
	}

	bp.logger.Infof("[BlockPersister] Finished processing block %s", block.Header.Hash().String())

	folder, _ := gocore.Config().Get("utxoPersister_workingDir", os.TempDir())

	if err := utxoDiff.Persist(folder); err != nil {
		return fmt.Errorf("[BlockPersister] error persisting utxo diff: %w", err)
	}

	// fmt.Println("UTXODiff block hash:", utxoDiff.BlockHash)
	// fmt.Println("UTXODiff block height:", utxoDiff.BlockHeight)

	// fmt.Println("UTXODiff removed UTXOs:")
	// utxoDiff.Removed.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
	// 	fmt.Printf("%v %v\n", &uk, uv)
	// 	return
	// })

	// fmt.Println("UTXODiff added UTXOs:")
	// var i int
	// utxoDiff.Added.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
	// 	fmt.Printf("%d: %v %v\n", i, &uk, uv)
	// 	i++
	// 	return
	// })

	// f, err = os.Open(fmt.Sprintf("data/blockpersister/%s_%d.utxodiff", utxoDiff.BlockHash.String(), utxoDiff.BlockHeight))
	// if err != nil {
	// 	return fmt.Errorf("error opening file: %w", err)
	// }
	// defer f.Close()

	// r := bufio.NewReader(f)

	// utxodiff, err := utxo_model.NewUTXODiffFromReader(r)
	// if err != nil {
	// 	return fmt.Errorf("error reading utxodiff: %w", err)
	// }

	// fmt.Println("UTXODiff block hash:", utxodiff.BlockHash)
	// fmt.Println("UTXODiff block height:", utxodiff.BlockHeight)

	// fmt.Println("UTXODiff removed UTXOs:")
	// utxodiff.Removed.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
	// 	fmt.Printf("%v %v\n", &uk, uv)
	// 	return
	// })

	// fmt.Println("UTXODiff added UTXOs:")
	// var j int
	// utxodiff.Added.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
	// 	fmt.Printf("%d: %v %v\n", j, &uk, uv)
	// 	j++
	// 	return
	// })

	// TODO: Persist the UTXO set with the current block hash
	// Load the UTXO set for block.header.previousHash
	// Apply all the diffs to the UTXO set
	// Persist the UTXO set with the current block hash

	return nil
}

func (bp *blockPersister) processSubtree(ctx context.Context, subtreeHash chainhash.Hash, w io.Writer, utxoDiff *utxo_model.UTXODiff) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var txCount int

	startTime, stat, ctx := util.NewStatFromContext(ctx, "processSubtree", bp.stats, false)
	stat.AddRanges(0, 10, 100, 1_000, 10_000, 100_000, 1_000_000)

	defer func() {
		stat.AddTimeForRange(startTime, txCount)

		prometheusBlockPersisterSubtrees.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	// 1. get the subtree from the subtree store
	subtreeReader, err := bp.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
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
			Hash: subtree.Nodes[i].Hash,
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
					bp.stats.NewStat("MetaBatchDecorate").AddTime(startTime)
					prometheusBlockPersisterSubtreeBatch.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
				}()

				end := util.Min(i+batchSize, len(txHashes))
				bp.logger.Debugf("[BlockPersister] Getting txmetas from store for subtree %s [%d:%d]", subtreeHash.String(), i, end)

				if err := bp.txStore.MetaBatchDecorate(ctx, txHashes[i:end], "tx"); err != nil {
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
		if data.Err != nil {
			return fmt.Errorf("[BlockPersister] error getting tx meta from store: %w", data.Err)
		}

		if data.Data == nil {
			if model.CoinbasePlaceholderHash.Equal(data.Hash) {
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

		// Process the utxo diff...
		utxoDiff.ProcessTx(data.Data.Tx)
	}

	txCount = len(txHashes)

	return nil
}
