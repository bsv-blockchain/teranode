package blockpersister

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	// Open file
	filename := path.Join(os.TempDir(), block.Header.Hash().String()+".dat")
	tmpFilename := filename + ".tmp"

	f, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error creating file: %w", err)
	}

	defer f.Close()

	// Write 80 byte block header
	n, err := f.Write(block.Header.Bytes())
	if err != nil {
		return fmt.Errorf("[BlockPersister] error writing block header: %w", err)
	}
	if n != 80 {
		return fmt.Errorf("[BlockPersister] error writing block header: wrote %d bytes, expected 80", n)
	}

	// Write varint of number of tx's
	if err := wire.WriteVarInt(f, 0, block.TransactionCount); err != nil {
		return fmt.Errorf("[BlockPersister] error writing transaction count: %w", err)
	}

	if _, err := f.Write(block.CoinbaseTx.Bytes()); err != nil {
		return fmt.Errorf("[BlockPersister] error writing coinbase tx: %w", err)
	}

	for i, subtreeHash := range block.Subtrees {
		buf, err := bp.processSubtree(ctx, *subtreeHash)
		if err != nil {
			return fmt.Errorf("[BlockPersister] error processing subtree %d [%s]: %w", i, subtreeHash.String(), err)
		}

		_, err = f.Write(buf.Bytes())
		if err != nil {
			return fmt.Errorf("[BlockPersister] error writing subtree %d [%s]: %w", i, subtreeHash.String(), err)
		}
	}

	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("[BlockPersister] error renaming file: %w", err)
	}

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
	}

	bp.l.Warnf("[BlockPersister] Wrote block %s to file %s", block.Header.Hash().String(), filename)

	return nil
}

func (bp *blockPersister) processSubtree(ctx context.Context, subtreeHash chainhash.Hash) (*bytes.Buffer, error) {
	startTime, stat, ctx := util.NewStatFromContext(ctx, "processSubtree", stats)
	defer func() {
		stat.AddTime(startTime)
		prometheusBlockPersisterSubtrees.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	// get the subtree from the subtree store
	subtreeReader, err := bp.r.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return nil, fmt.Errorf("[BlockPersister] error getting subtree %s from store: %w", subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return nil, fmt.Errorf("[BlockPersister] error deserializing subtree: %w", err)
	}

	// Create a new byte buffer to write the txs to
	var buf bytes.Buffer

	batchSize := 1024

	for i := 0; i < len(subtree.Nodes); i += batchSize {
		end := util.Min(i+batchSize, len(subtree.Nodes))

		missingTxHashesCompacted := make([]*txmeta.MissingTxHash, 0, end-i)

		for j := 0; j < util.Min(batchSize, len(subtree.Nodes)-i); j++ {
			missingTxHashesCompacted = append(missingTxHashesCompacted, &txmeta.MissingTxHash{
				Hash: &subtree.Nodes[i+j].Hash,
				Idx:  i + j,
			})
		}

		if err := bp.d.MetaBatchDecorate(ctx, missingTxHashesCompacted, "tx"); err != nil {
			return nil, fmt.Errorf("[BlockPersister] error getting tx metas from store: %w", err)
		}

		for k, data := range missingTxHashesCompacted {
			if data.Data == nil {
				if data.Hash.IsEqual(model.CoinbasePlaceholderHash) {
					if i+k != 0 {
						return nil, errors.New("[BlockPersister] coinbase tx is not first in subtree")
					}
					// The coinbase tx is not in the txmeta store and has been added to the block already
					continue
				}
				return nil, fmt.Errorf("[BlockPersister] error getting tx meta from store: %s", data.Hash.String())
			}

			if _, err := buf.Write(data.Data.Tx.Bytes()); err != nil {
				return nil, fmt.Errorf("[BlockPersister] error writing tx to file: %w", err)
			}
		}
	}

	return &buf, nil
}
