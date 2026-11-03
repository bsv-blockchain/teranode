package subtreeprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// keepReorgDeletedTxs saves the txs that removeCoinbaseUtxos is about to delete, so that moving the
// block forward again can recreate them. They are saved under the hash of the block whose coinbase
// they spend, parents before children, each with the previous outputs it spends.
//
// A file left by an earlier attempt is merged in, not replaced: a retry after a partial delete finds
// some of the txs already gone and must not lose them. A tx still in the store is saved with its
// current state, whatever the earlier file held.
//
// It returns the hashes of every tx it saved, parents first, including those carried over from an
// earlier file. The file is the journal of the delete: the caller deletes what the file lists.
func (stp *SubtreeProcessor) keepReorgDeletedTxs(ctx context.Context, block *model.Block, txHashes []chainhash.Hash) ([]chainhash.Hash, error) {
	if stp.subtreeStore == nil {
		if len(txHashes) == 0 {
			return nil, nil
		}

		return nil, errors.NewProcessingError("[keepReorgDeletedTxs][%s] no subtree store to save %d txs in", block.String(), len(txHashes))
	}

	blockHash := block.Header.Hash()

	previous, err := stp.readReorgDeletedTxs(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	savedByHash := make(map[chainhash.Hash]*reorgDeletedTx, len(previous))
	for _, saved := range previous {
		savedByHash[*saved.tx.TxIDChainHash()] = saved
	}

	entries := make([]*reorgDeletedTx, 0, len(previous)+len(txHashes))
	seen := make(map[chainhash.Hash]struct{}, len(previous)+len(txHashes))

	for i := range txHashes {
		if _, ok := seen[txHashes[i]]; ok {
			continue
		}

		txMeta, getErr := stp.utxoStore.Get(ctx, &txHashes[i], fields.Tx, fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs, fields.UnminedSince)
		if getErr != nil {
			if errors.Is(getErr, errors.ErrTxNotFound) {
				if saved, ok := savedByHash[txHashes[i]]; ok {
					seen[txHashes[i]] = struct{}{}
					entries = append(entries, saved)
				}

				continue
			}

			return nil, errors.NewStorageError("[keepReorgDeletedTxs][%s] error getting tx %s", block.String(), txHashes[i].String(), getErr)
		}

		if txMeta.Tx == nil {
			return nil, errors.NewProcessingError("[keepReorgDeletedTxs][%s] store returned no tx for %s", block.String(), txHashes[i].String())
		}

		if err = stp.utxoStore.PreviousOutputsDecorate(ctx, txMeta.Tx); err != nil {
			return nil, errors.NewStorageError("[keepReorgDeletedTxs][%s] error decorating previous outputs of %s", block.String(), txHashes[i].String(), err)
		}

		saved, buildErr := newReorgDeletedTx(txMeta)
		if buildErr != nil {
			return nil, errors.NewProcessingError("[keepReorgDeletedTxs][%s] error reading the mined state of %s", block.String(), txHashes[i].String(), buildErr)
		}

		seen[txHashes[i]] = struct{}{}
		entries = append(entries, saved)
	}

	for _, saved := range previous {
		if _, ok := seen[*saved.tx.TxIDChainHash()]; !ok {
			seen[*saved.tx.TxIDChainHash()] = struct{}{}
			entries = append(entries, saved)
		}
	}

	if len(entries) == 0 {
		return nil, nil
	}

	ordered := parentsFirst(entries)

	dah := stp.utxoStore.GetBlockHeight() + stp.settings.GetSubtreeValidationBlockHeightRetention()

	if err = stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, encodeReorgDeletedTxs(ordered), options.WithDeleteAt(dah), options.WithAllowOverwrite(true)); err != nil {
		return nil, errors.NewStorageError("[keepReorgDeletedTxs][%s] error saving %d txs", block.String(), len(ordered), err)
	}

	kept := make([]chainhash.Hash, len(ordered))
	for i, saved := range ordered {
		kept[i] = *saved.tx.TxIDChainHash()
	}

	return kept, nil
}

// readReorgDeletedTxs returns the txs saved for the block, or nil when none were saved.
func (stp *SubtreeProcessor) readReorgDeletedTxs(ctx context.Context, blockHash *chainhash.Hash) ([]*reorgDeletedTx, error) {
	if stp.subtreeStore == nil {
		return nil, nil
	}

	b, err := stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return nil, nil
		}

		return nil, errors.NewStorageError("[readReorgDeletedTxs][%s] error reading saved txs", blockHash.String(), err)
	}

	txs, err := decodeReorgDeletedTxs(b)
	if err != nil {
		return nil, errors.NewProcessingError("[readReorgDeletedTxs][%s] saved txs are unreadable", blockHash.String(), err)
	}

	return txs, nil
}

// restoreReorgDeletedTxs recreates the txs that removeCoinbaseUtxos deleted when the block was moved
// back. It runs after the block's coinbase is back in the store, so the txs can spend it again. The
// txs were validated before they were deleted, and the block that holds them is valid, so they are
// written without validation, as a block's txs are. A tx is skipped, and logged, when another tx now
// spends one of its inputs, when one of its parents is missing from the store, or when one of its saved
// parents was skipped. A tx saved with block IDs is on the longest chain only if the current chain holds
// those blocks, whatever its state was when it was deleted; if not, it is marked as not on the longest
// chain so block assembly offers it again. The saved file is deleted
// after a restore.
//
// It returns the txs it recreated that are not on the longest chain, so the caller can put them back
// in block assembly. A tx that was skipped, or that already existed, is not returned.
func (stp *SubtreeProcessor) restoreReorgDeletedTxs(ctx context.Context, block *model.Block) ([]*bt.Tx, error) {
	txs, err := stp.readReorgDeletedTxs(ctx, block.Header.Hash())
	if err != nil {
		return nil, err
	}

	if len(txs) == 0 {
		return nil, nil
	}

	blockHeight := block.Height
	if blockHeight == 0 {
		blockHeight = stp.utxoStore.GetBlockHeight()
	}

	// The txs spend this block's coinbase, so they were mined at least a coinbase maturity later. The
	// coinbase is immature at the height of this block, so spend at the first height it is mature.
	spendHeight := blockHeight + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // maturity is a small chain constant

	// each tx is written with the block info it had when it was deleted and the chain state the
	// current chain gives it, so a tx mined in a block is not left without block IDs and a tx whose
	// block left the chain is not left on the longest chain
	var markNotOnLongestChain []chainhash.Hash

	skipped := make(map[chainhash.Hash]struct{})

	var unmined []*bt.Tx

	for _, saved := range txs {
		txHash := *saved.tx.TxIDChainHash()

		if skippedParent := firstSkippedParent(saved.tx, skipped); skippedParent != nil {
			stp.logger.Errorf("[restoreReorgDeletedTxs][%s] tx %s was not recreated, its parent %s was not recreated", block.String(), txHash.String(), skippedParent.String())

			skipped[txHash] = struct{}{}

			continue
		}

		onLongestChain := false

		var createOpts []utxostore.CreateOption
		if len(saved.mined) > 0 {
			blockIDs := make([]uint32, len(saved.mined))
			for i := range saved.mined {
				blockIDs[i] = saved.mined[i].BlockID
			}

			var chainErr error

			if onLongestChain, chainErr = stp.blockchainClient.CheckBlockIsInCurrentChain(ctx, blockIDs); chainErr != nil {
				return nil, errors.NewProcessingError("[restoreReorgDeletedTxs][%s] error checking whether the blocks of tx %s are on the current chain", block.String(), txHash.String(), chainErr)
			}

			for i := range saved.mined {
				saved.mined[i].OnLongestChain = onLongestChain
			}

			createOpts = append(createOpts, utxostore.WithMinedBlockInfo(saved.mined...))
		}

		_, _, createErr := stp.utxoStore.SpendAndCreate(ctx, saved.tx, spendHeight, createOpts...)
		if createErr != nil && !errors.Is(createErr, errors.ErrTxExists) {
			if errors.Is(createErr, errors.ErrSpent) || errors.Is(createErr, errors.ErrTxNotFound) {
				stp.logger.Errorf("[restoreReorgDeletedTxs][%s] tx %s was not recreated, one of its inputs is spent by another tx or its parent is missing: %v", block.String(), txHash.String(), createErr)

				skipped[txHash] = struct{}{}

				continue
			}

			return nil, errors.NewStorageError("[restoreReorgDeletedTxs][%s] tx %s was not recreated", block.String(), txHash.String(), createErr)
		}

		if len(saved.mined) > 0 && !onLongestChain {
			markNotOnLongestChain = append(markNotOnLongestChain, txHash)
		}

		if createErr == nil && !onLongestChain {
			unmined = append(unmined, saved.tx)
		}
	}

	if len(markNotOnLongestChain) > 0 {
		if err = stp.utxoStore.MarkTransactionsOnLongestChain(ctx, markNotOnLongestChain, false); err != nil {
			return nil, errors.NewStorageError("[restoreReorgDeletedTxs][%s] error marking %d recreated txs as not on the longest chain", block.String(), len(markNotOnLongestChain), err)
		}
	}

	blockHash := block.Header.Hash()

	if delErr := stp.subtreeStore.Del(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs); delErr != nil && !errors.Is(delErr, errors.ErrNotFound) {
		return nil, errors.NewStorageError("[restoreReorgDeletedTxs][%s] error deleting the saved txs", block.String(), delErr)
	}

	stp.logger.Warnf("[restoreReorgDeletedTxs][%s] recreated %d txs deleted when the block was moved back and skipped %d", block.String(), len(txs)-len(skipped), len(skipped))

	return unmined, nil
}

func firstSkippedParent(tx *bt.Tx, skipped map[chainhash.Hash]struct{}) *chainhash.Hash {
	for _, input := range tx.Inputs {
		parent := input.PreviousTxIDChainHash()
		if _, ok := skipped[*parent]; ok {
			return parent
		}
	}

	return nil
}

// parentsFirst returns the entries ordered so that no tx comes before a parent that is also in the set.
func parentsFirst(entries []*reorgDeletedTx) []*reorgDeletedTx {
	byHash := make(map[chainhash.Hash]*reorgDeletedTx, len(entries))
	for _, e := range entries {
		byHash[*e.tx.TxIDChainHash()] = e
	}

	ordered := make([]*reorgDeletedTx, 0, len(entries))
	done := make(map[chainhash.Hash]struct{}, len(entries))

	var visit func(e *reorgDeletedTx)

	visit = func(e *reorgDeletedTx) {
		hash := *e.tx.TxIDChainHash()
		if _, ok := done[hash]; ok {
			return
		}

		done[hash] = struct{}{}

		for _, input := range e.tx.Inputs {
			if parent, inSet := byHash[*input.PreviousTxIDChainHash()]; inSet {
				visit(parent)
			}
		}

		ordered = append(ordered, e)
	}

	for _, e := range entries {
		visit(e)
	}

	return ordered
}

// reorgDeletedTx is a tx saved before it was deleted, with the state to give it back: the blocks it
// was mined in and whether it was off the longest chain. mined is empty for a tx that was never mined.
type reorgDeletedTx struct {
	tx      *bt.Tx
	mined   []utxostore.MinedBlockInfo
	unmined bool
}

const minedBlockInfoSize = 12

func newReorgDeletedTx(txMeta *meta.Data) (*reorgDeletedTx, error) {
	if len(txMeta.BlockHeights) != len(txMeta.BlockIDs) || len(txMeta.SubtreeIdxs) != len(txMeta.BlockIDs) {
		return nil, errors.NewProcessingError("record has %d block IDs, %d block heights and %d subtree indexes", len(txMeta.BlockIDs), len(txMeta.BlockHeights), len(txMeta.SubtreeIdxs))
	}

	unmined := txMeta.UnminedSince != 0
	mined := make([]utxostore.MinedBlockInfo, len(txMeta.BlockIDs))

	for i := range txMeta.BlockIDs {
		mined[i] = utxostore.MinedBlockInfo{
			BlockID:        txMeta.BlockIDs[i],
			BlockHeight:    txMeta.BlockHeights[i],
			SubtreeIdx:     txMeta.SubtreeIdxs[i],
			OnLongestChain: !unmined,
		}
	}

	return &reorgDeletedTx{tx: txMeta.Tx, mined: mined, unmined: unmined}, nil
}

// encodeReorgDeletedTxs writes each entry as a flag byte, the count of block infos, the block infos
// (block ID, height, subtree index, each a little-endian uint32) and the tx in extended format.
func encodeReorgDeletedTxs(entries []*reorgDeletedTx) []byte {
	var buf []byte

	for _, e := range entries {
		flags := byte(0)
		if e.unmined {
			flags = 1
		}

		buf = append(buf, flags)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.mined))) //nolint:gosec // a tx is in few blocks

		for _, m := range e.mined {
			buf = binary.LittleEndian.AppendUint32(buf, m.BlockID)
			buf = binary.LittleEndian.AppendUint32(buf, m.BlockHeight)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(m.SubtreeIdx)) //nolint:gosec // subtree index is small and non-negative
		}

		if e.tx.IsExtended() {
			buf = append(buf, e.tx.ExtendedBytes()...)
		} else {
			buf = append(buf, e.tx.Bytes()...)
		}
	}

	return buf
}

func decodeReorgDeletedTxs(b []byte) ([]*reorgDeletedTx, error) {
	reader := bytes.NewReader(b)
	entries := make([]*reorgDeletedTx, 0, 8)

	for reader.Len() > 0 {
		var head [5]byte
		if _, err := io.ReadFull(reader, head[:]); err != nil {
			return nil, errors.NewProcessingError("[decodeReorgDeletedTxs] truncated entry header", err)
		}

		count := binary.LittleEndian.Uint32(head[1:])
		if uint64(count)*minedBlockInfoSize > uint64(reader.Len()) {
			return nil, errors.NewProcessingError("[decodeReorgDeletedTxs] entry claims %d block infos, %d bytes remain", count, reader.Len())
		}

		entry := &reorgDeletedTx{unmined: head[0]&1 == 1, mined: make([]utxostore.MinedBlockInfo, count)}

		for i := range entry.mined {
			var info [minedBlockInfoSize]byte
			if _, err := io.ReadFull(reader, info[:]); err != nil {
				return nil, errors.NewProcessingError("[decodeReorgDeletedTxs] truncated block info", err)
			}

			entry.mined[i] = utxostore.MinedBlockInfo{
				BlockID:        binary.LittleEndian.Uint32(info[0:]),
				BlockHeight:    binary.LittleEndian.Uint32(info[4:]),
				SubtreeIdx:     int(binary.LittleEndian.Uint32(info[8:])),
				OnLongestChain: !entry.unmined,
			}
		}

		entry.tx = &bt.Tx{}
		if _, err := entry.tx.ReadFrom(reader); err != nil {
			return nil, errors.NewProcessingError("[decodeReorgDeletedTxs] failed to read transaction", err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
