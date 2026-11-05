// Package chainoracle reconciles a node's UTXO store and blob store against the
// best chain recorded in its blockchain store.
//
// The blockchain store is taken as the authority. Check walks its best chain,
// replays every block's transactions (coinbase first, then each subtree in
// order) to derive the coin state the chain implies, and diffs that against
// what the UTXO store and the blob store actually hold. It returns a structured
// Report, never just pass/fail, so a failing test prints the exact coins and
// files that disagree.
//
// It is meant to run after crash or fault injection, once a node reports a
// converged tip, to catch drift between the three stores that a tip-hash
// comparison cannot see.
//
// Limits: the UTXO store has no enumeration API, so a record for a transaction
// that is on no chain at all is not discovered unless a best-chain output names
// it as its spender. Checks start at Options.FromHeight; inputs spending outputs
// created below it are counted, not checked.
package chainoracle

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// headerPageSize bounds each GetBlockHeaders call while walking back from the tip.
const headerPageSize = 1000

// ChainReader is the read-only view of the blockchain store the oracle needs.
// blockchain.ClientI satisfies it directly; wrap a blockchain store with StoreChain.
type ChainReader interface {
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
}

// StoreChain adapts a blockchain store to ChainReader.
func StoreChain(s blockchainstore.Store) ChainReader {
	return storeChain{s}
}

type storeChain struct {
	blockchainstore.Store
}

func (s storeChain) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	block, _, err := s.Store.GetBlock(ctx, blockHash)
	return block, err
}

// Stores holds the node's store handles. Chain, UTXO and SubtreeStore are
// required. BlockStore is optional: without it the block files and the
// utxo-additions/utxo-deletions files are not checked.
type Stores struct {
	Chain        ChainReader
	UTXO         utxo.Store
	SubtreeStore blob.Store
	BlockStore   blob.Store
	Settings     *settings.Settings
}

// Options tunes a Check.
type Options struct {
	// FromHeight is the first height to replay. Zero means 1: the genesis
	// coinbase is never in the UTXO store. Set it to the seed height for a node
	// that started from a UTXO snapshot.
	FromHeight uint32
	// SkipUTXODeltas disables the utxo-additions/utxo-deletions checks, for
	// nodes that run with blockpersister_processUTXOFiles=false.
	SkipUTXODeltas bool
}

// outpoint identifies one transaction output.
type outpoint struct {
	txID chainhash.Hash
	vout uint32
}

// expectedOutput is the chain-implied state of one storable output.
type expectedOutput struct {
	utxoHash chainhash.Hash
	spentBy  *chainhash.Hash
	vin      int
}

// chainTx is one best-chain transaction, in replay order.
type chainTx struct {
	tx      *bt.Tx
	txID    chainhash.Hash
	height  uint32
	blockID uint32
	block   chainhash.Hash
	// vouts lists the outputs the UTXO store is expected to hold.
	vouts []uint32
}

// chainBlock is one best-chain block with what the oracle recovered for it.
type chainBlock struct {
	block  *model.Block
	hash   chainhash.Hash
	height uint32
	id     uint32
	// txs is the block's transactions in persisted order (coinbase first), or
	// nil if they could not be recovered.
	txs []*bt.Tx
}

type checker struct {
	stores Stores
	opts   Options
	report *Report

	genesisActivationHeight uint32

	txs     []*chainTx
	txIndex map[chainhash.Hash]*chainTx
	outputs map[outpoint]*expectedOutput
}

// Check reconciles the stores against the best chain and returns every
// discrepancy found. The error is reserved for failures that stop the check
// from running at all (the chain cannot be read, a store returns an error other
// than not-found); a disagreement is never an error, it is a Discrepancy.
func Check(ctx context.Context, stores Stores, opts Options) (*Report, error) {
	if stores.Chain == nil || stores.UTXO == nil || stores.SubtreeStore == nil || stores.Settings == nil {
		return nil, errors.NewInvalidArgumentError("chain oracle needs Chain, UTXO, SubtreeStore and Settings")
	}

	if opts.FromHeight == 0 {
		opts.FromHeight = 1
	}

	c := &checker{
		stores: stores,
		opts:   opts,
		report: &Report{
			FromHeight:        opts.FromHeight,
			BlockFilesChecked: stores.BlockStore != nil,
			UTXODeltasChecked: stores.BlockStore != nil && !opts.SkipUTXODeltas,
			ReplayComplete:    true,
		},
		txIndex: make(map[chainhash.Hash]*chainTx),
		outputs: make(map[outpoint]*expectedOutput),
	}

	if stores.Settings.ChainCfgParams != nil {
		c.genesisActivationHeight = stores.Settings.ChainCfgParams.GenesisActivationHeight
	}

	headers, metas, err := c.bestChain(ctx)
	if err != nil {
		return nil, err
	}

	// replay oldest first so every spend sees the output it consumes
	for i := len(headers) - 1; i >= 0; i-- {
		if err := c.replayBlock(ctx, headers[i], metas[i]); err != nil {
			return nil, err
		}
	}

	if err := c.checkUTXOStore(ctx); err != nil {
		return nil, err
	}

	return c.report, nil
}

// bestChain returns the best chain from the tip back to FromHeight, tip first,
// and records any break in its linkage.
func (c *checker) bestChain(ctx context.Context) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	tip, tipMeta, err := c.stores.Chain.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, nil, errors.NewProcessingError("chain oracle: read best block header", err)
	}

	c.report.Tip = *tip.Hash()
	c.report.TipHeight = tipMeta.Height

	if tipMeta.Height < c.opts.FromHeight {
		return nil, nil, nil
	}

	want := uint64(tipMeta.Height-c.opts.FromHeight) + 1

	var (
		headers = make([]*model.BlockHeader, 0, want)
		metas   = make([]*model.BlockHeaderMeta, 0, want)
		cursor  = tip.Hash()
	)

	for uint64(len(headers)) < want {
		n := min(want-uint64(len(headers)), headerPageSize)

		page, pageMetas, err := c.stores.Chain.GetBlockHeaders(ctx, cursor, n)
		if err != nil {
			return nil, nil, errors.NewProcessingError("chain oracle: read headers from %s", cursor, err)
		}

		if len(page) == 0 || len(page) != len(pageMetas) {
			return nil, nil, errors.NewProcessingError("chain oracle: headers from %s returned %d headers and %d metas", cursor, len(page), len(pageMetas))
		}

		headers = append(headers, page...)
		metas = append(metas, pageMetas...)
		cursor = page[len(page)-1].HashPrevBlock
	}

	for i := range headers {
		expectedHeight := tipMeta.Height - uint32(i) //nolint:gosec // i < want, which fits uint32

		if metas[i].Height != expectedHeight {
			c.report.add(Discrepancy{Kind: KindChainBroken, Height: metas[i].Height, Block: *headers[i].Hash(), Vout: -1,
				Detail: fmt.Sprintf("expected height %d walking back from the tip", expectedHeight)})
		}

		if i+1 < len(headers) && !headers[i].HashPrevBlock.IsEqual(headers[i+1].Hash()) {
			c.report.add(Discrepancy{Kind: KindChainBroken, Height: metas[i].Height, Block: *headers[i].Hash(), Vout: -1,
				Detail: fmt.Sprintf("previous hash %s but the chain's next block back is %s", headers[i].HashPrevBlock, headers[i+1].Hash())})
		}
	}

	return headers, metas, nil
}

// replayBlock recovers one block's transactions, checks its blob files and
// folds its coin changes into the expected state.
func (c *checker) replayBlock(ctx context.Context, header *model.BlockHeader, headerMeta *model.BlockHeaderMeta) error {
	hash := *header.Hash()

	block, err := c.stores.Chain.GetBlock(ctx, &hash)
	if err != nil {
		return errors.NewProcessingError("chain oracle: read block %s", hash, err)
	}

	cb := &chainBlock{block: block, hash: hash, height: headerMeta.Height, id: headerMeta.ID}

	c.report.Blocks++

	if err := c.checkBlockFile(ctx, cb); err != nil {
		return err
	}

	if err := c.recoverTxs(ctx, cb); err != nil {
		return err
	}

	if cb.txs == nil {
		c.report.ReplayComplete = false
		c.report.add(Discrepancy{Kind: KindReplayIncomplete, Height: cb.height, Block: cb.hash, Vout: -1,
			Detail: "the block's transactions could not be recovered from the subtree store or the UTXO store"})

		return nil
	}

	for _, tx := range cb.txs {
		c.applyTx(cb, tx)
	}

	return c.checkUTXODeltas(ctx, cb)
}

// applyTx folds one transaction into the expected coin state.
func (c *checker) applyTx(cb *chainBlock, tx *bt.Tx) {
	txID := *tx.TxIDChainHash()

	c.report.Txs++

	if prev, ok := c.txIndex[txID]; ok {
		c.report.add(Discrepancy{Kind: KindChainDuplicateTx, Height: cb.height, Block: cb.hash, TxID: txID, Vout: -1,
			Detail: fmt.Sprintf("already on the chain in block %s at height %d", prev.block, prev.height)})

		return
	}

	if !tx.IsCoinbase() {
		for vin, input := range tx.Inputs {
			op := outpoint{txID: *input.PreviousTxIDChainHash(), vout: input.PreviousTxOutIndex}

			exp, ok := c.outputs[op]
			if !ok {
				// created below FromHeight, or by an unstorable output; nothing to check
				c.report.ExternalInputs++
				continue
			}

			if exp.spentBy != nil {
				c.report.add(Discrepancy{Kind: KindChainDoubleSpend, Height: cb.height, Block: cb.hash, TxID: op.txID, Vout: int(op.vout),
					Detail: fmt.Sprintf("spent by %s and again by %s", exp.spentBy, txID)})

				continue
			}

			spender := txID
			exp.spentBy = &spender
			exp.vin = vin
		}
	}

	ct := &chainTx{tx: tx, txID: txID, height: cb.height, blockID: cb.id, block: cb.hash}

	for vout, output := range tx.Outputs {
		if output == nil || !utxo.ShouldStoreOutputAsUTXO(output, cb.height, c.genesisActivationHeight) {
			continue
		}

		utxoHash, err := util.UTXOHashFromOutput(&txID, output, uint32(vout)) //nolint:gosec // vout indexes tx.Outputs
		if err != nil {
			continue
		}

		c.outputs[outpoint{txID: txID, vout: uint32(vout)}] = &expectedOutput{utxoHash: *utxoHash} //nolint:gosec // as above
		ct.vouts = append(ct.vouts, uint32(vout))                                                  //nolint:gosec // as above
		c.report.Outputs++
	}

	c.txs = append(c.txs, ct)
	c.txIndex[txID] = ct
}

// checkBlockFile asserts the persisted block file exists and decodes to the
// best-chain block.
func (c *checker) checkBlockFile(ctx context.Context, cb *chainBlock) error {
	if c.stores.BlockStore == nil {
		return nil
	}

	b, err := c.stores.BlockStore.Get(ctx, cb.hash[:], fileformat.FileTypeBlock)
	if err != nil {
		if isBlobNotFound(err) {
			c.blobMissing(cb, chainhash.Hash{}, fileformat.FileTypeBlock)
			return nil
		}

		return errors.NewStorageError("chain oracle: read block file %s", cb.hash, err)
	}

	persisted, err := model.NewBlockFromBytes(b)
	if err != nil {
		c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeBlock, fmt.Sprintf("does not decode: %v", err))
		return nil
	}

	switch {
	case !persisted.Hash().IsEqual(&cb.hash):
		c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeBlock, fmt.Sprintf("hashes to %s", persisted.Hash()))
	case !slices.EqualFunc(persisted.Subtrees, cb.block.Subtrees, func(a, b *chainhash.Hash) bool { return a.IsEqual(b) }):
		c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeBlock, "subtree list differs from the blockchain store")
	case persisted.CoinbaseTx == nil || cb.block.CoinbaseTx == nil ||
		!persisted.CoinbaseTx.TxIDChainHash().IsEqual(cb.block.CoinbaseTx.TxIDChainHash()):
		c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeBlock, "coinbase differs from the blockchain store")
	case persisted.TransactionCount != cb.block.TransactionCount:
		c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeBlock,
			fmt.Sprintf("transaction count %d, blockchain store has %d", persisted.TransactionCount, cb.block.TransactionCount))
	}

	return nil
}

// recoverTxs fills cb.txs with the block's transactions, coinbase first, from
// the subtree files, checking each subtree and subtreeData file on the way. A
// missing subtreeData file falls back to the UTXO store's copy of each tx.
func (c *checker) recoverTxs(ctx context.Context, cb *chainBlock) error {
	if cb.block.CoinbaseTx == nil {
		return nil
	}

	txs := []*bt.Tx{cb.block.CoinbaseTx}

	for _, subtreeHash := range cb.block.Subtrees {
		subtreeTxs, err := c.recoverSubtreeTxs(ctx, cb, *subtreeHash)
		if err != nil {
			return err
		}

		if subtreeTxs == nil {
			return nil
		}

		txs = append(txs, subtreeTxs...)
	}

	cb.txs = txs

	return nil
}

// recoverSubtreeTxs returns the subtree's non-coinbase transactions in order, or
// nil if they cannot be recovered.
func (c *checker) recoverSubtreeTxs(ctx context.Context, cb *chainBlock, subtreeHash chainhash.Hash) ([]*bt.Tx, error) {
	b, err := c.stores.SubtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		if isBlobNotFound(err) {
			c.blobMissing(cb, subtreeHash, fileformat.FileTypeSubtree)
			return nil, nil
		}

		return nil, errors.NewStorageError("chain oracle: read subtree %s", subtreeHash, err)
	}

	st, err := subtreepkg.NewSubtreeFromReader(bytes.NewReader(b))
	if err != nil {
		c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtree, fmt.Sprintf("does not decode: %v", err))
		return nil, nil
	}

	if root := st.RootHash(); !root.IsEqual(&subtreeHash) {
		c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtree, fmt.Sprintf("root hash is %s", root))
		return nil, nil
	}

	txs, ok, err := c.readSubtreeData(ctx, cb, subtreeHash, st)
	if err != nil || ok {
		return txs, err
	}

	return c.txsFromUTXOStore(ctx, st)
}

// readSubtreeData reads and checks the subtreeData file. ok is false when the
// file is missing or does not match, after the discrepancy has been recorded.
func (c *checker) readSubtreeData(ctx context.Context, cb *chainBlock, subtreeHash chainhash.Hash, st *subtreepkg.Subtree) ([]*bt.Tx, bool, error) {
	r, err := c.stores.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		if isBlobNotFound(err) {
			if c.stores.BlockStore != nil {
				// the block persister writes subtreeData; without a block store
				// in scope the caller is not checking persisted files
				c.blobMissing(cb, subtreeHash, fileformat.FileTypeSubtreeData)
			}

			return nil, false, nil
		}

		return nil, false, errors.NewStorageError("chain oracle: read subtreeData %s", subtreeHash, err)
	}

	defer r.Close()

	br := bufio.NewReader(r)
	txs := make([]*bt.Tx, 0, st.Length())

	for i, node := range st.Nodes {
		tx := &bt.Tx{}

		if _, err := tx.ReadFrom(br); err != nil {
			c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtreeData, fmt.Sprintf("tx %d of %d does not decode: %v", i, len(st.Nodes), err))
			return nil, false, nil
		}

		if node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			// the persister writes the block's coinbase in the placeholder's slot
			if !tx.TxIDChainHash().IsEqual(cb.block.CoinbaseTx.TxIDChainHash()) {
				c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtreeData,
					fmt.Sprintf("coinbase slot holds %s, block coinbase is %s", tx.TxIDChainHash(), cb.block.CoinbaseTx.TxIDChainHash()))

				return nil, false, nil
			}

			continue
		}

		if !tx.TxIDChainHash().IsEqual(&node.Hash) {
			c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtreeData, fmt.Sprintf("tx %d is %s, subtree has %s", i, tx.TxIDChainHash(), node.Hash))
			return nil, false, nil
		}

		txs = append(txs, tx)
	}

	if _, err := br.ReadByte(); err != io.EOF {
		c.blobMismatch(cb, subtreeHash, fileformat.FileTypeSubtreeData, "trailing bytes after the subtree's last transaction")
		return nil, false, nil
	}

	return txs, true, nil
}

// txsFromUTXOStore recovers a subtree's transactions from the UTXO store, for
// when the subtreeData file is unusable. It returns nil if any is missing.
func (c *checker) txsFromUTXOStore(ctx context.Context, st *subtreepkg.Subtree) ([]*bt.Tx, error) {
	txs := make([]*bt.Tx, 0, st.Length())

	for _, node := range st.Nodes {
		if node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			continue
		}

		hash := node.Hash

		data, err := c.stores.UTXO.Get(ctx, &hash, fields.Tx)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				return nil, nil
			}

			return nil, errors.NewStorageError("chain oracle: read tx %s from the UTXO store", hash, err)
		}

		if data == nil || data.Tx == nil {
			return nil, nil
		}

		txs = append(txs, data.Tx)
	}

	return txs, nil
}

// checkUTXODeltas asserts the block's utxo-additions and utxo-deletions files
// hold exactly the coin changes the replay derived for it.
func (c *checker) checkUTXODeltas(ctx context.Context, cb *chainBlock) error {
	if !c.report.UTXODeltasChecked {
		return nil
	}

	us, err := utxopersister.GetUTXOSet(ctx, ulogger.TestLogger{}, c.stores.Settings, c.stores.BlockStore, &cb.hash, cb.height)
	if err != nil {
		return errors.NewProcessingError("chain oracle: open utxo deltas for %s", cb.hash, err)
	}

	wantAdditions, wantDeletions := c.expectedDeltas(cb)

	gotAdditions, ok, err := c.readAdditions(ctx, cb, us)
	if err != nil || !ok {
		return err
	}

	c.diffDeltas(cb, fileformat.FileTypeUtxoAdditions, wantAdditions, gotAdditions)

	gotDeletions, ok, err := c.readDeletions(ctx, cb, us)
	if err != nil || !ok {
		return err
	}

	c.diffDeltas(cb, fileformat.FileTypeUtxoDeletions, wantDeletions, gotDeletions)

	return nil
}

// expectedDeltas renders the block's coin changes in the delta files' terms:
// every transaction contributes an additions record (with only its storable
// outputs), every non-coinbase input a deletions record.
func (c *checker) expectedDeltas(cb *chainBlock) (map[string]int, map[string]int) {
	additions := make(map[string]int)
	deletions := make(map[string]int)

	for _, tx := range cb.txs {
		txID := tx.TxIDChainHash()

		if !tx.IsCoinbase() {
			for _, input := range tx.Inputs {
				deletions[deletionKey(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)]++
			}
		}

		var outs []*utxopersister.UTXO

		for vout, output := range tx.Outputs {
			if output != nil && utxo.ShouldStoreOutputAsUTXO(output, cb.height, c.genesisActivationHeight) {
				outs = append(outs, &utxopersister.UTXO{Index: uint32(vout), Value: output.Satoshis, Script: *output.LockingScript}) //nolint:gosec // vout indexes tx.Outputs
			}
		}

		additions[additionKey(txID, cb.height, tx.IsCoinbase(), outs)]++
	}

	return additions, deletions
}

func (c *checker) readAdditions(ctx context.Context, cb *chainBlock, us *utxopersister.UTXOSet) (map[string]int, bool, error) {
	r, err := us.GetUTXOAdditionsReader(ctx)
	if err != nil {
		return nil, false, c.deltaOpenError(ctx, cb, fileformat.FileTypeUtxoAdditions, err)
	}

	defer r.Close()

	got := make(map[string]int)

	for {
		w, err := utxopersister.NewUTXOWrapperFromReader(ctx, r)
		if err != nil {
			if err == io.EOF {
				return got, true, nil
			}

			c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeUtxoAdditions, fmt.Sprintf("record %d does not decode: %v", len(got), err))

			return nil, false, nil
		}

		got[additionKey(&w.TxID, w.Height, w.Coinbase, w.UTXOs)]++
	}
}

func (c *checker) readDeletions(ctx context.Context, cb *chainBlock, us *utxopersister.UTXOSet) (map[string]int, bool, error) {
	r, err := us.GetUTXODeletionsReader(ctx)
	if err != nil {
		return nil, false, c.deltaOpenError(ctx, cb, fileformat.FileTypeUtxoDeletions, err)
	}

	defer r.Close()

	got := make(map[string]int)

	for {
		d, err := utxopersister.NewUTXODeletionFromReader(r)
		if err != nil {
			if err == io.EOF {
				return got, true, nil
			}

			c.blobMismatch(cb, chainhash.Hash{}, fileformat.FileTypeUtxoDeletions, fmt.Sprintf("record %d does not decode: %v", len(got), err))

			return nil, false, nil
		}

		got[deletionKey(&d.TxID, d.Index)]++
	}
}

// deltaOpenError records a missing or mis-headed delta file as a discrepancy
// and passes any other error through.
func (c *checker) deltaOpenError(ctx context.Context, cb *chainBlock, fileType fileformat.FileType, err error) error {
	if isBlobNotFound(err) {
		c.blobMissing(cb, chainhash.Hash{}, fileType)
		return nil
	}

	// the header names a different block or height: readDeltaHeader's mismatch
	// is a storage error carrying that text, not a transient read failure
	exists, existsErr := c.stores.BlockStore.Exists(ctx, cb.hash[:], fileType)
	if existsErr == nil && exists {
		c.blobMismatch(cb, chainhash.Hash{}, fileType, err.Error())
		return nil
	}

	return errors.NewStorageError("chain oracle: open %s for %s", fileType, cb.hash, err)
}

func (c *checker) diffDeltas(cb *chainBlock, fileType fileformat.FileType, want, got map[string]int) {
	for key, n := range want {
		if got[key] != n {
			c.blobMismatch(cb, chainhash.Hash{}, fileType, fmt.Sprintf("record %s: chain implies %d, file has %d", key, n, got[key]))
		}
	}

	for key, n := range got {
		if _, ok := want[key]; !ok {
			c.blobMismatch(cb, chainhash.Hash{}, fileType, fmt.Sprintf("record %s: chain implies 0, file has %d", key, n))
		}
	}
}

// checkUTXOStore diffs every best-chain transaction and storable output against
// the UTXO store.
func (c *checker) checkUTXOStore(ctx context.Context) error {
	for _, ct := range c.txs {
		data, err := c.stores.UTXO.Get(ctx, &ct.txID, fields.BlockIDs, fields.BlockHeights, fields.UnminedSince)
		if err != nil && !errors.Is(err, errors.ErrTxNotFound) {
			return errors.NewStorageError("chain oracle: read UTXO record %s", ct.txID, err)
		}

		if err != nil || data == nil {
			if c.allSpent(ct) {
				c.report.PrunedTxs++
			} else {
				c.utxo(KindUTXOTxMissing, ct, -1, "no UTXO record, yet the chain leaves an output unspent")
			}

			continue
		}

		if data.UnminedSince != 0 {
			c.utxo(KindUTXOTxNotMined, ct, -1, fmt.Sprintf("record is unmined since height %d", data.UnminedSince))
		}

		if !slices.Contains(data.BlockIDs, ct.blockID) {
			c.utxo(KindUTXOTxNotMined, ct, -1, fmt.Sprintf("record names blocks %v, not best-chain block id %d", data.BlockIDs, ct.blockID))
		}

		for _, vout := range ct.vouts {
			if err := c.checkOutput(ctx, ct, vout); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *checker) checkOutput(ctx context.Context, ct *chainTx, vout uint32) error {
	exp := c.outputs[outpoint{txID: ct.txID, vout: vout}]
	utxoHash := exp.utxoHash

	resp, err := c.stores.UTXO.GetSpend(ctx, &utxo.Spend{TxID: &ct.txID, Vout: vout, UTXOHash: &utxoHash})
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			c.utxo(KindUTXOOutputMissing, ct, int(vout), "the record has no such output")
			return nil
		}

		if errors.Is(err, errors.ErrUtxoHashMismatch) {
			c.utxo(KindUTXOOutputMissing, ct, int(vout), "the stored output's hash does not match the chain's output")
			return nil
		}

		return errors.NewStorageError("chain oracle: read output %s:%d", ct.txID, vout, err)
	}

	if resp == nil || resp.Status == int(utxo.Status_NOT_FOUND) {
		c.utxo(KindUTXOOutputMissing, ct, int(vout), "the record has no such output")
		return nil
	}

	var actual *chainhash.Hash
	if resp.SpendingData != nil {
		actual = resp.SpendingData.TxID
	}

	switch {
	case exp.spentBy != nil && actual == nil:
		c.utxo(KindUTXOUnspentShouldBeSpent, ct, int(vout), fmt.Sprintf("the chain spends it in %s", exp.spentBy))

	case exp.spentBy != nil && (!actual.IsEqual(exp.spentBy) || resp.SpendingData.Vin != exp.vin):
		c.utxo(KindUTXOWrongSpender, ct, int(vout), fmt.Sprintf("store has %s:%d, chain has %s:%d", actual, resp.SpendingData.Vin, exp.spentBy, exp.vin))

	case exp.spentBy == nil && actual != nil:
		return c.checkOffChainSpender(ctx, ct, vout, *actual)
	}

	return nil
}

// checkOffChainSpender decides whether an output the chain leaves unspent may
// legitimately be spent in the store: only by an unmined (mempool) transaction.
func (c *checker) checkOffChainSpender(ctx context.Context, ct *chainTx, vout uint32, spender chainhash.Hash) error {
	if onChain, ok := c.txIndex[spender]; ok {
		// a best-chain tx spends it, yet the replay did not: the replay would
		// have recorded the spend, so the store names the wrong input
		c.utxo(KindUTXOWrongSpender, ct, int(vout), fmt.Sprintf("store has it spent by best-chain tx %s (height %d), whose inputs do not spend it", spender, onChain.height))
		return nil
	}

	if !c.report.ReplayComplete {
		// the spender may sit in a block the oracle could not replay
		return nil
	}

	data, err := c.stores.UTXO.Get(ctx, &spender, fields.BlockIDs, fields.UnminedSince)
	if err != nil && !errors.Is(err, errors.ErrTxNotFound) {
		return errors.NewStorageError("chain oracle: read spender %s", spender, err)
	}

	switch {
	case err != nil || data == nil:
		c.utxo(KindUTXOSpentShouldBeUnspent, ct, int(vout), fmt.Sprintf("spent by %s, which has no UTXO record", spender))
	case isMined(data):
		c.utxo(KindUTXOSpentShouldBeUnspent, ct, int(vout), fmt.Sprintf("spent by %s, recorded as mined in blocks %v but not on the best chain", spender, data.BlockIDs))
	default:
		c.report.MempoolSpends++
	}

	return nil
}

// isBlobNotFound matches a blob store miss: the stores report ErrNotFound, and
// readers layered on them may wrap it as ErrBlobNotFound.
func isBlobNotFound(err error) bool {
	return errors.Is(err, errors.ErrNotFound) || errors.Is(err, errors.ErrBlobNotFound)
}

func isMined(data *meta.Data) bool {
	return data.UnminedSince == 0 && len(data.BlockIDs) > 0
}

// allSpent reports whether the chain spends every storable output of ct, which
// is when the pruner may delete its record.
func (c *checker) allSpent(ct *chainTx) bool {
	for _, vout := range ct.vouts {
		if c.outputs[outpoint{txID: ct.txID, vout: vout}].spentBy == nil {
			return false
		}
	}

	return true
}

func (c *checker) utxo(kind Kind, ct *chainTx, vout int, detail string) {
	c.report.add(Discrepancy{Kind: kind, Height: ct.height, Block: ct.block, TxID: ct.txID, Vout: vout, Detail: detail})
}

// blobMissing and blobMismatch take the subtree hash for subtree and
// subtreeData files, and the zero hash for files keyed by the block hash.
func (c *checker) blobMissing(cb *chainBlock, subtree chainhash.Hash, fileType fileformat.FileType) {
	c.report.add(Discrepancy{Kind: KindBlobMissing, Height: cb.height, Block: cb.hash, Subtree: subtree, Vout: -1, FileType: fileType})
}

func (c *checker) blobMismatch(cb *chainBlock, subtree chainhash.Hash, fileType fileformat.FileType, detail string) {
	c.report.add(Discrepancy{Kind: KindBlobMismatch, Height: cb.height, Block: cb.hash, Subtree: subtree, Vout: -1, FileType: fileType, Detail: detail})
}

// additionKey renders one utxo-additions record so records compare by value.
func additionKey(txID *chainhash.Hash, height uint32, coinbase bool, outs []*utxopersister.UTXO) string {
	var b bytes.Buffer

	fmt.Fprintf(&b, "%s@%d coinbase=%v outputs=[", txID, height, coinbase)

	for i, o := range outs {
		if i > 0 {
			b.WriteByte(' ')
		}

		scriptHash := chainhash.HashH(o.Script)
		fmt.Fprintf(&b, "%d:%d:%x", o.Index, o.Value, scriptHash[:8])
	}

	b.WriteByte(']')

	return b.String()
}

// deletionKey renders one utxo-deletions record.
func deletionKey(txID *chainhash.Hash, vout uint32) string {
	return fmt.Sprintf("%s:%d", txID, vout)
}
