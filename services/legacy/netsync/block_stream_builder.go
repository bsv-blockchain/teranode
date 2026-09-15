package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
)

// subtreeEmitFunc is called once per completed subtree, in block order. The
// builder drops its reference to all three artefacts as soon as this returns, so
// an implementation that needs them beyond the call must take its own copy.
type subtreeEmitFunc func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error

// blockStreamBuilder turns a stream of a block's transactions into subtrees,
// emitting each one as it fills and keeping only its root hash afterwards.
//
// It can decide the whole partition before the first transaction arrives because
// a block message carries its transaction count immediately after the 80-byte
// header. That is what makes streaming possible at all: the subtree sizes are
// known up front rather than discovered at the end.
//
// Resident state is one subtree under construction plus one 32-byte hash per
// completed subtree. On mainnet, where a subtree holds 4096 transactions, a
// 260,000-transaction block is 64 hashes and one subtree rather than the whole
// block.
type blockStreamBuilder struct {
	maxItems     int
	txCount      int
	subtreeCount int
	subtreeSize  int
	finalLeaves  int

	emit func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error
	acc  *merkleAccumulator

	current     *subtreepkg.Subtree
	currentData *subtreepkg.Data
	currentMeta *subtreepkg.Meta
	currentCap  int

	subtreeHashes []chainhash.Hash
	emitted       int
	seen          int
	failed        error

	// dedup rejects a transaction whose hash has already appeared in this block.
	//
	// The merkle root cannot catch that: the duplicate-last-when-odd rule means a
	// mutated transaction list produces the same root, the same header and the
	// same block hash as the honest one, so CheckMerkleRoot passes on it. This is
	// the only thing that catches it, and it is required below the checkpoint too,
	// because a checkpoint anchors the block HASH while the peer supplies the BODY
	// (model/check_duplicate_txs.go:23).
	//
	// Put IS the check: it fails when the hash repeats, so there is no second
	// scan and nothing is re-read. The constructor refuses a nil map rather than
	// accept one: commit 143350330 left one route without this check and
	// c753d2e46 had to add it back (model/check_duplicate_txs.go), so a
	// constructor that lets an integrator silently skip it reintroduces the same
	// fault by a different door.
	dedup txmap.TxMap

	// recentOutputs maps a transaction seen earlier in THIS block to its
	// outputs, so a child spending one can be extended without a store lookup.
	//
	// Bounded, and the bound is the whole design. Remembering every output of
	// every transaction would put the block's entire output data back in the
	// heap, which is exactly what streaming the block exists to avoid: a decoded
	// block was measured at 3.3 to 12.1 GB against a 6 GiB process ceiling.
	// In-block spends cluster near their parent, so a small window catches the
	// great majority at fixed cost, and anything it misses is simply stored
	// unextended and re-extended by the reader, which it already does for every
	// out-of-block parent.
	recentOutputs map[chainhash.Hash][]*bt.Output
	// recentOrder is the insertion order of recentOutputs, so the oldest entry
	// is the one evicted.
	recentOrder []chainhash.Hash
	// recentLimit is how many transactions' outputs are remembered at once.
	recentLimit int
}

// streamExtendWindow is how many recent transactions' outputs the builder keeps
// so a child spending a parent in the same block can be extended without a store
// lookup. Best effort by design: a miss costs nothing beyond the reader
// re-extending, which it does for out-of-block parents anyway.
const streamExtendWindow = 4096

// newBlockStreamBuilder prepares a builder for a block declaring txCount
// transactions, including its coinbase, partitioned into subtrees of at most
// maxItems leaves. dedup rejects a block carrying the same transaction twice; it
// may be an in-memory map or the disk-backed one, both of which satisfy
// txmap.TxMap, and the pipeline does not care which is in use. dedup must not be
// nil: a nil map would silently disable the CVE-2012-2459 duplicate-transaction
// check, so the constructor refuses it outright instead of letting a hurried
// caller reach for nil.
func newBlockStreamBuilder(txCount, maxItems int, coinbase *bt.Tx, emit subtreeEmitFunc, dedup txmap.TxMap) (*blockStreamBuilder, error) {
	if coinbase == nil {
		return nil, errors.NewProcessingError("[blockStreamBuilder] no coinbase transaction")
	}

	if emit == nil {
		return nil, errors.NewProcessingError("[blockStreamBuilder] no emit function")
	}

	if dedup == nil {
		return nil, errors.NewProcessingError("[blockStreamBuilder] no duplicate-transaction map (dedup)")
	}

	// A coinbase-only block (txCount <= 1) has no transactions to stream: the
	// caller's early return for this case (handle_block.go prepareSubtrees)
	// produces zero subtrees and zero files, but this builder would instead emit
	// one subtree whose root is the go-subtree CoinbasePlaceholder constant —
	// the same placeholder root for every coinbase-only block in the chain, so
	// every one of them would write three files under the same three keys,
	// overwriting each other, and hand back a subtree list production never
	// produces. Refuse it outright rather than diverge from that caller.
	if txCount <= 1 {
		return nil, errors.NewProcessingError("[blockStreamBuilder] refusing a coinbase-only block, got tx count %d", txCount)
	}

	size, count, finalLeaves, err := partitionLegacyBlock(txCount, maxItems)
	if err != nil {
		return nil, err
	}

	acc, err := newMerkleAccumulator(count, coinbase.TxIDChainHash(), uint64(coinbase.Size()))
	if err != nil {
		return nil, err
	}

	b := &blockStreamBuilder{
		maxItems:      maxItems,
		txCount:       txCount,
		subtreeCount:  count,
		subtreeSize:   size,
		finalLeaves:   finalLeaves,
		emit:          emit,
		acc:           acc,
		subtreeHashes: make([]chainhash.Hash, 0, count),
		dedup:         dedup,
		recentLimit:   streamExtendWindow,
	}

	if err = b.startSubtree(); err != nil {
		return nil, err
	}

	// The coinbase occupies slot zero as a placeholder. Its real transaction id
	// is substituted when the first subtree's root is taken, which is what
	// merkleAccumulator does on the first Add.
	if err = b.current.AddCoinbaseNode(); err != nil {
		return nil, errors.NewSubtreeError("[blockStreamBuilder] failed adding the coinbase placeholder", err)
	}

	b.seen = 1

	return b, nil
}

// startSubtree opens the next subtree, sized from the partition decided in the
// constructor. Only the final subtree may be smaller than the rest.
func (b *blockStreamBuilder) startSubtree() error {
	capacity := b.subtreeSize
	if b.emitted == b.subtreeCount-1 && b.subtreeCount > 1 && b.finalLeaves < b.subtreeSize {
		capacity = b.finalLeaves
	}

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
	if err != nil {
		return errors.NewSubtreeError("[blockStreamBuilder] failed creating subtree %d of %d", b.emitted, b.subtreeCount, err)
	}

	b.current = st
	b.currentData = subtreepkg.NewSubtreeData(st)
	b.currentMeta = subtreepkg.NewSubtreeMeta(st)
	b.currentCap = capacity

	return nil
}

// AddTx adds one transaction in block order, emitting the current subtree if it
// becomes full. txHash is passed in rather than recomputed because the caller has
// already hashed the transaction to know what it is.
func (b *blockStreamBuilder) AddTx(tx *bt.Tx, txHash *chainhash.Hash) error {
	if b.failed != nil {
		return b.failed
	}

	if tx == nil || txHash == nil {
		return b.fail(errors.NewProcessingError("[blockStreamBuilder] nil transaction at index %d", b.seen))
	}

	if b.seen >= b.txCount {
		return b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] peer sent more transactions than the %d it declared", b.txCount))
	}

	// Memoise the hash the caller already computed, before anything below can
	// force it to be recomputed. go-bt's TxIDChainHash returns a cached hash
	// only after SetTxHash has been called; otherwise it re-serialises the
	// whole transaction and double-hashes it on every call, and never caches
	// that result itself. Both currentData.AddTx and
	// currentMeta.SetTxInpointsFromTx below call TxIDChainHash, so without
	// this line every transaction is fully re-serialised twice per AddTx. The
	// same pattern in block validation's extend stage, over a
	// 100,001-transaction block, cost two full passes over 3.44 GB on one
	// goroutine — 17.5 microseconds and 41 KB allocated per call for a 33 KB
	// transaction (see services/blockvalidation/quick_validate.go,
	// "TxIDChainHash never populates its own cache"). Production does not hit
	// this: createTxMap calls SetTxHash from the wire hash before handing
	// transactions on.
	tx.SetTxHash(txHash)

	if b.dedup != nil {
		if err := b.dedup.Put(*txHash, uint64(b.dedup.Length())); err != nil {
			// go-tx-map's own in-memory implementations wrap their exported
			// sentinel txmap.ErrHashAlreadyExists (tx_map.go:132) with %w, so
			// errors.Is unwraps it correctly — confirmed against the pinned
			// go-tx-map version, and matched by the library's own test at
			// tx_map_test.go:224. The disk-backed model.DiskTxMapUint64 instead
			// returns teranode's errors.ErrTxExists directly. Checking both
			// keeps this a BlockInvalidError (peer misbehaviour) rather than a
			// generic processing error regardless of which concrete
			// txmap.TxMap a future caller supplies; collapsing to only one
			// classifies a genuine duplicate as a transient fault the moment
			// the other map's wording ever changes.
			if errors.Is(err, errors.ErrTxExists) || errors.Is(err, txmap.ErrHashAlreadyExists) {
				return b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] block contains duplicate transaction %s (CVE-2012-2459)", txHash))
			}

			return b.fail(errors.NewProcessingError("[blockStreamBuilder] failed recording transaction %s for duplicate detection", txHash, err))
		}
	}

	nodeIdx := b.current.Length()

	// Extend what this block can answer for itself. Every input whose parent
	// went past earlier in this same block is filled in here, for the cost of a
	// map lookup: the parent's outputs are already in hand and no store is
	// touched. Anything else is left standard, and the reader re-extends it on
	// demand the way it already does for an out-of-block parent.
	extended := b.extendFromBlock(tx)

	// A fee is only real when every input is extended: a partial extension
	// cannot produce one, and a wrong fee is worse than no fee. Zero stays the
	// default otherwise. That default is safe at every height regardless: below
	// the checkpoint subtree fees are not consensus-checked at all, and above it
	// newSubtreeWriter/newSubtreeWriterUnresolvedHeight (subtree_writer.go)
	// write every subtree from this path as FileTypeSubtreeToCheck, never the
	// already-validated FileTypeSubtree, so subtree validation re-derives the
	// real fee from the transactions before any consensus check reads it —
	// exactly as it does for a subtree fetched whole from a peer, which also
	// carries no fee of its own until that same re-derivation runs. What
	// stamping the real fee here buys is work that re-derivation no longer has
	// to do, and a transaction that already arrives extended for every
	// consumer downstream.
	var fee uint64

	// calculateTransactionFee's error return is not propagated, unlike its call
	// in the production subtree builder (createSubtrees, handle_block.go),
	// where the same error is fatal for the block. That divergence is
	// deliberate here, not an oversight: with extended == true the "not
	// extended" branch of that error is unreachable, so what can still come
	// back is only its input-less-than-output case, and the fee this builder
	// stamps is provisional in every case — subtree validation recomputes it
	// from transaction metadata before any consensus check reads it. Falling
	// back to the same zero used for a miss costs nothing that path does not
	// already re-derive; failing the whole block over a number about to be
	// thrown away would not.
	if extended {
		if f, feeErr := calculateTransactionFee(tx); feeErr == nil && f > 0 {
			fee = f
		}
	}

	if err := b.current.AddNode(*txHash, fee, uint64(tx.Size())); err != nil {
		return b.fail(errors.NewSubtreeError("[blockStreamBuilder] failed adding transaction %s to subtree %d", txHash, b.emitted, err))
	}

	if err := b.currentData.AddTx(tx, nodeIdx); err != nil {
		return b.fail(errors.NewTxError("[blockStreamBuilder] failed adding transaction %s to subtree data", txHash, err))
	}

	if err := b.currentMeta.SetTxInpointsFromTx(tx); err != nil {
		return b.fail(errors.NewTxError("[blockStreamBuilder] failed adding transaction %s to subtree meta", txHash, err))
	}

	b.seen++

	// Remembered only now, after this transaction has already been extended
	// (or not) from what came before it: a transaction must not extend from
	// its own outputs.
	b.rememberOutputs(*txHash, tx)

	// The final subtree is never auto-emitted here, even when a transaction
	// fills it exactly: Finish is the only caller allowed to emit it. Without
	// this guard, a block whose transaction count divides evenly into the
	// partition (e.g. 20 leaves as 8+8+4) fills the last subtree on the final
	// AddTx call and emits it mid-stream, which is indistinguishable from a
	// truncated block that happened to stop at the right length. Deferring to
	// Finish keeps one place responsible for deciding the stream is complete.
	if b.current.Length() >= b.currentCap && b.emitted < b.subtreeCount-1 {
		if err := b.emitCurrent(); err != nil {
			return err
		}
	}

	return nil
}

// extendFromBlock fills in every input whose parent appeared earlier in this
// block, and reports whether the transaction ended up fully extended.
//
// It never reaches a store. That is the point: this runs on the peer's read
// goroutine, and a lookup per input there would put the socket behind the UTXO
// store.
func (b *blockStreamBuilder) extendFromBlock(tx *bt.Tx) bool {
	if tx.IsCoinbase() {
		return false
	}

	full := true

	for _, in := range tx.Inputs {
		if in.PreviousTxScript != nil {
			continue
		}

		outputs, ok := b.recentOutputs[*in.PreviousTxIDChainHash()]
		if !ok || int(in.PreviousTxOutIndex) >= len(outputs) {
			full = false

			continue
		}

		parent := outputs[in.PreviousTxOutIndex]
		in.PreviousTxScript = parent.LockingScript
		in.PreviousTxSatoshis = parent.Satoshis
	}

	return full
}

// rememberOutputs records this transaction's outputs for the children that
// follow it in this block, evicting the oldest once the window is full.
func (b *blockStreamBuilder) rememberOutputs(txHash chainhash.Hash, tx *bt.Tx) {
	if b.recentOutputs == nil {
		b.recentOutputs = make(map[chainhash.Hash][]*bt.Output, b.recentLimit)
	}

	b.recentOutputs[txHash] = tx.Outputs
	b.recentOrder = append(b.recentOrder, txHash)

	for len(b.recentOrder) > b.recentLimit {
		delete(b.recentOutputs, b.recentOrder[0])
		b.recentOrder = b.recentOrder[1:]
	}
}

// rememberedOutputs reports how many transactions' outputs are held, for the
// bound's own test.
func (b *blockStreamBuilder) rememberedOutputs() int {
	return len(b.recentOutputs)
}

// emitCurrent hands the completed subtree to the caller, folds its root into the
// accumulator, and drops every reference to it.
func (b *blockStreamBuilder) emitCurrent() error {
	isLast := b.emitted == b.subtreeCount-1

	if err := b.emit(b.emitted, b.current, b.currentData, b.currentMeta); err != nil {
		return b.fail(err)
	}

	if err := b.acc.Add(b.current, isLast); err != nil {
		return b.fail(err)
	}

	b.subtreeHashes = append(b.subtreeHashes, *b.current.RootHash())
	b.emitted++

	b.current = nil
	b.currentData = nil
	b.currentMeta = nil

	if b.emitted < b.subtreeCount {
		return b.startSubtree()
	}

	return nil
}

// Finish emits any partial final subtree and returns the block's merkle root
// together with the subtree root hashes in block order.
func (b *blockStreamBuilder) Finish() (*chainhash.Hash, []chainhash.Hash, error) {
	if b.failed != nil {
		return nil, nil, b.failed
	}

	if b.seen != b.txCount {
		return nil, nil, b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] stream ended after %d of the %d transactions it declared", b.seen, b.txCount))
	}

	if b.current != nil && b.current.Length() > 0 {
		if err := b.emitCurrent(); err != nil {
			return nil, nil, err
		}
	}

	root, err := b.acc.Root()
	if err != nil {
		return nil, nil, b.fail(err)
	}

	return root, b.subtreeHashes, nil
}

// SubtreeHashes returns the subtree root hashes emitted so far, in block order.
func (b *blockStreamBuilder) SubtreeHashes() []chainhash.Hash {
	return b.subtreeHashes
}

// fail latches the first error. Once a block has failed, there is no second
// source for the subtree files it was writing, so continuing would leave a block
// whose artefacts are incomplete rather than absent.
func (b *blockStreamBuilder) fail(err error) error {
	if b.failed == nil {
		b.failed = err
		b.current = nil
		b.currentData = nil
		b.currentMeta = nil
	}

	return b.failed
}
