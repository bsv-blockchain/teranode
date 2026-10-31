package blockvalidation

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// Overtaken-tip repair (issue 2051).
//
// setTxMined clears unmined_since on a block's txs when the block is on the longest chain
// at the time it runs. If a competing chain later overtakes that block before block
// assembly has moved onto it, block assembly reaches the new tip by a pure catch-up and
// never sees the block as a moveBack block, so nothing in block assembly restores
// unmined_since. Those txs then sit in the mempool looking mined: the pruner treats them
// (and their parents) as prunable, and loadUnminedTransactions cannot see them. At scale
// block assembly lags by minutes, so this is the normal case in a fork storm, not an edge.
//
// Block assembly's reorg used to cover this by re-marking its whole mempool on every reorg,
// a batch write per mempool tx. The repair here replaces that: it tracks the blocks
// setTxMined wrote as on-chain and, when one has left the chain, re-marks its txs that are
// not mined on the new chain.
//
// It runs on the setMined worker, between blocks, one slice of txs at a time. Running it
// there, rather than concurrently, is what makes it correct: no setTxMined write can land
// in the middle of a slice's read-decide-write, so for a tx the overtaken block shares with
// another block both orders end right. If the slice runs first, the tx is stamped and the
// other block's setTxMined clears it again; if that setTxMined ran first, the tx's block
// ids show it is mined on the longest chain and the slice leaves it alone. The slice size
// bounds how long a repair delays the next block's mined_set.
//
// Limitations. The repair is serialised with this process's setMined worker only: it
// assumes one block validation instance writes setTxMined for the chain. Its state is in
// memory. After a restart the tracker is rebuilt from the chain without knowing which
// blocks were re-marked, so a re-marked block that comes back onto the chain afterwards
// keeps those txs flagged unmined until block assembly's next unmined-transaction load
// fixes them (fixUnminedSinceInconsistencies). That is the safe direction. The unsafe one
// remains for fork blocks deeper than tipRepairSeedForkDepth when the seed runs: the seed
// does not see them, so if one was overtaken with its repair unfinished, or with a
// setTxMined that cleared some of its txs and then failed, those txs keep looking mined
// while off the chain, and nothing else re-marks them.

const (
	// tipRepairWindow is how deep below the best block a tracked block is kept. A reorg
	// deeper than this is not repaired here.
	tipRepairWindow = 1000

	// tipRepairSeedForkDepth bounds the fork blocks the startup seed looks at, measured from
	// the best height. The seed cannot tell a fork block that was once the tip from one
	// that never was, and each one costs a read per tx, so it stays shallow.
	tipRepairSeedForkDepth = 10

	// tipRepairSeedMaxBlocks caps the recent blocks the startup seed reads to find them.
	tipRepairSeedMaxBlocks = 1000

	// tipRepairMaxAttempts is how many times a block may fail permanently (its block or
	// subtrees are gone) before it is dropped. Transient failures do not count.
	tipRepairMaxAttempts = 10

	// defaultTipRepairSliceSize is used when BlockValidation.TipRepairSliceSize is unset.
	defaultTipRepairSliceSize = 250_000

	// tipRepairPendingBaseBackoff is how long a block with subtrees set aside waits for its
	// first pass over them again; each later pass waits twice as long, up to
	// tipRepairPendingMaxBackoff. The wait is in time, not blocks, so a subtree that recovers
	// is repaired well before the pruner can delete a parent its off-chain txs spent.
	tipRepairPendingBaseBackoff = time.Second
	tipRepairPendingMaxBackoff  = time.Minute

	// tipRepairSubtreeReadAttempts and tipRepairSubtreeReadBackoff match Block.GetSubtrees.
	tipRepairSubtreeReadAttempts = 3
	tipRepairSubtreeReadBackoff  = 100 * time.Millisecond
)

// tipRepairExpired reports whether a tracked block at height has left the window. Each slice
// reads main-chain ids with GetBlockHeaderIDs(best, tipRepairWindow), which covers best down
// to best-tipRepairWindow+1; a block below that would look off chain while it is not.
func tipRepairExpired(height, bestHeight uint32) bool {
	return height+tipRepairWindow <= bestHeight
}

// isPermanentRepairError reports whether a repair error means the block can never be
// repaired: the block or its subtrees are gone. A storage error is the store failing while
// it looked, whatever it wraps, so it is never permanent (the subtreeAbsent rule).
func isPermanentRepairError(err error) bool {
	return (errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrSubtreeNotFound) ||
		errors.Is(err, errors.ErrBlobNotFound) || errors.Is(err, errors.ErrNotFound)) && !errors.Is(err, errors.ErrStorageError)
}

// minedOnChainBlock is a block whose txs setTxMined may have marked mined on the longest
// chain, in a write that completed or one that failed part way.
type minedOnChainBlock struct {
	id     uint32
	height uint32
	// gen changes whenever a new on-chain write of the block starts. A repair in progress
	// for an older gen is abandoned: the new write cleared the txs again.
	gen uint64
	// stamped is set once any of the block's txs were re-marked not on the longest chain,
	// so they need marking mined again if the block comes back onto the chain.
	stamped bool
	// done is set when every tx of the block has been through the repair while it was off
	// the chain; it is cleared when the block is seen back on the chain.
	done     bool
	failures int // permanent failures so far
	// deferred is set when the block's job kept failing and was set aside, so other
	// blocks are repaired first.
	deferred bool
	// pending holds the subtrees an unmark job could not read and set aside; the block is
	// not done, and its next unmark job reads only these. nil means all of them.
	pending []*chainhash.Hash
	// passes counts the unmark jobs that ended with subtrees set aside. The next one may
	// start at nextPass, or sooner once the best height is above passHeight, so a subtree
	// that stays unreadable is not read again on every slice, but is on every block.
	passes     int
	nextPass   time.Time
	passHeight uint32
}

// clearPending has the block's next unmark job read all of it again.
func (b *minedOnChainBlock) clearPending() {
	b.pending = nil
	b.passes = 0
	b.nextPass = time.Time{}
	b.passHeight = 0
}

// minedOnChainTracker holds the blocks the overtaken-tip repair watches. A nil tracker
// tracks nothing, for a BlockValidation not built by NewBlockValidation.
type minedOnChainTracker struct {
	mu      sync.Mutex
	blocks  map[chainhash.Hash]minedOnChainBlock
	nextGen uint64
	// now times the back-off between passes over subtrees set aside.
	now func() time.Time
}

func newMinedOnChainTracker() *minedOnChainTracker {
	return &minedOnChainTracker{blocks: make(map[chainhash.Hash]minedOnChainBlock), now: time.Now}
}

// nextPendingPass returns the earliest nextPass of the blocks with subtrees set aside, by
// an unmark job or a remine.
func (t *minedOnChainTracker) nextPendingPass() (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	var earliest time.Time

	for _, b := range t.blocks {
		// What startNextJob would start: a block that is done is owed nothing.
		owed := !b.done && (b.pending != nil || (b.stamped && b.passes > 0))
		if owed && (earliest.IsZero() || b.nextPass.Before(earliest)) {
			earliest = b.nextPass
		}
	}

	return earliest, !earliest.IsZero()
}

// setAsideSubtrees returns how many subtrees of the tracked blocks an unmark job set aside.
func (t *minedOnChainTracker) setAsideSubtrees() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	n := 0
	for _, b := range t.blocks {
		n += len(b.pending)
	}

	return n
}

// track watches a block whose on-chain write is starting: the write clears the block's txs
// again, so the block gets a new gen and starts over.
func (t *minedOnChainTracker) track(hash chainhash.Hash, id, height uint32) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// stamped stays: if this write fails before clearing the re-marked txs again, the remine
	// job is what marks them mined, and a remine over txs already mined writes nothing.
	t.nextGen++
	t.blocks[hash] = minedOnChainBlock{id: id, height: height, gen: t.nextGen, failures: t.blocks[hash].failures, stamped: t.blocks[hash].stamped}
}

// seed watches a block found by the restart seed. A block already watched keeps its state.
func (t *minedOnChainTracker) seed(hash chainhash.Hash, id, height uint32) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.blocks[hash]; ok {
		return
	}

	t.nextGen++
	t.blocks[hash] = minedOnChainBlock{id: id, height: height, gen: t.nextGen}
}

// markWritten records that a setTxMined write of the block has completed and reports
// whether the block is tracked, in which case the caller asks for a repair slice. The write
// as on the longest chain may have cleared txs after a repair read them, so a finished repair
// runs again, and it cleared every tx of the block, so no remine is owed after it. A write
// as off the chain clears none, and changes nothing here.
func (t *minedOnChainTracker) markWritten(hash chainhash.Hash, onLongestChain bool) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	b, ok := t.blocks[hash]
	if !ok {
		return false
	}

	if onLongestChain {
		b.done = false
		b.clearPending()
		b.stamped = false
		t.blocks[hash] = b
	}

	return true
}

// update applies fn to the block's entry if it is still at gen.
func (t *minedOnChainTracker) update(hash chainhash.Hash, gen uint64, fn func(*minedOnChainBlock)) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if b, ok := t.blocks[hash]; ok && b.gen == gen {
		fn(&b)
		t.blocks[hash] = b
	}
}

func (t *minedOnChainTracker) drop(hash chainhash.Hash) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.blocks, hash)
}

func (t *minedOnChainTracker) empty() bool {
	if t == nil {
		return true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.blocks) == 0
}

func (t *minedOnChainTracker) snapshot() map[chainhash.Hash]minedOnChainBlock {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	out := make(map[chainhash.Hash]minedOnChainBlock, len(t.blocks))
	for hash, b := range t.blocks {
		out[hash] = b
	}

	return out
}

// tipRepairJobKind is what a tipRepairJob writes.
type tipRepairJobKind int

const (
	// unmarkJob re-marks the txs of a block that left the chain and are not mined on it.
	unmarkJob tipRepairJobKind = iota
	// remineJob marks the txs of a re-marked block mined again, now that it is back on the
	// chain, for those that still carry the block's id.
	remineJob
)

// tipRepairJob is a repair in progress over one block, worked through one subtree, and
// within a subtree one slice of txs, per step. Only the current subtree's tx hashes are
// held.
type tipRepairJob struct {
	kind     tipRepairJobKind
	hash     chainhash.Hash
	id       uint32
	gen      uint64
	subtrees []*chainhash.Hash
	// nextSubtree is the index of the subtree to load once current is used up.
	nextSubtree int
	current     []subtreepkg.Node
	marked      int
	total       int
	// failures counts the consecutive failed steps.
	failures int
	// subtreeFailures counts the failed loads of the subtree at nextSubtree.
	subtreeFailures int
	// unreadable holds the subtrees set aside because they could not be read.
	unreadable []*chainhash.Hash
	// height is the best height at the job's latest step.
	height uint32
}

// writeRepairHoldLocked records in the blockchain state the tracked blocks that are off the
// main chain and not yet repaired, pending or with subtrees set aside: their transactions may
// look mined while off the chain, and the pruner deletes nothing while any is listed. A block
// on the chain owing a remine is not listed: its transactions look unmined while mined, which
// only keeps their parents longer. The record is written only when it changes. It is not
// written before the tracker is seeded, so a hold recorded before a restart is read back
// first (seedMinedOnChainTrackerLocked).
func (u *BlockValidation) writeRepairHoldLocked(ctx context.Context, blockchainClient blockchain.ClientI) error {
	if !u.tipRepairSeeded {
		return nil
	}

	held := make([]model.HeldBlock, 0)

	for hash, b := range u.minedOnChain.snapshot() {
		if !u.tipRepairMainChain[b.id] && !b.done {
			held = append(held, model.HeldBlock{Hash: hash, ID: b.id, Height: b.height})
		}
	}

	slices.SortFunc(held, func(a, b model.HeldBlock) int {
		if c := cmp.Compare(a.Height, b.Height); c != 0 {
			return c
		}

		return bytes.Compare(a.Hash[:], b.Hash[:])
	})

	data := model.EncodeOvertakenTipRepairHold(held)
	if u.tipRepairHoldWritten && bytes.Equal(data, u.tipRepairHold) {
		return nil
	}

	if err := blockchainClient.SetState(ctx, model.OvertakenTipRepairHoldKey, data); err != nil {
		return errors.NewServiceError("[repairOvertakenTips] failed to record the pruning hold", err)
	}

	if len(held) > 0 {
		u.logger.Warnf("[repairOvertakenTips] holding pruning for %d overtaken blocks still being repaired, lowest at height %d", len(held), held[0].Height)
	} else if u.tipRepairHoldWritten && len(u.tipRepairHold) > 0 {
		u.logger.Infof("[repairOvertakenTips] every overtaken block is repaired, pruning released")
	}

	u.tipRepairHold = data
	u.tipRepairHoldWritten = true

	return nil
}

// signalTipRepair asks the setMined worker for a repair slice. Never blocks; a pending
// request covers any number of calls.
func (u *BlockValidation) signalTipRepair() {
	u.tipRepairPending.Store(true)

	select {
	case u.tipRepairSignal <- struct{}{}:
	default:
	}
}

// runTipRepairSlice is called by the setMined worker when a slice was requested. It runs one
// slice and requests the next while there is more to do. A transient failure is retried on
// the next request (a chain change, a write, or the periodic tick, which keeps asking while
// tipRepairRetry is set).
func (u *BlockValidation) runTipRepairSlice(ctx context.Context, blockchainClient blockchain.ClientI) {
	more, err := u.tipRepairSlice(ctx, blockchainClient)
	u.tipRepairRetry.Store(err != nil)

	if err != nil {
		if ctx.Err() != nil {
			return
		}

		u.logger.Errorf("[repairOvertakenTips] slice failed, will retry: %v", err)

		return
	}

	if more {
		u.signalTipRepair()
		return
	}

	// Nothing is due now. A pass over subtrees set aside comes due at its nextPass even on
	// an idle chain, with no block or write to request a slice, so wake for the earliest.
	// A pass already due that this slice did not start is not waiting on the clock, and
	// waking for it at once would only repeat this slice.
	if at, ok := u.minedOnChain.nextPendingPass(); ok {
		if wait := at.Sub(u.minedOnChain.now()); wait > 0 {
			if u.tipRepairWake != nil {
				u.tipRepairWake.Stop()
			}

			u.tipRepairWake = time.AfterFunc(wait, u.signalTipRepair)
		}
	}
}

// seedMinedOnChainTrackerLocked rebuilds the in-memory tracker after a restart: the
// main-chain blocks inside the window, and the fork blocks within tipRepairSeedForkDepth of
// the best height, which may have been the tip before the restart.
func (u *BlockValidation) seedMinedOnChainTrackerLocked(ctx context.Context, blockchainClient blockchain.ClientI) error {
	// The blocks held before the restart first: their repair had not finished, and the
	// window and fork depth below may no longer reach them. Read before anything here can
	// write the hold again.
	if err := u.seedHeldBlocksLocked(ctx, blockchainClient); err != nil {
		return err
	}

	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewServiceError("[repairOvertakenTips] seed: failed to get best block header", err)
	}

	headers, metas, err := u.mainChainHeaders(ctx, blockchainClient, *bestHeader.Hash())
	if err != nil {
		return errors.NewServiceError("[repairOvertakenTips] seed: failed to get main chain headers", err)
	}

	u.trackMinedHeaders(headers, metas)

	// Fork blocks near the tip: the highest blocks, side chains included, in pages within
	// blockchain_maxBlocksByHeightRange. GetChainTips would do too, but on SQLite it holds a
	// row cursor open across nested queries and deadlocks against a concurrent write
	// transaction.
	pageMax := int64(u.settings.BlockChain.MaxBlocksByHeightRange)
	if pageMax <= 0 {
		pageMax = tipRepairSeedMaxBlocks
	}

	seen := make(map[chainhash.Hash]bool)
	fromHeight := uint32(0) // 0 is no upper bound

	for read := int64(0); read < tipRepairSeedMaxBlocks; {
		page := min(tipRepairSeedMaxBlocks-read, pageMax)

		recent, err := blockchainClient.GetLastNBlocks(ctx, page, true, fromHeight)
		if err != nil {
			return errors.NewServiceError("[repairOvertakenTips] seed: failed to get recent blocks", err)
		}

		read += int64(len(recent))
		added := false

		for _, info := range recent {
			if info.Height+tipRepairSeedForkDepth < bestMeta.Height {
				u.tipRepairSeeded = true // highest first: the rest are deeper
				return nil
			}

			header, err := model.NewBlockHeaderFromBytes(info.BlockHeader)
			if err != nil {
				return errors.NewProcessingError("[repairOvertakenTips] seed: invalid block header at height %d", info.Height, err)
			}

			// A page starts at the lowest height of the last one, which it may share.
			if seen[*header.Hash()] {
				continue
			}

			seen[*header.Hash()] = true
			added = true

			_, meta, err := blockchainClient.GetBlockHeader(ctx, header.Hash())
			if err != nil {
				return errors.NewServiceError("[repairOvertakenTips] seed: failed to get block header %s", header.Hash().String(), err)
			}

			if meta == nil || meta.Invalid {
				continue
			}

			// A block whose mined_set is not written is watched too: a setTxMined that failed
			// part way before the restart may have cleared some of its txs, and its retry,
			// with onLongestChain=false once it is off the chain, leaves unmined_since alone.
			u.minedOnChain.seed(*header.Hash(), meta.ID, meta.Height)
		}

		lowest := recent[len(recent)-1].Height
		if int64(len(recent)) < page || lowest == 0 {
			break
		}

		// The next page starts at the lowest height of this one, which it may share. A page
		// with nothing new, or one all at the height it started from, would be read again
		// as it is: step below it. Blocks at one height beyond the cap cannot be reached and
		// are left out.
		next := lowest
		if !added || (fromHeight != 0 && lowest >= fromHeight) {
			next = lowest - 1
		}

		if fromHeight != 0 && next >= fromHeight {
			break // the store did not bound the page by fromHeight
		}

		fromHeight = next
	}

	u.tipRepairSeeded = true

	return nil
}

// holdRecordedLocked reports whether a hold recorded before this process started lists any
// block. It reads the record once: after that, only this process writes it.
func (u *BlockValidation) holdRecordedLocked(ctx context.Context, blockchainClient blockchain.ClientI) (bool, error) {
	if u.tipRepairHoldChecked {
		return false, nil
	}

	data, err := blockchainClient.GetState(ctx, model.OvertakenTipRepairHoldKey)
	if err != nil && !errors.Is(err, errors.ErrNotFound) && !strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
		return false, errors.NewServiceError("[repairOvertakenTips] failed to read the pruning hold", err)
	}

	u.tipRepairHoldChecked = true

	if len(data) == 0 {
		return false, nil
	}

	// One that cannot be decoded may list blocks: seed, which logs it, and write it again.
	held, err := model.DecodeOvertakenTipRepairHold(data)

	return err != nil || len(held) > 0, nil
}

// seedHeldBlocksLocked tracks the blocks a hold recorded before the restart lists. No record
// means a node that has never held pruning.
func (u *BlockValidation) seedHeldBlocksLocked(ctx context.Context, blockchainClient blockchain.ClientI) error {
	data, err := blockchainClient.GetState(ctx, model.OvertakenTipRepairHoldKey)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			return nil
		}

		return errors.NewServiceError("[repairOvertakenTips] seed: failed to read the pruning hold", err)
	}

	held, err := model.DecodeOvertakenTipRepairHold(data)
	if err != nil {
		// Unreadable, it protects nothing; the hold is written again from the tracker.
		u.logger.Errorf("[repairOvertakenTips] seed: ignoring an unreadable pruning hold: %v", err)
		return nil
	}

	for _, b := range held {
		u.minedOnChain.seed(b.Hash, b.ID, b.Height)
	}

	u.tipRepairHold = data
	u.tipRepairHoldWritten = true

	return nil
}

// mainChainHeaders returns the tipRepairWindow headers ending at best, best first, read in
// pages within blockchain_maxBlockHeadersPerRequest, which the blockchain service enforces
// and may be set below the window.
func (u *BlockValidation) mainChainHeaders(ctx context.Context, blockchainClient blockchain.ClientI, best chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	pageMax := uint64(tipRepairWindow)
	if limit := u.settings.BlockChain.MaxBlockHeadersPerRequest; limit > 0 {
		pageMax = min(pageMax, uint64(limit))
	}

	headers := make([]*model.BlockHeader, 0, tipRepairWindow)
	metas := make([]*model.BlockHeaderMeta, 0, tipRepairWindow)
	next := best

	for uint64(len(headers)) < tipRepairWindow {
		page := min(tipRepairWindow-uint64(len(headers)), pageMax)

		pageHeaders, pageMetas, err := blockchainClient.GetBlockHeaders(ctx, &next, page)
		if err != nil {
			return nil, nil, err
		}

		headers = append(headers, pageHeaders...)
		metas = append(metas, pageMetas...)

		if uint64(len(pageHeaders)) < page {
			break // reached genesis
		}

		prev := pageHeaders[len(pageHeaders)-1].HashPrevBlock
		if prev == nil || prev.IsEqual(&chainhash.Hash{}) {
			break
		}

		next = *prev
	}

	return headers, metas, nil
}

func (u *BlockValidation) trackMinedHeaders(headers []*model.BlockHeader, metas []*model.BlockHeaderMeta) {
	for i, meta := range metas {
		if i >= len(headers) || meta == nil || meta.Invalid {
			continue
		}

		// A block whose mined_set is not written too: a setTxMined that failed part way
		// may have cleared some of its txs.
		u.minedOnChain.seed(*headers[i].Hash(), meta.ID, meta.Height)
	}
}

// tipRepairSlice does one bounded step of the repair against the current best chain and
// reports whether more steps are pending. A step is one of: load the next subtree of the
// job in progress, or write the next slice of its txs; start a job, marking a re-marked
// block that is back on the chain mined again before repairing a block that left it.
// Housekeeping drops expired and invalid blocks.
//
// The repair is serialised with this process's setMined worker only. Several block
// validation instances writing setTxMined for the same chain would each run their own
// repair, unserialised against the others' writes. That race errs on the safe side: a
// mark(false) landing after another instance's setTxMined leaves the tx's block ids alone,
// so the tx is over-retained (flagged unmined while mined on the chain) until
// fixUnminedSinceInconsistencies clears it, never pruned while off the chain.
func (u *BlockValidation) tipRepairSlice(ctx context.Context, blockchainClient blockchain.ClientI) (more bool, err error) {
	u.tipRepairMu.Lock()
	defer u.tipRepairMu.Unlock()

	defer func() {
		prometheusBlockValidationTipRepairSubtreesSetAside.Set(float64(u.minedOnChain.setAsideSubtrees()))
	}()

	tracked, onMainChain, bestHeight, active, err := u.tipRepairHousekeepingLocked(ctx, blockchainClient)
	if err != nil || !active {
		return false, err
	}

	// The step may finish a block's repair, or set its subtrees aside: record the hold as it
	// stands after it.
	defer func() {
		if holdErr := u.writeRepairHoldLocked(ctx, blockchainClient); holdErr != nil && err == nil {
			more, err = true, holdErr
		}
	}()

	return u.tipRepairStepLocked(ctx, blockchainClient, tracked, onMainChain, bestHeight)
}

// refreshRepairHold brings the pruning hold up to date with the chain as it is now, without
// repairing anything: setTxMinedStatus calls it before setting a block's mined_set, which is
// what lets the pruner process that height. A block the chain has just overtaken is held
// before the pruner can see the new height, so the pruner deletes nothing until its repair
// has finished. It reads the chain from the blockchain service and writes the hold there, the
// same service SetBlockMinedSet writes to; it does no repair work, so a repair that fails does
// not stop healthy blocks from being set mined.
func (u *BlockValidation) refreshRepairHold(ctx context.Context, blockchainClient blockchain.ClientI) error {
	u.tipRepairMu.Lock()
	defer u.tipRepairMu.Unlock()

	if _, _, _, active, err := u.tipRepairHousekeepingLocked(ctx, blockchainClient); err != nil || !active {
		return err
	}

	return u.writeRepairHoldLocked(ctx, blockchainClient)
}

// tipRepairHousekeepingLocked seeds the tracker after a restart, reads the main chain, and
// drops or reopens tracked blocks as the chain has moved. active is false until this process
// has tracked a block, when there is nothing to do.
func (u *BlockValidation) tipRepairHousekeepingLocked(ctx context.Context, blockchainClient blockchain.ClientI) (tracked map[chainhash.Hash]minedOnChainBlock,
	onMainChain map[uint32]bool, bestHeight uint32, active bool, err error) {
	// Nothing to do until this process has written a block as on the longest chain, unless a
	// hold recorded before a restart lists blocks still to repair: the restart seed waits for
	// one or the other. After a restart the next block's setTxMined comes within one block
	// interval, and a process that never marks blocks mined (or a test harness that never runs
	// setTxMined) makes one state read here at most.
	if !u.tipRepairSeeded && u.minedOnChain.empty() {
		held, err := u.holdRecordedLocked(ctx, blockchainClient)
		if err != nil || !held {
			return nil, nil, 0, false, err
		}
	}

	if !u.tipRepairSeeded {
		if err = u.seedMinedOnChainTrackerLocked(ctx, blockchainClient); err != nil {
			return nil, nil, 0, false, err
		}
	}

	onMainChain, bestHeight, err = u.mainChainIDsLocked(ctx, blockchainClient)
	if err != nil {
		return nil, nil, 0, false, err
	}

	tracked = u.minedOnChain.snapshot()

	for hash, b := range tracked {
		switch {
		case tipRepairExpired(b.height, bestHeight):
			if !onMainChain[b.id] && !b.done {
				u.logger.Warnf("[repairOvertakenTips][%s] block left the repair window before its repair finished; some of its txs may still look mined", hash.String())
			}

			u.minedOnChain.drop(hash)
			delete(tracked, hash)

		case onMainChain[b.id] && b.pending != nil:
			// Back on the chain: if it leaves again, all of it needs repairing, and not behind
			// the other blocks for subtrees it could not read last time.
			u.minedOnChain.update(hash, b.gen, func(e *minedOnChainBlock) {
				e.clearPending()
				e.deferred = false
			})
			b.clearPending()
			b.deferred = false
			tracked[hash] = b

		case onMainChain[b.id] && b.deferred && !b.stamped:
			// On the chain with no remine owed: the job that failed is moot, and if the block
			// leaves again its repair must not wait behind every other block.
			u.minedOnChain.update(hash, b.gen, func(e *minedOnChainBlock) { e.deferred = false })
			b.deferred = false
			tracked[hash] = b

		case onMainChain[b.id] && b.done && !b.stamped:
			// Back on the chain and nothing was re-marked: nothing to undo, but if it
			// leaves again it needs repairing again.
			u.minedOnChain.update(hash, b.gen, func(e *minedOnChainBlock) { e.done = false })
			b.done = false
			tracked[hash] = b
		}
	}

	return tracked, onMainChain, bestHeight, true, nil
}

// tipRepairStepLocked does one step of the job in progress, or starts the next job.
func (u *BlockValidation) tipRepairStepLocked(ctx context.Context, blockchainClient blockchain.ClientI, tracked map[chainhash.Hash]minedOnChainBlock,
	onMainChain map[uint32]bool, bestHeight uint32) (more bool, err error) {
	if job := u.tipRepairJob; job != nil {
		b, ok := tracked[job.hash]

		switch {
		case !ok || b.gen != job.gen:
			u.tipRepairJob = nil // dropped, or rewritten by a new setTxMined
			return true, nil
		case job.kind == unmarkJob && onMainChain[b.id], job.kind == remineJob && !onMainChain[b.id]:
			u.tipRepairJob = nil // the chain moved; the next step starts the right job
			return true, nil
		}

		job.height = bestHeight

		if err = u.jobStep(ctx, blockchainClient, job, onMainChain); err == nil {
			job.failures = 0
			return true, nil
		}

		// A job that keeps failing is set aside, so it does not hold up the other blocks;
		// it starts over once they have had their turn.
		if job.failures++; job.failures >= tipRepairMaxAttempts {
			u.logger.Warnf("[repairOvertakenTips][%s] setting the repair aside after %d failed steps, other blocks go first", job.hash.String(), job.failures)
			u.minedOnChain.update(job.hash, job.gen, func(e *minedOnChainBlock) { e.deferred = true })
			u.tipRepairJob = nil
		}

		return true, err
	}

	// Blocks set aside after failing come last.
	for _, deferred := range []bool{false, true} {
		if more, started, err := u.startNextJob(ctx, blockchainClient, tracked, onMainChain, bestHeight, deferred); started {
			return more, err
		}
	}

	return false, nil
}

// startNextJob starts a job on the next block that needs one, among the blocks whose deferred
// flag matches, and reports whether it found one.
func (u *BlockValidation) startNextJob(ctx context.Context, blockchainClient blockchain.ClientI, tracked map[chainhash.Hash]minedOnChainBlock,
	onMainChain map[uint32]bool, bestHeight uint32, deferred bool) (more, started bool, err error) {
	// A re-marked block back on the chain first: its txs look unmined while mined.
	for hash, b := range tracked {
		if b.deferred == deferred && b.stamped && onMainChain[b.id] && !b.waiting(u.minedOnChain.now(), bestHeight) {
			return true, true, u.startJob(ctx, blockchainClient, remineJob, hash, b, bestHeight)
		}
	}

	for hash, b := range tracked {
		if b.deferred != deferred || b.done || onMainChain[b.id] || (b.pending != nil && b.waiting(u.minedOnChain.now(), bestHeight)) {
			continue
		}

		_, meta, err := blockchainClient.GetBlockHeader(ctx, &hash)
		if err != nil {
			return true, true, u.repairFailed(hash, b.gen, errors.NewServiceError("[repairOvertakenTips][%s] failed to get block header", hash.String(), err))
		}

		if meta == nil {
			return true, true, u.repairFailed(hash, b.gen, errors.NewServiceError("[repairOvertakenTips][%s] block header meta is nil", hash.String()))
		}

		if meta.Invalid {
			// block validation's unsetMined owns invalid blocks
			u.minedOnChain.drop(hash)
			continue
		}

		return true, true, u.startJob(ctx, blockchainClient, unmarkJob, hash, b, bestHeight)
	}

	return false, false, nil
}

// mainChainIDsLocked returns the ids of the tipRepairWindow blocks ending at the best block,
// and the best height. When the best block only extends the one seen last time, the cached
// set is extended with the new block's id instead of fetched again, so a step costs two
// small blockchain calls in steady state rather than a 1000-id fetch.
func (u *BlockValidation) mainChainIDsLocked(ctx context.Context, blockchainClient blockchain.ClientI) (map[uint32]bool, uint32, error) {
	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, 0, errors.NewServiceError("[repairOvertakenTips] failed to get best block header", err)
	}

	bestHash := *bestHeader.Hash()

	switch {
	case u.tipRepairMainChain != nil && bestHash == u.tipRepairBestHash:
		return u.tipRepairMainChain, bestMeta.Height, nil

	case u.tipRepairMainChain != nil && bestHeader.HashPrevBlock.IsEqual(&u.tipRepairBestHash):
		// GetBestBlockHeader's meta carries no block id over gRPC, so ask for it.
		ids, err := blockchainClient.GetBlockHeaderIDs(ctx, &bestHash, 1)
		if err != nil {
			return nil, 0, errors.NewServiceError("[repairOvertakenTips] failed to get best block id", err)
		}

		if len(ids) == 0 {
			return nil, 0, errors.NewServiceError("[repairOvertakenTips] no id for best block %s", bestHash.String())
		}

		// Ids that fell out of the window stay in the set; tipRepairExpired drops their
		// blocks before the set is consulted for them.
		u.tipRepairMainChain[ids[0]] = true
		u.tipRepairBestHash = bestHash
		return u.tipRepairMainChain, bestMeta.Height, nil
	}

	// A new chain, not an extension of the one seen last time: a reorg, or several blocks at
	// once. An unmark job leaves a tx mined while another block holding it is on the chain,
	// so every block on the chain must be tracked, to have its own repair when it leaves.
	// Blocks written on the chain by setTxMined are; one that reached the chain only through
	// a child, its own setTxMined having run while it was off the chain, is tracked here,
	// before the set is cached, so a failed read is retried in full.
	headers, metas, err := u.mainChainHeaders(ctx, blockchainClient, bestHash)
	if err != nil {
		return nil, 0, errors.NewServiceError("[repairOvertakenTips] failed to get main chain headers", err)
	}

	// No headers is a failed read, not a chain with no blocks: taken as one, every tracked
	// block would look off the chain.
	if len(metas) == 0 {
		return nil, 0, errors.NewServiceError("[repairOvertakenTips] no main chain headers for best block %s", bestHash.String())
	}

	u.trackMinedHeaders(headers, metas)

	onMainChain := make(map[uint32]bool, len(metas))
	for _, meta := range metas {
		if meta != nil {
			onMainChain[meta.ID] = true
		}
	}

	u.tipRepairMainChain = onMainChain
	u.tipRepairBestHash = bestHash

	return onMainChain, bestMeta.Height, nil
}

// startJob begins a job over the block: it reads the block's subtree list, not its txs.
func (u *BlockValidation) startJob(ctx context.Context, blockchainClient blockchain.ClientI, kind tipRepairJobKind, hash chainhash.Hash, b minedOnChainBlock, bestHeight uint32) error {
	block, err := blockchainClient.GetBlock(ctx, &hash)
	if err != nil {
		return u.repairFailed(hash, b.gen, errors.NewServiceError("[repairOvertakenTips][%s] failed to get block", hash.String(), err))
	}

	u.minedOnChain.update(hash, b.gen, func(e *minedOnChainBlock) {
		e.deferred = false

		// A remine reads the whole block. The unmark job's set-aside subtrees no longer
		// apply; the remine's own back-off, from an earlier remine, stays.
		if kind == remineJob {
			e.done = false

			if e.pending != nil {
				e.clearPending()
			}
		}
	})

	subtrees := block.Subtrees
	if kind == unmarkJob && b.pending != nil {
		subtrees = b.pending
	}

	u.tipRepairJob = &tipRepairJob{kind: kind, hash: hash, id: b.id, gen: b.gen, subtrees: subtrees, height: bestHeight}

	return nil
}

// jobStep writes the next slice of the job's txs, first loading the next subtree if the
// current one is used up, and finishes the job with its last slice. A subtree that is gone
// is given up and counted: nothing can name its txs again. One that still cannot be loaded
// after tipRepairMaxAttempts failed loads, whatever the error, is set aside, so the rest of
// the block is repaired: a run of short reads looks the same as a corrupt file, and a store
// that stays down must not hold up the later subtrees. A block with subtrees set aside is
// not done, so it holds pruning until they read. The block's next unmark job reads the set-aside subtrees
// again; a remine job with subtrees set aside leaves the block stamped, so a later remine
// does them (see finishJob). Until a subtree is set aside, a load error fails the step, to
// be retried.
func (u *BlockValidation) jobStep(ctx context.Context, blockchainClient blockchain.ClientI, job *tipRepairJob, onMainChain map[uint32]bool) error {
	for len(job.current) == 0 {
		if job.nextSubtree >= len(job.subtrees) {
			u.finishJob(job)
			return nil
		}

		subtreeHash := job.subtrees[job.nextSubtree]

		nodes, err := u.loadSubtreeNodes(ctx, subtreeHash)
		if err != nil {
			job.subtreeFailures++

			switch {
			case isPermanentRepairError(err):
				// Gone for good: nothing can name its txs again, so holding pruning for it would
				// only delay the loss. It is given up, as a gone block is (repairFailed).
				u.logger.Errorf("[repairOvertakenTips][%s] giving up subtree %s, it is gone; its txs may stay marked mined while off the longest chain: %v", job.hash.String(), subtreeHash.String(), err)
				prometheusBlockValidationTipRepairAbandoned.WithLabelValues("subtree").Inc()
			case ctx.Err() == nil && job.subtreeFailures >= tipRepairMaxAttempts:
				u.logger.Errorf("[repairOvertakenTips][%s] setting subtree %s aside after %d failed loads, it is read again later: %v", job.hash.String(), subtreeHash.String(), job.subtreeFailures, err)
				job.unreadable = append(job.unreadable, subtreeHash)
			default:
				return errors.NewProcessingError("[repairOvertakenTips][%s] failed to load subtree %d", job.hash.String(), job.nextSubtree, err)
			}
		}

		job.nextSubtree++
		job.subtreeFailures = 0
		job.current = nodes
	}

	sliceSize := u.settings.BlockValidation.TipRepairSliceSize
	if sliceSize <= 0 {
		sliceSize = defaultTipRepairSliceSize
	}

	sliceNodes := job.current[:min(sliceSize, len(job.current))]

	slice := make([]chainhash.Hash, 0, len(sliceNodes))
	for _, node := range sliceNodes {
		if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			slice = append(slice, node.Hash)
		}
	}

	switch job.kind {
	case unmarkJob:
		notOnLongestChain, err := model.FilterTxsNotOnLongestChain(ctx, u.logger, u.settings, u.utxoStore, blockchainClient, onMainChain, job.id, slice, true)
		if err != nil {
			return errors.NewProcessingError("[repairOvertakenTips][%s] error filtering txs of overtaken block", job.hash.String(), err)
		}

		if len(notOnLongestChain) > 0 {
			// Before writing, so a write that fails part way still counts as stamped.
			u.minedOnChain.update(job.hash, job.gen, func(e *minedOnChainBlock) { e.stamped = true })

			if err = u.utxoStore.MarkTransactionsOnLongestChain(ctx, notOnLongestChain, false, utxo.WithSkipMissing()); err != nil {
				return errors.NewProcessingError("[repairOvertakenTips][%s] error marking txs of overtaken block as not on longest chain", job.hash.String(), err)
			}
		}

		job.marked += len(notOnLongestChain)

	case remineJob:
		// Only the txs that were stamped need writing, and only those still in this block: a
		// tx pruned and created again while the block was off the chain is in no block.
		unmined, err := model.FilterTxsUnmined(ctx, u.logger, u.settings, u.utxoStore, job.id, slice)
		if err != nil {
			return errors.NewProcessingError("[repairOvertakenTips][%s] error reading txs of block back on the longest chain", job.hash.String(), err)
		}

		if len(unmined) > 0 {
			if err = u.utxoStore.MarkTransactionsOnLongestChain(ctx, unmined, true, utxo.WithSkipMissing()); err != nil {
				return errors.NewProcessingError("[repairOvertakenTips][%s] error marking txs of block back on the longest chain", job.hash.String(), err)
			}
		}

		job.marked += len(unmined)
	}

	job.total += len(slice)
	job.current = job.current[len(sliceNodes):]

	if len(job.current) == 0 && job.nextSubtree >= len(job.subtrees) {
		u.finishJob(job)
	}

	return nil
}

// finishJob records a completed job on its block.
func (u *BlockValidation) finishJob(job *tipRepairJob) {
	u.tipRepairJob = nil

	switch job.kind {
	case unmarkJob:
		// A block with subtrees set aside is not done: its next job reads them again,
		// after the other blocks have had their turn, and once twice as long as last time
		// has passed or a new block has arrived, whichever comes first.
		passes := 0
		u.minedOnChain.update(job.hash, job.gen, func(e *minedOnChainBlock) {
			e.done = len(job.unreadable) == 0
			e.pending = job.unreadable
			e.deferred = !e.done
			e.failures = 0

			if e.done {
				e.passes = 0
				e.nextPass = time.Time{}
				e.passHeight = 0
			} else {
				u.backOffPass(e, job)
			}

			passes = e.passes
		})

		switch {
		case job.marked > 0:
			u.logger.Warnf("[repairOvertakenTips][%s] block left the longest chain after its txs were marked mined, marked %d of %d txs as not on longest chain", job.hash.String(), job.marked, job.total)
		case len(job.unreadable) > 0 && passes == 1:
			u.logger.Warnf("[repairOvertakenTips][%s] block left the longest chain, %d of its subtrees could not be read and are set aside, their txs may look mined until a later pass reads them", job.hash.String(), len(job.unreadable))
		default:
			// Such as a fork block the restart seed tracks that was never on the chain, or a
			// later pass over subtrees that still cannot be read.
			u.logger.Infof("[repairOvertakenTips][%s] block is off the longest chain, marked %d of %d txs as not on longest chain, %d subtrees set aside", job.hash.String(), job.marked, job.total, len(job.unreadable))
		}

	case remineJob:
		// A remine that set subtrees aside leaves the block stamped: their re-mine is still
		// owed, and a later remine of the block does it, backed off like an unmark pass.
		u.minedOnChain.update(job.hash, job.gen, func(e *minedOnChainBlock) {
			e.failures = 0

			if len(job.unreadable) > 0 {
				e.deferred = true
				u.backOffPass(e, job)

				return
			}

			e.stamped = false
			e.passes = 0
			e.nextPass = time.Time{}
			e.passHeight = 0
		})

		u.logger.Infof("[repairOvertakenTips][%s] block is back on the longest chain, marked %d of %d txs mined again, %d subtrees set aside", job.hash.String(), job.marked, job.total, len(job.unreadable))
	}
}

// backOffPass records a pass that ended with subtrees set aside: the next is due twice as
// long after it as the last one, up to tipRepairPendingMaxBackoff, or once a block arrives
// after it.
func (u *BlockValidation) backOffPass(e *minedOnChainBlock, job *tipRepairJob) {
	e.passes++
	e.nextPass = u.minedOnChain.now().Add(min(tipRepairPendingBaseBackoff<<min(e.passes-1, 30), tipRepairPendingMaxBackoff))
	e.passHeight = job.height
}

// waiting reports whether the block's next pass over subtrees set aside is not yet due.
func (b *minedOnChainBlock) waiting(now time.Time, bestHeight uint32) bool {
	return b.passes > 0 && now.Before(b.nextPass) && bestHeight <= b.passHeight
}

// repairFailed records a failed step for a block and returns err. A permanent failure (the
// block or its subtrees are gone) counts toward tipRepairMaxAttempts, after which the block
// is dropped; a transient one is only retried.
func (u *BlockValidation) repairFailed(hash chainhash.Hash, gen uint64, err error) error {
	if !isPermanentRepairError(err) {
		return err
	}

	failures := 0
	u.minedOnChain.update(hash, gen, func(e *minedOnChainBlock) {
		e.failures++
		failures = e.failures
	})

	if failures >= tipRepairMaxAttempts {
		u.logger.Errorf("[repairOvertakenTips][%s] giving up after %d failed attempts; its txs may still look mined: %v", hash.String(), tipRepairMaxAttempts, err)
		prometheusBlockValidationTipRepairAbandoned.WithLabelValues("block").Inc()
		u.minedOnChain.drop(hash)

		if u.tipRepairJob != nil && u.tipRepairJob.hash == hash {
			u.tipRepairJob = nil
		}
	}

	return err
}

// loadSubtreeNodes returns the nodes of one subtree, read like setTxMined reads it
// (Block.GetSubtrees): from the .subtree file, or the .subtreeToCheck file when that is
// missing. A read that fails, for example truncated while another service moves the file, is
// tried again, and an error that outlasts the attempts is left for the next step to retry.
func (u *BlockValidation) loadSubtreeNodes(ctx context.Context, subtreeHash *chainhash.Hash) ([]subtreepkg.Node, error) {
	var err error

	for attempt := 0; attempt < tipRepairSubtreeReadAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(tipRepairSubtreeReadBackoff):
			}
		}

		var nodes []subtreepkg.Node
		if nodes, err = u.readSubtreeNodes(ctx, subtreeHash); err == nil {
			return nodes, nil
		}

		if subtreeAbsent(err) {
			return nil, err
		}
	}

	// Not a permanent error, even when the subtree still cannot be deserialized: a short
	// read looks the same as a corrupt file. The step is retried, and jobStep sets the
	// subtree aside only after it has failed for tipRepairMaxAttempts steps.
	return nil, err
}

func (u *BlockValidation) readSubtreeNodes(ctx context.Context, subtreeHash *chainhash.Hash) ([]subtreepkg.Node, error) {
	reader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if subtreeAbsent(err) {
		reader, err = u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		_ = reader.Close()
	}()

	subtree, err := subtreepkg.NewSubtreeFromReader(bufio.NewReader(reader))
	if err != nil {
		return nil, errors.NewSubtreeInvalidError("failed to deserialize subtree %s", subtreeHash.String(), err)
	}

	return subtree.Nodes, nil
}
