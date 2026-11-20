// Package repairmissingparents rebuilds UTXO-store records that were pruned while a
// child transaction still referenced them (issue 1768), using a healthy peer's asset
// service as the source of truth.
//
// For each missing parent the tool fetches the transaction, its mined-state metadata
// and the per-output spent state from the peer, maps the peer's block hashes to local
// blocks on the current chain, recreates the record as mined, and replays every
// recorded spend with the real spending transaction so the store's UTXO-hash check runs
// against the actual locking script and value.
//
// A record is written only when every stored output has an unambiguous peer state (OK,
// IMMATURE, or SPENT with spending data) and every spender's other inputs are already
// recorded locally, so the replay changes nothing outside the rebuilt record. A record
// whose replay or read-back fails is deleted again. A record that is already present is
// compared with the peer and reported when it differs.
package repairmissingparents

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// Options controls one repair run.
type Options struct {
	// PeerURL is the healthy peer's asset service base URL (scheme://host[:port]).
	PeerURL string
	// TxIDs are parent hashes the operator already knows are missing.
	TxIDs []chainhash.Hash
	// Scan walks every unmined transaction in the store and queues any parent it
	// references that is absent.
	Scan bool
	// DryRun prints the plan and writes nothing.
	DryRun bool
	// ForceLive proceeds when the node's FSM is not IDLE.
	ForceLive bool
	// Stdout receives the report. Defaults to os.Stdout.
	Stdout io.Writer
}

// Outcome is the result for one parent record.
type Outcome string

const (
	OutcomeRepaired            Outcome = "repaired"
	OutcomePlanned             Outcome = "planned"
	OutcomeAlreadyPresent      Outcome = "already present"
	OutcomePresentMismatch     Outcome = "present but differs from peer"
	OutcomePeerPruned          Outcome = "peer has pruned it"
	OutcomePeerStateUnusable   Outcome = "peer output state unusable"
	OutcomeChainMismatch       Outcome = "peer block not on our chain"
	OutcomeUnsafeSpender       Outcome = "spender touches outputs outside the repair"
	OutcomeInconsistentSpender Outcome = "store records a different spender"
	OutcomeFailed              Outcome = "failed"
)

const (
	statusOK       = "OK"
	statusSpent    = "SPENT"
	statusImmature = "IMMATURE"
	statusNotFound = "NOT_FOUND"
)

// PlannedSpend is one output the peer records as spent.
type PlannedSpend struct {
	Vout    uint32
	Spender chainhash.Hash
	Vin     int
}

// Result is the report line for one parent.
type Result struct {
	Parent       chainhash.Hash
	Outcome      Outcome
	Detail       string
	BlockHeights []uint32
	BlockIDs     []uint32
	Spends       []PlannedSpend
}

// Report is the outcome of a run.
type Report struct {
	// Scanned is the number of unmined transactions walked when Options.Scan is set.
	Scanned int
	Results []Result
}

// Failed reports whether any parent ended in a state other than repaired, planned
// or already present.
func (r *Report) Failed() bool {
	for _, res := range r.Results {
		switch res.Outcome {
		case OutcomeRepaired, OutcomePlanned, OutcomeAlreadyPresent:
		default:
			return true
		}
	}

	return false
}

// Write prints the report as a table.
func (r *Report) Write(w io.Writer) {
	writeResults(w, r.Results)

	if r.Scanned > 0 {
		fmt.Fprintf(w, "scanned %d unmined transactions\n", r.Scanned)
	}
}

func writeResults(w io.Writer, results []Result) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PARENT\tOUTCOME\tBLOCKS (height->id)\tSPENT OUTPUTS\tDETAIL")

	for _, res := range results {
		blocks := make([]string, 0, len(res.BlockHeights))

		for i, h := range res.BlockHeights {
			if i < len(res.BlockIDs) {
				blocks = append(blocks, fmt.Sprintf("%d->%d", h, res.BlockIDs[i]))
			} else {
				blocks = append(blocks, fmt.Sprintf("%d->?", h))
			}
		}

		spends := make([]string, 0, len(res.Spends))

		for _, s := range res.Spends {
			spends = append(spends, fmt.Sprintf("%d<-%s[%d]", s.Vout, s.Spender.String(), s.Vin))
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", res.Parent.String(), res.Outcome, strings.Join(blocks, ","), strings.Join(spends, " "), res.Detail)
	}

	_ = tw.Flush()
}

// Repairer performs the repair against a store, a blockchain client and a peer.
type Repairer struct {
	logger        ulogger.Logger
	store         utxo.Store
	chain         blockchain.ClientI
	peer          Peer
	opts          Options
	genesisHeight uint32
}

// NewRepairer wires the dependencies. Run builds them from settings; tests inject mocks.
func NewRepairer(logger ulogger.Logger, tSettings *settings.Settings, store utxo.Store, chain blockchain.ClientI, peer Peer, opts Options) *Repairer {
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}

	return &Repairer{
		logger:        logger,
		store:         store,
		chain:         chain,
		peer:          peer,
		opts:          opts,
		genesisHeight: tSettings.ChainCfgParams.GenesisActivationHeight,
	}
}

// Run builds the store, blockchain client and peer from settings and performs the repair.
// The blockchain service must be running: the tool reads the FSM state and resolves
// block hashes through it.
func Run(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, opts Options) (*Report, error) {
	if opts.PeerURL == "" {
		return nil, errors.NewConfigurationError("a peer asset URL is required (--peer)")
	}

	chain, err := blockchain.NewClient(ctx, logger, tSettings, "repair-missing-parents")
	if err != nil {
		return nil, errors.NewServiceError("failed to reach the blockchain service; it must be running with the FSM in IDLE", err)
	}

	store, err := utxofactory.NewStore(ctx, logger, tSettings, "repair-missing-parents", false)
	if err != nil {
		return nil, errors.NewStorageError("failed to open utxo store", err)
	}

	return NewRepairer(logger, tSettings, store, chain, NewHTTPPeer(opts.PeerURL, nil), opts).Run(ctx)
}

// plan is everything needed to write one parent, computed before any write.
type plan struct {
	res          Result
	tx           *bt.Tx
	minedInfos   []utxo.MinedBlockInfo
	createHeight uint32
	spenders     []*bt.Tx
	stale        []*aerospike.Key
}

// Run plans every parent first, prints the plan, and then writes only the parents
// whose plan succeeded. Nothing is written when Options.DryRun is set.
func (r *Repairer) Run(ctx context.Context) (*Report, error) {
	if err := r.gate(ctx); err != nil {
		return nil, err
	}

	currentHeight, _, err := r.chain.GetBestHeightAndTime(ctx)
	if err != nil {
		return nil, errors.NewServiceError("failed to read best height", err)
	}

	if err := r.store.SetBlockHeight(currentHeight); err != nil {
		return nil, errors.NewStorageError("failed to set store block height", err)
	}

	report := &Report{}

	if r.opts.PeerURL != "" {
		fmt.Fprintf(r.opts.Stdout, "source peer: %s\n", r.opts.PeerURL)
	}

	if r.opts.DryRun {
		fmt.Fprintln(r.opts.Stdout, "dry run: nothing will be written")
	}

	plans, scanned, err := r.planAll(ctx)
	if err != nil {
		return nil, err
	}

	report.Scanned = scanned

	sort.Slice(plans, func(i, j int) bool {
		return plans[i].res.Parent.String() < plans[j].res.Parent.String()
	})

	if !r.opts.DryRun && len(plans) > 0 {
		planned := make([]Result, 0, len(plans))
		for _, p := range plans {
			planned = append(planned, p.res)
		}

		fmt.Fprintln(r.opts.Stdout, "plan:")
		writeResults(r.opts.Stdout, planned)
		fmt.Fprintln(r.opts.Stdout, "result:")
	}

	for _, p := range plans {
		if !r.opts.DryRun && p.res.Outcome == OutcomePlanned {
			p.res = r.write(ctx, p, currentHeight)
		}

		report.Results = append(report.Results, p.res)
	}

	report.Write(r.opts.Stdout)

	return report, nil
}

// planAll walks the queue of parents and plans each one. A spender that is unmined
// here and references a further absent parent queues that parent too.
func (r *Repairer) planAll(ctx context.Context) ([]*plan, int, error) {
	queue := make([]chainhash.Hash, 0, len(r.opts.TxIDs))
	queued := make(map[chainhash.Hash]struct{}, len(r.opts.TxIDs))

	enqueue := func(h chainhash.Hash) {
		if _, ok := queued[h]; ok {
			return
		}

		queued[h] = struct{}{}
		queue = append(queue, h)
	}

	for _, h := range r.opts.TxIDs {
		enqueue(h)
	}

	scanned := 0

	if r.opts.Scan {
		missing, n, err := r.scanUnmined(ctx)
		if err != nil {
			return nil, n, err
		}

		scanned = n

		for _, h := range missing {
			enqueue(h)
		}
	}

	plans := make([]*plan, 0, len(queue))

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		p, more := r.planOne(ctx, parent)
		plans = append(plans, p)

		for _, h := range more {
			enqueue(h)
		}
	}

	return plans, scanned, nil
}

// gate refuses to run against a live node unless ForceLive is set. Block validation
// and the repair would otherwise race on the same records.
func (r *Repairer) gate(ctx context.Context) error {
	state, err := r.chain.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewServiceError("failed to read FSM state", err)
	}

	if state == nil {
		return errors.NewServiceError("blockchain service returned no FSM state; cannot confirm the node is IDLE")
	}

	if *state == blockchain.FSMStateIDLE {
		return nil
	}

	if r.opts.ForceLive {
		r.logger.Warnf("[repair-missing-parents] FSM state is %s but --force-live given; proceeding", state.String())
		return nil
	}

	return errors.NewProcessingError("FSM state is %s, expected IDLE; run 'teranode-cli setfsmstate --fsmstate idle' first or pass --force-live", state.String())
}

// scanUnmined returns the parents of unmined transactions that are absent from the store.
func (r *Repairer) scanUnmined(ctx context.Context) ([]chainhash.Hash, int, error) {
	iter, err := r.store.GetUnminedTxIterator()
	if err != nil {
		return nil, 0, errors.NewStorageError("failed to get unmined tx iterator", err)
	}
	defer func() { _ = iter.Close() }()

	checked := make(map[chainhash.Hash]bool)
	missing := make([]chainhash.Hash, 0)
	scanned := 0

	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			return nil, scanned, errors.NewStorageError("failed to iterate unmined transactions", err)
		}

		if batch == nil {
			break
		}

		for _, tx := range batch {
			if tx == nil || tx.Skip || tx.TxInpoints == nil {
				continue
			}

			scanned++

			for _, parent := range tx.TxInpoints.ParentTxHashes {
				if present, seen := checked[parent]; seen {
					if !present {
						r.logger.Warnf("[repair-missing-parents] unmined %s also references missing parent %s", tx.Hash.String(), parent.String())
					}

					continue
				}

				present, err := r.exists(ctx, parent)
				if err != nil {
					return nil, scanned, err
				}

				checked[parent] = present

				if !present {
					r.logger.Warnf("[repair-missing-parents] unmined %s references missing parent %s", tx.Hash.String(), parent.String())
					missing = append(missing, parent)
				}
			}
		}
	}

	return missing, scanned, nil
}

// lookup returns the local record for hash, or nil when the store does not hold it.
func (r *Repairer) lookup(ctx context.Context, hash chainhash.Hash, fieldNames ...fields.FieldName) (*meta.Data, error) {
	md, err := r.store.Get(ctx, &hash, fieldNames...)
	if err == nil {
		return md, nil
	}

	if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
		return nil, nil
	}

	return nil, errors.NewStorageError("failed to look up %s", hash.String(), err)
}

func (r *Repairer) exists(ctx context.Context, hash chainhash.Hash) (bool, error) {
	md, err := r.lookup(ctx, hash, fields.BlockIDs)
	return md != nil, err
}

// peerView is the peer's account of one parent, checked against our chain.
type peerView struct {
	tx           *bt.Tx
	minedInfos   []utxo.MinedBlockInfo
	createHeight uint32
}

// readPeer fetches the parent from the peer and fills res with the blocks and spends
// it would be rebuilt with. A non-empty outcome means the parent cannot be rebuilt.
func (r *Repairer) readPeer(ctx context.Context, parent chainhash.Hash, res *Result) (*peerView, Outcome) {
	tx, err := r.peer.GetTx(ctx, &parent)
	if err != nil {
		return nil, r.peerFailure(res, err)
	}

	if !tx.IsExtended() {
		res.Detail = "parent from peer is not extended; cannot rebuild its inputs and fee"
		return nil, OutcomeFailed
	}

	pm, err := r.peer.GetTxMeta(ctx, &parent)
	if err != nil {
		return nil, r.peerFailure(res, err)
	}

	utxos, err := r.peer.GetUTXOs(ctx, &parent)
	if err != nil {
		return nil, r.peerFailure(res, err)
	}

	if len(pm.BlockHeights) == 0 {
		res.Detail = "peer holds the transaction as unmined; nothing to rebuild as a mined parent"
		return nil, OutcomeFailed
	}

	createHeight := pm.BlockHeights[0]

	minedInfos, notes, err := r.mapBlocks(ctx, pm)
	if err != nil {
		res.BlockHeights = pm.BlockHeights
		res.Detail = err.Error()

		return nil, OutcomeChainMismatch
	}

	for _, info := range minedInfos {
		res.BlockHeights = append(res.BlockHeights, info.BlockHeight)
		res.BlockIDs = append(res.BlockIDs, info.BlockID)
	}

	res.Detail = strings.Join(notes, "; ")

	spends, err := r.plannedSpends(tx, utxos, createHeight)
	if err != nil {
		res.Detail = err.Error()
		return nil, OutcomePeerStateUnusable
	}

	res.Spends = spends

	return &peerView{tx: tx, minedInfos: minedInfos, createHeight: createHeight}, ""
}

// planOne decides what to do with one parent without writing anything. It returns the
// plan and any further absent parents that unmined spenders reference.
func (r *Repairer) planOne(ctx context.Context, parent chainhash.Hash) (*plan, []chainhash.Hash) {
	p := &plan{res: Result{Parent: parent}}

	present, err := r.exists(ctx, parent)
	if err != nil {
		p.res.Outcome, p.res.Detail = OutcomeFailed, err.Error()
		return p, nil
	}

	view, outcome := r.readPeer(ctx, parent, &p.res)
	if outcome != "" {
		p.res.Outcome = outcome
		return p, nil
	}

	if present {
		// Presence alone proves nothing: an interrupted earlier run can leave a record
		// with outputs unspent that the peer records as spent.
		if err := r.verify(ctx, parent, p.res, len(view.tx.Outputs), false); err != nil {
			p.res.Outcome, p.res.Detail = OutcomePresentMismatch, err.Error()
			return p, nil
		}

		p.res.Outcome = OutcomeAlreadyPresent

		return p, nil
	}

	spenders, more, outcome, err := r.checkSpenders(ctx, view.tx, p.res.Spends)
	if err != nil {
		p.res.Outcome, p.res.Detail = outcome, err.Error()
		return p, more
	}

	stale, err := r.findStaleChildRecords(parent, len(view.tx.Outputs))
	if err != nil {
		p.res.Outcome, p.res.Detail = OutcomeFailed, err.Error()
		return p, more
	}

	if len(stale) > 0 {
		verb := "removed"
		if r.opts.DryRun {
			verb = "would be removed"
		}

		p.res.Detail = joinDetail(p.res.Detail, fmt.Sprintf("%d stale pagination record(s) %s before rebuild", len(stale), verb))
	}

	p.res.Outcome = OutcomePlanned
	p.tx = view.tx
	p.minedInfos = view.minedInfos
	p.createHeight = view.createHeight
	p.spenders = spenders
	p.stale = stale

	return p, more
}

// write creates the parent, replays its spends and reads it back. Any failure after
// the create deletes the record again, so a rerun never finds a partial rebuild.
func (r *Repairer) write(ctx context.Context, p *plan, currentHeight uint32) Result {
	res := p.res
	parent := res.Parent

	if err := r.deleteStaleChildRecords(p.stale); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
		return res
	}

	// WithCreateOnly: the parent's own inputs were spent long ago and its parents may be
	// legitimately pruned; only the record itself is rebuilt. The coinbase flag is
	// derived from the transaction by the store, never taken from the peer.
	createOpts := []utxo.CreateOption{
		utxo.WithCreateOnly(),
		utxo.WithMinedBlockInfo(p.minedInfos...),
		utxo.WithTXID(&parent),
	}

	if _, _, err := r.store.SpendAndCreate(ctx, p.tx, p.createHeight, createOpts...); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("create: %v", err)
		return res
	}

	for _, spender := range p.spenders {
		// WithSpendOnly spends every input of the spender. checkSpenders proved that each
		// input outside this parent is already recorded as spent by this spender (an
		// idempotent re-spend) or belongs to an absent parent of a spender we hold (the
		// store's already-blessed fallback), so only this parent's outputs change.
		_, _, err := r.store.SpendAndCreate(ctx, spender, currentHeight, utxo.WithSpendOnly())
		if err == nil {
			continue
		}

		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("spend by %s: %v", spender.TxID(), err)

		if errors.Is(err, errors.ErrSpent) {
			res.Outcome = OutcomeInconsistentSpender
		}

		return r.rollback(ctx, res)
	}

	if err := r.verify(ctx, parent, res, len(p.tx.Outputs), true); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("verify: %v", err)
		return r.rollback(ctx, res)
	}

	res.Outcome = OutcomeRepaired

	return res
}

// rollback deletes a parent whose rebuild failed after the create, including any
// pagination records, and records the result in res.Detail.
func (r *Repairer) rollback(ctx context.Context, res Result) Result {
	if err := r.store.DeleteComplete(ctx, &res.Parent); err != nil {
		r.logger.Errorf("[repair-missing-parents] %s: rollback failed, record left partial: %v", res.Parent.String(), err)
		res.Detail = joinDetail(res.Detail, fmt.Sprintf("ROLLBACK FAILED, record left partial: %v", err))

		return res
	}

	res.Detail = joinDetail(res.Detail, "record rolled back")

	return res
}

func joinDetail(a, b string) string {
	if a == "" {
		return b
	}

	return a + "; " + b
}

func (r *Repairer) peerFailure(res *Result, err error) Outcome {
	res.Detail = err.Error()

	if errors.Is(err, errors.ErrNotFound) {
		return OutcomePeerPruned
	}

	return OutcomeFailed
}

// mapBlocks resolves the peer's blocks to local blocks on our current chain. The block
// the peer marks as its main-chain entry must exist here at the same height, be valid
// and be on our current chain, or the parent is not rebuilt: a peer on a different
// chain cannot inject its block IDs. Other entries (fork blocks) are kept only when
// they meet the same test and are dropped otherwise, so every entry written is
// genuinely on the longest chain. It returns notes on dropped entries.
func (r *Repairer) mapBlocks(ctx context.Context, pm *peerTxMeta) ([]utxo.MinedBlockInfo, []string, error) {
	n := len(pm.BlockHeights)

	if len(pm.BlockHashes) != n || len(pm.SubtreeIdxs) != n {
		return nil, nil, errors.NewProcessingError("peer returned %d block hashes and %d subtree indexes for %d heights", len(pm.BlockHashes), len(pm.SubtreeIdxs), n)
	}

	if pm.MainChainIndex == nil {
		return nil, nil, errors.NewProcessingError("peer did not report mainChainIndex; it is too old to use as a source")
	}

	mainIdx := *pm.MainChainIndex
	if mainIdx < 0 || mainIdx >= n {
		return nil, nil, errors.NewProcessingError("peer could not place the transaction on its main chain (mainChainIndex %d)", mainIdx)
	}

	infos := make([]utxo.MinedBlockInfo, 0, n)
	notes := make([]string, 0)

	for i, height := range pm.BlockHeights {
		if pm.SubtreeIdxs[i] < 0 {
			return nil, nil, errors.NewProcessingError("peer returned subtree index %d at height %d", pm.SubtreeIdxs[i], height)
		}

		info, reason, err := r.resolveBlock(ctx, pm.BlockHashes[i], height, pm.SubtreeIdxs[i])
		if err != nil {
			return nil, nil, err
		}

		if reason != "" {
			if i == mainIdx {
				return nil, nil, errors.NewProcessingError("peer's main-chain block at height %d: %s", height, reason)
			}

			notes = append(notes, fmt.Sprintf("dropped peer block at height %d: %s", height, reason))

			continue
		}

		infos = append(infos, *info)
	}

	return infos, notes, nil
}

// resolveBlock maps one peer block to a local block on our current chain. A non-empty
// reason means the block is not usable; an error means the peer data is inconsistent.
func (r *Repairer) resolveBlock(ctx context.Context, hashStr string, height uint32, subtreeIdx int) (*utxo.MinedBlockInfo, string, error) {
	if hashStr == "" {
		return nil, "peer could not resolve the block hash", nil
	}

	blockHash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return nil, "", errors.NewProcessingError("peer block hash %q at height %d", hashStr, height, err)
	}

	_, bm, err := r.chain.GetBlockHeader(ctx, blockHash)
	if err != nil || bm == nil {
		return nil, fmt.Sprintf("block %s is not in our blockchain", blockHash.String()), nil
	}

	if bm.Height != height {
		return nil, "", errors.NewProcessingError("peer block %s is at height %d here, peer says %d", blockHash.String(), bm.Height, height)
	}

	if bm.Invalid {
		return nil, fmt.Sprintf("block %s is marked invalid here", blockHash.String()), nil
	}

	onChain, err := r.chain.CheckBlockIsInCurrentChain(ctx, []uint32{bm.ID})
	if err != nil {
		return nil, "", errors.NewServiceError("check block %s is on our current chain", blockHash.String(), err)
	}

	if !onChain {
		return nil, fmt.Sprintf("block %s is not on our current chain", blockHash.String()), nil
	}

	return &utxo.MinedBlockInfo{BlockID: bm.ID, BlockHeight: height, SubtreeIdx: subtreeIdx, OnLongestChain: true}, "", nil
}

// plannedSpends checks the peer's per-output state and returns the outputs it records
// as spent. Every output must appear exactly once. A stored output is accepted as
// unspent only on OK or IMMATURE and as spent only on SPENT with spending data; any
// other state (FROZEN, CONFLICTING, LOCKED, or NOT_FOUND, which the asset service also
// reports when its own lookup fails) aborts the parent. An output the store never keeps
// as a UTXO (provably unspendable) may be OK or NOT_FOUND.
func (r *Repairer) plannedSpends(tx *bt.Tx, utxos []peerUTXO, createHeight uint32) ([]PlannedSpend, error) {
	if len(utxos) != len(tx.Outputs) {
		return nil, errors.NewProcessingError("peer returned state for %d outputs, transaction has %d", len(utxos), len(tx.Outputs))
	}

	byVout := make([]*peerUTXO, len(tx.Outputs))

	for i := range utxos {
		u := &utxos[i]

		if int(u.Vout) >= len(tx.Outputs) || byVout[u.Vout] != nil {
			return nil, errors.NewProcessingError("peer returned a duplicate or out-of-range output %d", u.Vout)
		}

		byVout[u.Vout] = u
	}

	spends := make([]PlannedSpend, 0)
	problems := make([]string, 0)

	for vout, u := range byVout {
		stored := utxo.ShouldStoreOutputAsUTXO(tx.Outputs[vout], createHeight, r.genesisHeight)

		switch {
		case !stored && (u.Status == statusOK || u.Status == statusNotFound):
		case !stored:
			problems = append(problems, fmt.Sprintf("output %d is unspendable but peer reports %s", vout, u.Status))
		case u.Status == statusOK || u.Status == statusImmature:
		case u.Status == statusSpent && u.SpendingData != nil && u.SpendingData.TxID != nil:
			spends = append(spends, PlannedSpend{Vout: u.Vout, Spender: *u.SpendingData.TxID, Vin: u.SpendingData.Vin})
		case u.Status == statusSpent:
			problems = append(problems, fmt.Sprintf("output %d is SPENT without spending data", vout))
		default:
			problems = append(problems, fmt.Sprintf("output %d is %s", vout, u.Status))
		}
	}

	if len(problems) > 0 {
		return nil, errors.NewProcessingError("peer output state: %s", strings.Join(problems, ", "))
	}

	return spends, nil
}

// checkSpenders loads each distinct spender from the peer and proves that replaying it
// changes nothing outside this parent. Each spender input on this parent must match the
// plan. Every other input must already be recorded here as spent by this spender at
// that vin, or belong to an absent parent of a spender we hold, which the store's
// already-blessed fallback skips without a write. An absent parent of a spender that is
// unmined here is a further hole and is returned for queueing; one of a mined spender
// was pruned by design and is ignored.
func (r *Repairer) checkSpenders(ctx context.Context, parentTx *bt.Tx, spends []PlannedSpend) ([]*bt.Tx, []chainhash.Hash, Outcome, error) {
	parent := *parentTx.TxIDChainHash()

	planned := make(map[uint32]PlannedSpend, len(spends))
	perSpender := make(map[chainhash.Hash]int, len(spends))

	for _, s := range spends {
		planned[s.Vout] = s
		perSpender[s.Spender]++
	}

	seen := make(map[chainhash.Hash]struct{}, len(spends))
	spenders := make([]*bt.Tx, 0, len(spends))
	more := make([]chainhash.Hash, 0)

	for _, s := range spends {
		if _, ok := seen[s.Spender]; ok {
			continue
		}

		seen[s.Spender] = struct{}{}

		spenderHash := s.Spender

		tx, err := r.peer.GetTx(ctx, &spenderHash)
		if err != nil {
			return nil, more, OutcomeFailed, errors.NewProcessingError("spender %s of output %d", spenderHash.String(), s.Vout, err)
		}

		if !tx.IsExtended() {
			return nil, more, OutcomeFailed, errors.NewProcessingError("spender %s from peer is not extended; cannot compute utxo hashes", spenderHash.String())
		}

		local, err := r.lookup(ctx, spenderHash, fields.BlockIDs)
		if err != nil {
			return nil, more, OutcomeFailed, err
		}

		matched := 0

		for vin, in := range tx.Inputs {
			prev := *in.PreviousTxIDChainHash()

			if prev.IsEqual(&parent) {
				ps, ok := planned[in.PreviousTxOutIndex]
				if !ok || !ps.Spender.IsEqual(&spenderHash) || ps.Vin != vin {
					return nil, more, OutcomeUnsafeSpender, errors.NewProcessingError("spender %s input %d spends output %d, which the peer does not record as spent by it there", spenderHash.String(), vin, in.PreviousTxOutIndex)
				}

				// The txid does not cover the extended input data. A spender whose input
				// disagrees with the parent's output would fail the store's hash check, and
				// the store's rollback of that failure would unspend this spender's other,
				// already-recorded inputs.
				out := parentTx.Outputs[in.PreviousTxOutIndex]
				if in.PreviousTxSatoshis != out.Satoshis || in.PreviousTxScript == nil || !bytes.Equal(*in.PreviousTxScript, *out.LockingScript) {
					return nil, more, OutcomeUnsafeSpender, errors.NewProcessingError("spender %s input %d carries a locking script or value that differs from parent output %d", spenderHash.String(), vin, in.PreviousTxOutIndex)
				}

				matched++

				continue
			}

			absent, err := r.checkOtherInput(ctx, spenderHash, vin, in)
			if err != nil {
				return nil, more, OutcomeUnsafeSpender, err
			}

			if !absent {
				continue
			}

			if local == nil {
				return nil, more, OutcomeUnsafeSpender, errors.NewProcessingError("spender %s is not held here and its input %d spends absent parent %s; the replay cannot run", spenderHash.String(), vin, prev.String())
			}

			if len(local.BlockIDs) == 0 {
				more = append(more, prev)
			}
		}

		// Every input on this parent matched a distinct planned spend; a planned spend
		// left over is one this spender does not make.
		if matched != perSpender[spenderHash] {
			return nil, more, OutcomeUnsafeSpender, errors.NewProcessingError("peer records %d outputs as spent by %s, but it spends %d of them", perSpender[spenderHash], spenderHash.String(), matched)
		}

		spenders = append(spenders, tx)
	}

	return spenders, more, "", nil
}

// checkOtherInput reports whether the input's parent is absent here. A present parent
// must already record this outpoint as spent by spender at vin; anything else is an error.
func (r *Repairer) checkOtherInput(ctx context.Context, spender chainhash.Hash, vin int, in *bt.Input) (bool, error) {
	prev := *in.PreviousTxIDChainHash()

	present, err := r.exists(ctx, prev)
	if err != nil {
		return false, err
	}

	if !present {
		return true, nil
	}

	utxoHash, err := util.UTXOHashFromInput(in)
	if err != nil {
		return false, errors.NewProcessingError("spender %s input %d utxo hash", spender.String(), vin, err)
	}

	sr, err := r.store.GetSpend(ctx, &utxo.Spend{TxID: &prev, Vout: in.PreviousTxOutIndex, UTXOHash: utxoHash})
	if err != nil {
		return false, errors.NewStorageError("read %s:%d", prev.String(), in.PreviousTxOutIndex, err)
	}

	if sr == nil || sr.SpendingData == nil || sr.SpendingData.TxID == nil || !sr.SpendingData.TxID.IsEqual(&spender) || sr.SpendingData.Vin != vin {
		state := "no state"
		if sr != nil {
			state = utxo.Status(sr.Status).String() //nolint:gosec
			if sr.SpendingData != nil {
				state += " by " + sr.SpendingData.String()
			}
		}

		return false, errors.NewProcessingError("spender %s input %d spends %s:%d, which is %s here; the replay would change it", spender.String(), vin, prev.String(), in.PreviousTxOutIndex, state)
	}

	return false, nil
}

// verify reads the parent back and compares it with the plan. With exactBlocks the
// block entries must match the plan exactly; otherwise every planned entry must be
// present. Spent state must match the peer for every output.
func (r *Repairer) verify(ctx context.Context, parent chainhash.Hash, planned Result, outputs int, exactBlocks bool) error {
	got, err := r.store.Get(ctx, &parent, fields.BlockIDs, fields.BlockHeights, fields.Utxos)
	if err != nil {
		return err
	}

	if exactBlocks {
		if !slices.Equal(got.BlockIDs, planned.BlockIDs) {
			return errors.NewProcessingError("block IDs %v, want %v", got.BlockIDs, planned.BlockIDs)
		}

		if !slices.Equal(got.BlockHeights, planned.BlockHeights) {
			return errors.NewProcessingError("block heights %v, want %v", got.BlockHeights, planned.BlockHeights)
		}
	} else if err := containsBlocks(got, planned); err != nil {
		return err
	}

	if len(got.SpendingDatas) != outputs {
		return errors.NewProcessingError("store holds state for %d outputs, transaction has %d", len(got.SpendingDatas), outputs)
	}

	want := make(map[uint32]*spend.SpendingData, len(planned.Spends))
	for _, s := range planned.Spends {
		spender := s.Spender
		want[s.Vout] = spend.NewSpendingData(&spender, s.Vin)
	}

	for vout, sd := range got.SpendingDatas {
		w := want[uint32(vout)] //nolint:gosec

		switch {
		case w == nil && sd == nil:
		case w == nil:
			return errors.NewProcessingError("output %d is spent by %s in the store but unspent at the peer", vout, sd.String())
		case sd == nil || sd.TxID == nil:
			return errors.NewProcessingError("output %d is unspent in the store but spent by %s at the peer", vout, w.String())
		case !sd.TxID.IsEqual(w.TxID) || sd.Vin != w.Vin:
			return errors.NewProcessingError("output %d spent by %s in the store, %s at the peer", vout, sd.String(), w.String())
		}
	}

	return nil
}

func containsBlocks(got *meta.Data, planned Result) error {
	have := make(map[[2]uint32]struct{}, len(got.BlockIDs))

	for i, id := range got.BlockIDs {
		if i < len(got.BlockHeights) {
			have[[2]uint32{id, got.BlockHeights[i]}] = struct{}{}
		}
	}

	for i, id := range planned.BlockIDs {
		if _, ok := have[[2]uint32{id, planned.BlockHeights[i]}]; !ok {
			return errors.NewProcessingError("block %d at height %d is missing; store has IDs %v at heights %v", id, planned.BlockHeights[i], got.BlockIDs, got.BlockHeights)
		}
	}

	return nil
}

// paginatedStore is the part of the Aerospike store the stale-child check needs. The
// utxo.Store interface addresses records by transaction only; pagination records are an
// Aerospike layout detail, so the check is skipped for any other store.
type paginatedStore interface {
	GetUtxoBatchSize() int
	GetClient() *uaerospike.Client
	GetNamespace() string
	GetName() string
}

// findStaleChildRecords returns the keys of pagination records that survived the
// master's deletion. The pruner stamps master and children with the same deleteAtHeight,
// but a failed cascade or a master-only Delete leaves children behind (see
// Store.DeleteComplete). A surviving child keeps its old spent state and locked flag, so
// a rebuild on top of it never re-signals ALLSPENT to the new master and a descendant
// addressing it can get TX_LOCKED. They are removed and rebuilt from the peer instead.
func (r *Repairer) findStaleChildRecords(parent chainhash.Hash, outputs int) ([]*aerospike.Key, error) {
	ps, ok := r.store.(paginatedStore)
	if !ok || ps.GetUtxoBatchSize() <= 0 {
		return nil, nil
	}

	batchSize := ps.GetUtxoBatchSize()
	extra := (outputs+batchSize-1)/batchSize - 1

	stale := make([]*aerospike.Key, 0)

	for n := 1; n <= extra; n++ {
		key, err := aerospike.NewKey(ps.GetNamespace(), ps.GetName(), uaerospike.CalculateKeySourceInternal(&parent, uint32(n))) //nolint:gosec
		if err != nil {
			return nil, errors.NewProcessingError("child record key %d for %s", n, parent.String(), err)
		}

		exists, err := ps.GetClient().Exists(nil, key)
		if err != nil {
			return nil, errors.NewStorageError("check child record %d for %s", n, parent.String(), err)
		}

		if exists {
			r.logger.Warnf("[repair-missing-parents] %s: pagination record %d survived the master's deletion; it will be removed and rebuilt", parent.String(), n)
			stale = append(stale, key)
		}
	}

	return stale, nil
}

func (r *Repairer) deleteStaleChildRecords(keys []*aerospike.Key) error {
	if len(keys) == 0 {
		return nil
	}

	ps := r.store.(paginatedStore)

	for _, key := range keys {
		if _, err := ps.GetClient().Delete(nil, key); err != nil && !errors.Is(err, aerospike.ErrKeyNotFound) {
			return errors.NewStorageError("delete stale child record %v", key, err)
		}
	}

	return nil
}
