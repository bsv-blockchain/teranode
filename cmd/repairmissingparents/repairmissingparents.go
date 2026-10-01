// Package repairmissingparents rebuilds UTXO-store records that were pruned while a
// child transaction still referenced them (issue 1768), using a healthy peer's asset
// service as the source of truth.
//
// For each missing parent the tool fetches the transaction, its mined-state metadata
// and the per-output spent state from the peer, maps the peer's block hashes to local
// block IDs, recreates the record as mined, and replays every recorded spend with the
// real spending transaction so the store's UTXO-hash check runs against the actual
// locking script and value. An output is never recreated as unspent when the peer
// records it as spent.
package repairmissingparents

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
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
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
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
	OutcomePlanned             Outcome = "planned (dry-run)"
	OutcomeAlreadyPresent      Outcome = "already present"
	OutcomePeerPruned          Outcome = "peer has pruned it"
	OutcomeChainMismatch       Outcome = "peer block not on our chain"
	OutcomeInconsistentSpender Outcome = "store records a different spender"
	OutcomeFailed              Outcome = "failed"
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
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PARENT\tOUTCOME\tBLOCKS (height->id)\tSPENT OUTPUTS\tDETAIL")

	for _, res := range r.Results {
		blocks := ""
		for i, h := range res.BlockHeights {
			if i > 0 {
				blocks += ","
			}
			if i < len(res.BlockIDs) {
				blocks += fmt.Sprintf("%d->%d", h, res.BlockIDs[i])
			} else {
				blocks += fmt.Sprintf("%d->?", h)
			}
		}

		spends := ""
		for i, s := range res.Spends {
			if i > 0 {
				spends += " "
			}
			spends += fmt.Sprintf("%d<-%s[%d]", s.Vout, s.Spender.String(), s.Vin)
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", res.Parent.String(), res.Outcome, blocks, spends, res.Detail)
	}

	_ = tw.Flush()

	if r.Scanned > 0 {
		fmt.Fprintf(w, "scanned %d unmined transactions\n", r.Scanned)
	}
}

// Repairer performs the repair against a store, a blockchain client and a peer.
type Repairer struct {
	logger ulogger.Logger
	store  utxo.Store
	chain  blockchain.ClientI
	peer   Peer
	opts   Options
}

// NewRepairer wires the dependencies. Run builds them from settings; tests inject mocks.
func NewRepairer(logger ulogger.Logger, store utxo.Store, chain blockchain.ClientI, peer Peer, opts Options) *Repairer {
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}

	return &Repairer{logger: logger, store: store, chain: chain, peer: peer, opts: opts}
}

// Run builds the store, blockchain client and peer from settings and performs the repair.
func Run(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, opts Options) (*Report, error) {
	if opts.PeerURL == "" {
		return nil, errors.NewConfigurationError("a peer asset URL is required (--peer)")
	}

	chain, err := blockchain.NewClient(ctx, logger, tSettings, "repair-missing-parents")
	if err != nil {
		return nil, errors.NewServiceError("failed to create blockchain client", err)
	}

	store, err := utxofactory.NewStore(ctx, logger, tSettings, "repair-missing-parents", false)
	if err != nil {
		return nil, errors.NewStorageError("failed to open utxo store", err)
	}

	return NewRepairer(logger, store, chain, NewHTTPPeer(opts.PeerURL, nil), opts).Run(ctx)
}

// Run performs the repair and prints the report to Options.Stdout.
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

	if r.opts.Scan {
		missing, scanned, err := r.scanUnmined(ctx)
		if err != nil {
			return nil, err
		}

		report.Scanned = scanned

		for _, h := range missing {
			enqueue(h)
		}
	}

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		res, more := r.repairOne(ctx, parent, currentHeight)
		report.Results = append(report.Results, res)

		for _, h := range more {
			enqueue(h)
		}
	}

	sort.Slice(report.Results, func(i, j int) bool {
		return report.Results[i].Parent.String() < report.Results[j].Parent.String()
	})

	report.Write(r.opts.Stdout)

	return report, nil
}

// gate refuses to run against a live node unless ForceLive is set. Block validation
// and the repair would otherwise race on the same records.
func (r *Repairer) gate(ctx context.Context) error {
	state, err := r.chain.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewServiceError("failed to read FSM state", err)
	}

	if state == nil || *state == blockchain.FSMStateIDLE {
		return nil
	}

	if r.opts.ForceLive {
		r.logger.Warnf("[repair-missing-parents] FSM state is %s but --force-live given; proceeding", state.String())
		return nil
	}

	return errors.NewProcessingError("FSM state is %s, expected IDLE; stop the node or pass --force-live", state.String())
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

func (r *Repairer) exists(ctx context.Context, hash chainhash.Hash) (bool, error) {
	_, err := r.store.Get(ctx, &hash, fields.BlockIDs)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
		return false, nil
	}

	return false, errors.NewStorageError("failed to look up %s", hash.String(), err)
}

// repairOne rebuilds one parent. It returns the result and any further parents found
// missing while preparing the spenders.
func (r *Repairer) repairOne(ctx context.Context, parent chainhash.Hash, currentHeight uint32) (Result, []chainhash.Hash) {
	res := Result{Parent: parent}

	present, err := r.exists(ctx, parent)
	if err != nil {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
		return res, nil
	}

	if present {
		res.Outcome = OutcomeAlreadyPresent
		return res, nil
	}

	tx, err := r.peer.GetTx(ctx, &parent)
	if err != nil {
		return r.peerFailure(res, err), nil
	}

	pm, err := r.peer.GetTxMeta(ctx, &parent)
	if err != nil {
		return r.peerFailure(res, err), nil
	}

	utxos, err := r.peer.GetUTXOs(ctx, &parent)
	if err != nil {
		return r.peerFailure(res, err), nil
	}

	if len(pm.BlockHeights) == 0 {
		res.Outcome, res.Detail = OutcomeFailed, "peer holds the transaction as unmined; nothing to rebuild as a mined parent"
		return res, nil
	}

	res.BlockHeights = pm.BlockHeights

	minedInfos, ids, err := r.mapBlocks(ctx, pm)
	if err != nil {
		res.Outcome, res.Detail = OutcomeChainMismatch, err.Error()
		return res, nil
	}

	res.BlockIDs = ids

	for _, u := range utxos {
		if u.Status != statusSpent || u.SpendingData == nil || u.SpendingData.TxID == nil {
			continue
		}

		res.Spends = append(res.Spends, PlannedSpend{Vout: u.Vout, Spender: *u.SpendingData.TxID, Vin: u.SpendingData.Vin})
	}

	spenders, more, err := r.fetchSpenders(ctx, parent, res.Spends)
	if err != nil {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
		return res, more
	}

	stale, err := r.findStaleChildRecords(parent, len(tx.Outputs))
	if err != nil {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
		return res, more
	}

	if len(stale) > 0 {
		res.Detail = fmt.Sprintf("%d stale pagination record(s) removed before rebuild", len(stale))
	}

	if r.opts.DryRun {
		res.Outcome = OutcomePlanned
		return res, more
	}

	if err := r.deleteStaleChildRecords(stale); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
		return res, more
	}

	// WithCreateOnly: the parent's own inputs were spent long ago and its parents may be
	// legitimately pruned; only the record itself is rebuilt.
	createOpts := []utxo.CreateOption{
		utxo.WithCreateOnly(),
		utxo.WithMinedBlockInfo(minedInfos...),
		utxo.WithTXID(&parent),
		utxo.WithSetCoinbase(pm.IsCoinbase),
	}

	if _, _, err := r.store.SpendAndCreate(ctx, tx, pm.BlockHeights[0], createOpts...); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("create: %v", err)
		return res, more
	}

	for _, spender := range spenders {
		// WithSpendOnly: the spender's own record already exists (mined or unmined);
		// only its spend of the rebuilt parent is replayed. Inputs already recorded with
		// this same spender are idempotent.
		spendOpts := []utxo.CreateOption{
			utxo.WithSpendOnly(),
			utxo.WithIgnoreConflicting(true),
			utxo.WithIgnoreLocked(true),
		}

		_, _, err := r.store.SpendAndCreate(ctx, spender, currentHeight, spendOpts...)
		if err == nil {
			continue
		}

		if errors.Is(err, errors.ErrSpent) {
			res.Outcome, res.Detail = OutcomeInconsistentSpender, fmt.Sprintf("spender %s: %v", spender.TxID(), err)
			return res, more
		}

		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("spend by %s: %v", spender.TxID(), err)
		return res, more
	}

	if err := r.verify(ctx, parent, res); err != nil {
		res.Outcome, res.Detail = OutcomeFailed, fmt.Sprintf("verify: %v", err)
		return res, more
	}

	res.Outcome = OutcomeRepaired

	return res, more
}

func (r *Repairer) peerFailure(res Result, err error) Result {
	if errors.Is(err, errors.ErrNotFound) {
		res.Outcome, res.Detail = OutcomePeerPruned, err.Error()
	} else {
		res.Outcome, res.Detail = OutcomeFailed, err.Error()
	}

	return res
}

// mapBlocks resolves each peer block hash to the local block ID and checks the height
// matches, so a peer on a different chain at that height cannot inject its block IDs.
func (r *Repairer) mapBlocks(ctx context.Context, pm *peerTxMeta) ([]utxo.MinedBlockInfo, []uint32, error) {
	if len(pm.BlockHashes) != len(pm.BlockHeights) {
		return nil, nil, errors.NewProcessingError("peer returned %d block hashes for %d heights", len(pm.BlockHashes), len(pm.BlockHeights))
	}

	infos := make([]utxo.MinedBlockInfo, 0, len(pm.BlockHeights))
	ids := make([]uint32, 0, len(pm.BlockHeights))

	for i, height := range pm.BlockHeights {
		if pm.BlockHashes[i] == "" {
			return nil, nil, errors.NewProcessingError("peer could not resolve its block at height %d", height)
		}

		blockHash, err := chainhash.NewHashFromStr(pm.BlockHashes[i])
		if err != nil {
			return nil, nil, errors.NewProcessingError("peer block hash %q at height %d", pm.BlockHashes[i], height, err)
		}

		_, meta, err := r.chain.GetBlockHeader(ctx, blockHash)
		if err != nil {
			return nil, nil, errors.NewProcessingError("peer block %s (height %d) is not in our blockchain", blockHash.String(), height, err)
		}

		if meta.Height != height {
			return nil, nil, errors.NewProcessingError("peer block %s is at height %d here, peer says %d", blockHash.String(), meta.Height, height)
		}

		subtreeIdx := 0
		if i < len(pm.SubtreeIdxs) {
			subtreeIdx = pm.SubtreeIdxs[i]
		}

		infos = append(infos, utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: height, SubtreeIdx: subtreeIdx, OnLongestChain: true})
		ids = append(ids, meta.ID)
	}

	return infos, ids, nil
}

// fetchSpenders loads each distinct spending transaction from the peer and checks that
// every other parent it spends is present. Absent ones are returned for queueing; the
// spender is still returned so the caller can replay it once they exist. The spend of
// a parent that is still absent fails inside Store.Spend and is reported there.
func (r *Repairer) fetchSpenders(ctx context.Context, parent chainhash.Hash, spends []PlannedSpend) ([]*bt.Tx, []chainhash.Hash, error) {
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
			return nil, more, errors.NewProcessingError("spender %s of output %d", spenderHash.String(), s.Vout, err)
		}

		if !tx.IsExtended() {
			return nil, more, errors.NewProcessingError("spender %s from peer is not extended; cannot compute utxo hashes", spenderHash.String())
		}

		for _, in := range tx.Inputs {
			other := *in.PreviousTxIDChainHash()
			if other.IsEqual(&parent) {
				continue
			}

			present, err := r.exists(ctx, other)
			if err != nil {
				return nil, more, err
			}

			if !present {
				more = append(more, other)
			}
		}

		spenders = append(spenders, tx)
	}

	return spenders, more, nil
}

// verify reads the rebuilt record back and compares it with the plan.
func (r *Repairer) verify(ctx context.Context, parent chainhash.Hash, planned Result) error {
	got, err := r.store.Get(ctx, &parent, fields.BlockIDs, fields.BlockHeights, fields.Utxos)
	if err != nil {
		return err
	}

	if !equalUint32s(got.BlockIDs, planned.BlockIDs) {
		return errors.NewProcessingError("block IDs %v, want %v", got.BlockIDs, planned.BlockIDs)
	}

	if !equalUint32s(got.BlockHeights, planned.BlockHeights) {
		return errors.NewProcessingError("block heights %v, want %v", got.BlockHeights, planned.BlockHeights)
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

func equalUint32s(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
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
