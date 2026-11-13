package teranodecli

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/cmd/rewindblockchain/rewindblockchain"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// The mined-set audit looks for blocks stored with mined_set true whose transactions do not
// carry the block's id. Two routes stored such blocks before the fix in PR 2180: a catch-up
// block whose quick validation reserved an id, stamped some batches and then fell through to
// full validation, and a ProcessBlock call that supplied a block_id and took full validation.
// Both stored the block with mined_set true, and the setMined worker skips every such block, so
// nothing tagged the rest of its transactions. A later block that spends one of their outputs
// can then fail with empty parent block ids or a locked output.
//
// The audit only reads. There is no repair: an affected node is resynced.
//
// It samples rather than scans. For each block it reads the first transaction after the
// coinbase in the first subtree and the first transaction in the last subtree. Quick
// validation stamps the subtrees batch by batch in order, so a failed attempt leaves the later
// subtrees untagged, and the last subtree is the one to look at; the ProcessBlock route tagged
// nothing, so either sample finds it. A block with one subtree has one sample.

const (
	// minedSetAuditHeaderPage is how many block heights one GetBlockHeadersByHeight call covers.
	minedSetAuditHeaderPage = 1000

	// minedSetAuditDefaultMaxBlocks bounds a run unless the operator raises it.
	minedSetAuditDefaultMaxBlocks = 10_000

	// A serialized subtree is a 32-byte root, 8 bytes of fees, 8 bytes of size and an
	// 8-byte node count, then 48 bytes per node: a 32-byte transaction hash, its fee and
	// its size (go-subtree Subtree.Serialize).
	subtreeNodeCountOffset = 48
	subtreeHeaderBytes     = 56
	subtreeNodeBytes       = 48
)

// minedSetAuditChain is the part of the blockchain client the audit reads.
type minedSetAuditChain interface {
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
}

// minedSetAuditor holds what one audit run reads from.
type minedSetAuditor struct {
	chain    minedSetAuditChain
	utxo     utxo.Store
	subtrees blob.Store

	// netted reports whether a block at the height can only be quick validated on this UTXO
	// store (model.NettedRouteAt). Full validation never stores such a block, so neither old
	// route can have produced one, and the netting store does not keep a record for every
	// transaction, so a sample there proves nothing.
	netted func(height uint32) bool
}

// minedSetAuditFinding is one block the audit reports.
type minedSetAuditFinding struct {
	Height  uint32
	Hash    chainhash.Hash
	BlockID uint32
	TxID    chainhash.Hash // the sampled transaction found wrong, for an affected block
	Locked  bool           // whether that transaction's outputs are still locked
	Reason  string         // what is wrong, or why the block could not be checked
}

// minedSetAuditResult counts every main-chain block in the range by what the audit found.
type minedSetAuditResult struct {
	Scanned      int // main-chain blocks in the range
	NotMinedSet  int // mined_set false: the setMined worker still owns them
	Netted       int // on the netted route, not checkable and not reachable by the old routes
	CoinbaseOnly int // no transaction besides the coinbase to sample
	Tagged       int // every sampled transaction found carries the block's id and is unlocked
	Unchecked    []minedSetAuditFinding
	Affected     []minedSetAuditFinding
}

// run audits the main-chain blocks from startHeight to endHeight inclusive.
func (a *minedSetAuditor) run(ctx context.Context, startHeight, endHeight, maxBlocks uint32) (*minedSetAuditResult, error) {
	if endHeight < startHeight {
		return nil, errors.NewInvalidArgumentError("end height %d is below start height %d", endHeight, startHeight)
	}

	if span := uint64(endHeight) - uint64(startHeight) + 1; span > uint64(maxBlocks) {
		return nil, errors.NewInvalidArgumentError("the range %d to %d holds %d heights, more than --max-blocks %d; narrow the range or raise --max-blocks", startHeight, endHeight, span, maxBlocks)
	}

	result := &minedSetAuditResult{}

	for pageStart := uint64(startHeight); pageStart <= uint64(endHeight); pageStart += minedSetAuditHeaderPage {
		pageEnd := min(pageStart+minedSetAuditHeaderPage-1, uint64(endHeight))

		headers, _, err := a.chain.GetBlockHeadersByHeight(ctx, uint32(pageStart), uint32(pageEnd)) //nolint:gosec // both are within the uint32 range the caller passed
		if err != nil {
			return nil, errors.NewProcessingError("failed to read block headers %d to %d", pageStart, pageEnd, err)
		}

		// The height query leaves mined_set out of its metas, so each block's own header
		// read supplies it.
		for _, header := range headers {
			_, meta, err := a.chain.GetBlockHeader(ctx, header.Hash())
			if err != nil {
				return nil, errors.NewProcessingError("failed to read block header %s", header.Hash().String(), err)
			}

			if err := a.auditBlock(ctx, header.Hash(), meta, result); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// auditBlock classifies one main-chain block into result.
func (a *minedSetAuditor) auditBlock(ctx context.Context, hash *chainhash.Hash, meta *model.BlockHeaderMeta, result *minedSetAuditResult) error {
	result.Scanned++

	if !meta.MinedSet {
		result.NotMinedSet++
		return nil
	}

	if a.netted != nil && a.netted(meta.Height) {
		result.Netted++
		return nil
	}

	finding := minedSetAuditFinding{Height: meta.Height, Hash: *hash, BlockID: meta.ID}

	block, err := a.chain.GetBlock(ctx, hash)
	if err != nil {
		return errors.NewProcessingError("failed to read block %s at height %d", hash.String(), meta.Height, err)
	}

	samples, reason, err := a.sampleTxIDs(ctx, block)
	if err != nil {
		return err
	}

	if reason != "" {
		finding.Reason = reason
		result.Unchecked = append(result.Unchecked, finding)

		return nil
	}

	if len(samples) == 0 {
		result.CoinbaseOnly++
		return nil
	}

	found := 0

	for _, txID := range samples {
		data, err := a.utxo.Get(ctx, &txID, fields.BlockIDs, fields.Locked)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return errors.NewProcessingError("failed to read transaction %s of block %s", txID.String(), hash.String(), err)
		}

		found++

		// A transaction quick validation stamped is created locked and unlocked only after the
		// block commits. One that carries the id but is still locked was stamped by an attempt
		// that fell through to full validation, or by a commit whose unlock failed after the
		// block was stored, and nothing unlocked it.
		switch {
		case !slices.Contains(data.BlockIDs, meta.ID):
			finding.Reason = "does not carry the block id"
		case data.Locked:
			finding.Reason = "carries the block id but its outputs are still locked"
		default:
			continue
		}

		finding.TxID = txID
		finding.Locked = data.Locked
		result.Affected = append(result.Affected, finding)

		return nil
	}

	if found == 0 {
		finding.Reason = "no sampled transaction is in the UTXO store"
		result.Unchecked = append(result.Unchecked, finding)

		return nil
	}

	result.Tagged++

	return nil
}

// sampleTxIDs returns the transactions the audit checks for block. A non-empty reason means
// the block cannot be checked, and an empty sample list with no reason means the block holds
// only its coinbase.
func (a *minedSetAuditor) sampleTxIDs(ctx context.Context, block *model.Block) ([]chainhash.Hash, string, error) {
	if len(block.Subtrees) == 0 {
		return nil, "", nil
	}

	var samples []chainhash.Hash

	// Node 0 of the first subtree is the coinbase placeholder, so the first real transaction
	// is node 1.
	first, ok, reason, err := a.subtreeNode(ctx, block.Subtrees[0], 1)
	if err != nil || reason != "" {
		return nil, reason, err
	}

	if ok {
		samples = append(samples, first)
	}

	if last := len(block.Subtrees) - 1; last > 0 {
		node, ok, reason, err := a.subtreeNode(ctx, block.Subtrees[last], 0)
		if err != nil || reason != "" {
			return nil, reason, err
		}

		if ok {
			samples = append(samples, node)
		}
	}

	return samples, "", nil
}

// subtreeNode reads the transaction hash at index from a stored subtree, streaming only the
// bytes up to it. ok is false when the subtree has no node at index. A non-empty reason means
// the subtree file is missing, unreadable or malformed. The file blob store checks the file's
// own header inside GetIoReader and returns a storage error for a short, bad-magic or
// wrong-type file, so a storage error is one block's finding too, not a failure of the run.
// A cancelled or expired context still fails the run.
func (a *minedSetAuditor) subtreeNode(ctx context.Context, subtreeHash *chainhash.Hash, index uint64) (chainhash.Hash, bool, string, error) {
	var node chainhash.Hash

	reader, err := a.subtrees.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return node, false, "", errors.NewProcessingError("failed to open subtree %s", subtreeHash.String(), err)
		case errors.Is(err, errors.ErrNotFound):
			return node, false, fmt.Sprintf("subtree %s is not in the subtree store", subtreeHash.String()), nil
		case errors.Is(err, errors.ErrStorageError):
			return node, false, fmt.Sprintf("subtree %s could not be read: %v", subtreeHash.String(), err), nil
		}

		return node, false, "", errors.NewProcessingError("failed to open subtree %s", subtreeHash.String(), err)
	}

	defer func() { _ = reader.Close() }()

	var header [subtreeHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return node, false, fmt.Sprintf("subtree %s is shorter than its header", subtreeHash.String()), nil //nolint:nilerr // a malformed file is a finding, not a failure of the run
	}

	if count := binary.LittleEndian.Uint64(header[subtreeNodeCountOffset:]); index >= count {
		return node, false, "", nil
	}

	if _, err := io.CopyN(io.Discard, reader, int64(index)*subtreeNodeBytes); err != nil { //nolint:gosec // index is 0 or 1
		return node, false, fmt.Sprintf("subtree %s is shorter than its node count", subtreeHash.String()), nil //nolint:nilerr // as above
	}

	if _, err := io.ReadFull(reader, node[:]); err != nil {
		return node, false, fmt.Sprintf("subtree %s is shorter than its node count", subtreeHash.String()), nil //nolint:nilerr // as above
	}

	return node, true, "", nil
}

// report prints result and returns an error when a block is affected, so the command exits
// non-zero.
func (r *minedSetAuditResult) report(out io.Writer, startHeight, endHeight uint32) error {
	for _, f := range r.Affected {
		_, _ = fmt.Fprintf(out, "AFFECTED  height %d block %s id %d: transaction %s %s (locked=%t)\n", f.Height, f.Hash.String(), f.BlockID, f.TxID.String(), f.Reason, f.Locked)
	}

	for _, f := range r.Unchecked {
		_, _ = fmt.Fprintf(out, "UNCHECKED height %d block %s id %d: %s\n", f.Height, f.Hash.String(), f.BlockID, f.Reason)
	}

	_, _ = fmt.Fprintf(out, "\nMined-set audit of main-chain heights %d to %d\n", startHeight, endHeight)
	_, _ = fmt.Fprintf(out, "  blocks scanned:                %d\n", r.Scanned)
	_, _ = fmt.Fprintf(out, "  mined_set false (not audited): %d\n", r.NotMinedSet)
	_, _ = fmt.Fprintf(out, "  netted route (not audited):    %d\n", r.Netted)
	_, _ = fmt.Fprintf(out, "  coinbase only:                 %d\n", r.CoinbaseOnly)
	_, _ = fmt.Fprintf(out, "  samples tagged:                %d\n", r.Tagged)
	_, _ = fmt.Fprintf(out, "  unchecked:                     %d\n", len(r.Unchecked))
	_, _ = fmt.Fprintf(out, "  affected:                      %d\n", len(r.Affected))

	if len(r.Affected) == 0 {
		return nil
	}

	_, _ = fmt.Fprintln(out, "\nThis node stored blocks with mined_set true whose transactions were never tagged with the block id, or never unlocked. Nothing repairs them in place: resync the node.")

	return errors.NewProcessingError("%d block(s) stored with mined_set true carry untagged or locked transactions", len(r.Affected))
}

// minedSetAuditFlags are the audit-mined-set command's flags.
type minedSetAuditFlags struct {
	startHeight *uint
	endHeight   *uint
	maxBlocks   *uint
}

// minedSetAuditLongHelp is what --help prints above the flags.
const minedSetAuditLongHelp = `teranode-cli audit-mined-set --start-height=<h> --end-height=<h> [--max-blocks=<n>]

Lists main-chain blocks stored with mined_set true whose sampled transactions do not carry
the block's id in the UTXO store, or carry it with their outputs still locked. Releases before PR 2180 could store such a block when a
catch-up block fell from quick validation to full validation, or when a ProcessBlock caller
supplied a block_id. A quick-validated block whose unlock failed after it was stored also
leaves its transactions tagged but locked. Nothing tags or unlocks those transactions
afterwards, and a later block spending one of their outputs can fail.

It samples two transactions per block (the first after the coinbase and the first of the
last subtree), so a block it reports as tagged is not proven clean. A block is UNCHECKED
when its subtree file is gone, unreadable or malformed, or no sampled transaction is in the UTXO store; UNCHECKED is
not a pass. Blocks on a netting UTXO store's below-checkpoint route are skipped: full
validation never stores them.

Quick validation stores a block with mined_set true and unlocks its outputs just after, so
a block being committed while the audit runs can show as locked; run it again for that
height before acting on it.

Remedy: there is no in-place repair. Resync an affected node.

It needs the blockchain service running, and reads blocks through it. It opens the UTXO
store and the subtree store from this node's settings. It changes no data, but opening the
UTXO store runs that store's normal startup, which creates any missing schema (and on
Postgres can start the index build the node itself runs). It exits non-zero when it finds an
affected block.
`

func registerMinedSetAuditFlags(fs *flag.FlagSet) *minedSetAuditFlags {
	return &minedSetAuditFlags{
		startHeight: fs.Uint("start-height", 0, "First main-chain height to audit"),
		endHeight:   fs.Uint("end-height", 0, "Last main-chain height to audit (required)"),
		maxBlocks:   fs.Uint("max-blocks", minedSetAuditDefaultMaxBlocks, "Refuse a range holding more heights than this"),
	}
}

func minedSetAuditExecute(logger ulogger.Logger, tSettings *settings.Settings, f *minedSetAuditFlags) func(args []string) error {
	return func(args []string) error {
		if *f.endHeight == 0 {
			return errors.NewInvalidArgumentError("--end-height is required")
		}

		for name, v := range map[string]uint{"--start-height": *f.startHeight, "--end-height": *f.endHeight, "--max-blocks": *f.maxBlocks} {
			if v > math.MaxUint32 {
				return errors.NewInvalidArgumentError("%s %d is above %d", name, v, uint64(math.MaxUint32))
			}
		}

		startHeight, endHeight, maxBlocks := uint32(*f.startHeight), uint32(*f.endHeight), uint32(*f.maxBlocks) //nolint:gosec // bounded above

		ctx := context.Background()

		chain, err := blockchain.NewClient(ctx, logger, tSettings, "audit-mined-set")
		if err != nil {
			return errors.NewServiceError("failed to connect to the blockchain service", err)
		}

		utxoStore, err := utxofactory.NewStore(ctx, logger, tSettings, "audit-mined-set", false)
		if err != nil {
			return errors.NewStorageError("failed to open the UTXO store", err)
		}

		subtreeStore, err := rewindblockchain.NewSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		auditor := &minedSetAuditor{
			chain:    chain,
			utxo:     utxoStore,
			subtrees: subtreeStore,
			netted: func(height uint32) bool {
				return model.NettedRouteAt(tSettings, utxoStore, height)
			},
		}

		result, err := auditor.run(ctx, startHeight, endHeight, maxBlocks)
		if err != nil {
			return err
		}

		return result.report(os.Stdout, startHeight, endHeight)
	}
}
