// Package seedimport loads a verifiable UTXO seed into a node's UTXO store.
package seedimport

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/muhash"
	"github.com/bsv-blockchain/teranode/pkg/seedcheckpoint"
	"github.com/bsv-blockchain/teranode/pkg/utxoseed"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/cohort"
)

// maxVoutIndex bounds the highest vout index a seed wrapper may carry. The UTXO
// store maps an output's position in tx.Outputs to its vout, so a UTXO at vout V
// requires a length-(V+1) slice — placement is positional and cannot be packed
// densely without corrupting vouts. Seed data is unverified until the post-load
// set-hash check, so without this bound a hostile wrapper claiming vout ~2^32
// would force make([]*bt.Output, maxVout+1) to allocate ~32 GB before the
// integrity gate is reached. The ceiling is generous (no real transaction has
// this many outputs) but caps a single wrapper's transient allocation at ~128 MB,
// and wrappers are processed one at a time so it does not accumulate.
const maxVoutIndex = 1 << 24

// wrapperToTx synthesizes a partial transaction carrying only the surviving
// outputs of a UTXOWrapper. Inputs are empty; Outputs are sparse (nil at vouts
// that were already spent) because the store keys each output by its slice
// index = vout. The real txid is set via SetTxHash so any internal
// TxIDChainHash() call is correct; Create is additionally given WithTXID.
func wrapperToTx(w *utxopersister.UTXOWrapper) (*bt.Tx, error) {
	maxVout := uint32(0)
	for _, u := range w.UTXOs {
		if u.Index > maxVout {
			maxVout = u.Index
		}
	}

	if maxVout >= maxVoutIndex {
		return nil, errors.NewProcessingError("wrapper %s vout %d exceeds bound %d", w.TxID.String(), maxVout, maxVoutIndex)
	}

	tx := bt.NewTx()

	tx.Outputs = make([]*bt.Output, maxVout+1)
	for _, u := range w.UTXOs {
		tx.Outputs[u.Index] = &bt.Output{
			Satoshis:      u.Value,
			LockingScript: bscript.NewFromBytes(u.Script),
		}
	}

	txid := w.TxID
	tx.SetTxHash(&txid)

	return tx, nil
}

// loadWrapper stores a wrapper's surviving outputs as UTXOs mined in block
// blockID, keyed by the wrapper's real txid.
// cohortStamping carries UtxoStore.CohortStamping: when false no cohort label is
// stamped at all, so the stored label stays at cohort.Unset.
func loadWrapper(ctx context.Context, store utxo.Store, w *utxopersister.UTXOWrapper, blockID uint32, cohortStamping bool) error {
	tx, err := wrapperToTx(w)
	if err != nil {
		return err
	}

	txid := w.TxID

	createOpts := []utxo.CreateOption{
		utxo.WithCreateOnly(),
		utxo.WithTXID(&txid),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID:        blockID,
			BlockHeight:    w.Height,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}),
		utxo.WithSetCoinbase(w.Coinbase),
	}

	// Snapshot imports are born mined and their real creation time is unknown, so
	// they carry the historical sentinel rather than a wall-clock label. (issue 556)
	if cohortStamping {
		createOpts = append(createOpts, utxo.WithCohort(cohort.Historical))
	}

	_, _, err = store.SpendAndCreate(ctx, tx, w.Height, createOpts...)

	return err
}

// BlockHeaderLookup binds a seed's block hash to the PoW-validated header chain.
type BlockHeaderLookup interface {
	BlockIDAndHeight(ctx context.Context, blockHash *chainhash.Hash) (id uint32, height uint32, onMainChain bool, err error)
}

// blockchainStore is the minimal slice of the blockchain store needed to resolve
// a block hash to its ID, height, and main-chain membership.
type blockchainStore interface {
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)
}

// blockchainLookup is the production BlockHeaderLookup backed by the blockchain store.
type blockchainLookup struct {
	store blockchainStore
}

// NewBlockchainLookup returns a BlockHeaderLookup that resolves block hashes
// against the given blockchain store.
func NewBlockchainLookup(store blockchainStore) BlockHeaderLookup {
	return &blockchainLookup{store: store}
}

func (l *blockchainLookup) BlockIDAndHeight(ctx context.Context, h *chainhash.Hash) (uint32, uint32, bool, error) {
	_, meta, err := l.store.GetBlockHeader(ctx, h)
	if err != nil {
		return 0, 0, false, err
	}

	onMain, err := l.store.CheckBlockIsInCurrentChain(ctx, []uint32{meta.ID})
	if err != nil {
		return 0, 0, false, err
	}

	return meta.ID, meta.Height, onMain, nil
}

// LoadTrustedKeys parses compiled-in + flag-provided compressed authority pubkeys (hex).
func LoadTrustedKeys(compiledIn []string, flagHex string) ([][]byte, error) {
	hexKeys := append([]string{}, compiledIn...)
	if flagHex != "" {
		hexKeys = append(hexKeys, flagHex)
	}

	if len(hexKeys) == 0 {
		return nil, errors.NewProcessingError("no trusted authority key: build with a compiled-in key or pass --authority-pubkey")
	}

	out := make([][]byte, 0, len(hexKeys))

	for _, h := range hexKeys {
		raw, err := hex.DecodeString(h)
		if err != nil {
			return nil, errors.NewProcessingError("invalid authority pubkey hex %q", h, err)
		}

		pub, err := bec.ParsePubKey(raw)
		if err != nil {
			return nil, errors.NewProcessingError("invalid authority pubkey %q", h, err)
		}

		// Normalize to the 33-byte compressed form. The checkpoint embeds the
		// signer's compressed pubkey and VerifyWithKey compares raw bytes, so an
		// operator who pastes an uncompressed key would otherwise never match.
		out = append(out, pub.Compressed())
	}

	return out, nil
}

// Config holds everything Run needs; stores are injected so Run is testable.
type Config struct {
	SeedStore   blob.Store
	UTXOStore   utxo.Store
	Lookup      BlockHeaderLookup
	TrustedKeys [][]byte
	BlockHash   chainhash.Hash

	// NetworkMagic is the local network's wire magic (settings.ChainCfgParams.Net).
	// It is mixed into the checkpoint signature digest so a checkpoint signed for
	// another network cannot verify here.
	NetworkMagic uint32

	// CohortStamping carries UtxoStore.CohortStamping. When false, imported
	// transactions are created with no cohort label at all (cohort.Unset).
	CohortStamping bool
}

// Run verifies and loads the seed identified by cfg.BlockHash.
func Run(ctx context.Context, logger ulogger.Logger, cfg Config) error {
	sc, err := readSignedCheckpointBlob(ctx, cfg.SeedStore, cfg.BlockHash)
	if err != nil {
		return err
	}

	if err := verifyAgainstTrusted(sc, cfg.TrustedKeys, cfg.NetworkMagic); err != nil {
		return err
	}

	if sc.Checkpoint.CommitmentVersion != utxoseed.CommitmentVersion {
		return errors.NewProcessingError("unsupported commitment version %d: this build computes the set commitment at version %d", sc.Checkpoint.CommitmentVersion, utxoseed.CommitmentVersion)
	}

	if sc.Checkpoint.BlockHash != cfg.BlockHash {
		return errors.NewProcessingError("checkpoint blockHash does not match requested block")
	}

	blockID, height, onMain, err := cfg.Lookup.BlockIDAndHeight(ctx, &cfg.BlockHash)
	if err != nil {
		return errors.NewProcessingError("header lookup failed (is the node header-synced?)", err)
	}

	if !onMain {
		return errors.NewProcessingError("seed block %s is not on the most-work chain", cfg.BlockHash.String())
	}

	if height != sc.Checkpoint.Height {
		return errors.NewProcessingError("height mismatch: checkpoint %d, chain %d", sc.Checkpoint.Height, height)
	}

	// streamLoad returns every UTXO it created even on failure, so a mid-stream
	// error (store write, truncation, corrupt chunk) rolls back the partial set
	// rather than orphaning it — a re-run would otherwise hit duplicate-create
	// errors against the leftover records.
	created, digest, err := streamLoad(ctx, cfg, blockID)
	if err != nil {
		rollback(ctx, logger, cfg.UTXOStore, created)
		return err
	}

	if digest != sc.Checkpoint.SetHash {
		rollback(ctx, logger, cfg.UTXOStore, created)
		return errors.NewProcessingError("set hash mismatch: loaded set does not match signed checkpoint")
	}

	logger.Infof("[seedimport] loaded %d transactions for block %s height %d", len(created), cfg.BlockHash.String(), height)

	return nil
}

func readSignedCheckpointBlob(ctx context.Context, store blob.Store, blockHash chainhash.Hash) (*seedcheckpoint.SignedCheckpoint, error) {
	b, err := store.Get(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint)
	if err != nil {
		return nil, errors.NewProcessingError("reading signed checkpoint for %s", blockHash.String(), err)
	}

	return seedcheckpoint.ParseSignedCheckpoint(b)
}

func verifyAgainstTrusted(sc *seedcheckpoint.SignedCheckpoint, trusted [][]byte, netMagic uint32) error {
	for _, key := range trusted {
		if sc.VerifyWithKey(key, netMagic) == nil {
			return nil
		}
	}

	return errors.NewProcessingError("checkpoint not signed by any trusted authority key (or signed for a different network)")
}

// seedFooterLen is the trailing footer written by CreateUTXOSet after the last
// UTXOWrapper: txCount(8 LE) | utxoCount(8 LE).
const seedFooterLen = 16

// streamLoad reassembles the seed body, loads every surviving UTXO, and returns
// the recomputed set digest. It returns the txids it created on every path
// (including errors) so the caller can roll back a partial load.
func streamLoad(ctx context.Context, cfg Config, blockID uint32) (created []chainhash.Hash, digest [32]byte, err error) {
	pr, pw := io.Pipe()

	// Closing the read end on every return path unblocks the writer goroutine
	// (otherwise a mid-stream error leaves it blocked forever in w.Write).
	defer pr.Close()

	go func() {
		pw.CloseWithError(utxopersister.StreamSeedPackage(ctx, cfg.SeedStore, cfg.BlockHash, pw))
	}()

	// bufio lets us Peek the stream to tell "another wrapper follows" from "the
	// trailing footer" structurally, instead of classifying a read error by its
	// message text.
	br := bufio.NewReader(pr)

	acc := muhash.New()

	var hdr [68]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return created, [32]byte{}, errors.NewProcessingError("reading seed header", err)
	}

	var currentHash chainhash.Hash
	copy(currentHash[:], hdr[0:32])

	if currentHash != cfg.BlockHash {
		return created, [32]byte{}, errors.NewProcessingError("seed body block hash does not match requested block")
	}

	var utxoCount uint64

	for {
		// A UTXOWrapper is always larger than the seedFooterLen-byte footer, so
		// if fewer than seedFooterLen+1 bytes remain we have reached the footer
		// (or a truncated tail). This avoids matching io.ErrUnexpectedEOF by
		// substring, which teranode's errors package cannot distinguish from a
		// genuine mid-stream read error carrying the same text.
		if _, perr := br.Peek(seedFooterLen + 1); perr != nil {
			if !errors.Is(perr, io.EOF) && !errors.Is(perr, io.ErrUnexpectedEOF) {
				// A real stream error (e.g. a corrupt chunk), not end-of-stream.
				return created, [32]byte{}, perr
			}

			if err := validateFooter(br, uint64(len(created)), utxoCount); err != nil {
				return created, [32]byte{}, err
			}

			break
		}

		w, err := utxopersister.NewUTXOWrapperFromReader(ctx, br)
		if err != nil {
			return created, [32]byte{}, err
		}

		for _, u := range w.UTXOs {
			acc.Add(utxoseed.Element(w.TxID, u.Index, w.Height, w.Coinbase, u.Value, u.Script))
			utxoCount++
		}

		if err := loadWrapper(ctx, cfg.UTXOStore, w, blockID, cfg.CohortStamping); err != nil {
			return created, [32]byte{}, err
		}

		created = append(created, w.TxID)
	}

	return created, acc.Digest(), nil
}

// validateFooter consumes the trailing footer and confirms it matches the set
// actually loaded, then that nothing follows it. A count mismatch means the body
// was truncated or tampered before the (more expensive) set-hash check.
func validateFooter(r io.Reader, wantTxs, wantUTXOs uint64) error {
	var footer [seedFooterLen]byte
	if _, err := io.ReadFull(r, footer[:]); err != nil {
		return errors.NewProcessingError("seed body truncated: cannot read %d-byte footer", seedFooterLen, err)
	}

	if _, err := io.ReadFull(r, make([]byte, 1)); err != io.EOF {
		return errors.NewProcessingError("seed body has trailing bytes after footer")
	}

	gotTxs := binary.LittleEndian.Uint64(footer[0:8])
	gotUTXOs := binary.LittleEndian.Uint64(footer[8:16])

	if gotTxs != wantTxs || gotUTXOs != wantUTXOs {
		return errors.NewProcessingError("seed footer mismatch: footer declares %d txs / %d utxos, loaded %d / %d", gotTxs, gotUTXOs, wantTxs, wantUTXOs)
	}

	return nil
}

func rollback(ctx context.Context, logger ulogger.Logger, store utxo.Store, created []chainhash.Hash) {
	for i := range created {
		if err := store.Delete(ctx, &created[i]); err != nil {
			logger.Warnf("[seedimport] rollback: failed to delete %s: %v", created[i].String(), err)
		}
	}
}
