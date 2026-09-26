package netsync

import (
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// writtenSubtree records one artefact this block put in the store, so a failed
// merkle root knows what to try to remove. It is not a record of what this
// block alone wrote: put treats an already-present blob as success (two blocks
// can share an identical run of transactions and so the same subtree under the
// same key), and Emit still records the artefact when that happens. Removing
// it is acceptable rather than a correctness bug, because the file is
// content-addressed and rebuildable — see DeleteAll's comment below.
type writtenSubtree struct {
	Hash     chainhash.Hash
	FileType fileformat.FileType
}

// subtreeWriter puts a completed subtree's three artefacts in the blob store, in
// the order that makes a crash decidable.
//
// Transactions first, then inpoints, then the structure. The structure file is
// what everything else is found by, so writing it last makes its presence the
// marker that the other two are complete. That matters more here than it would
// elsewhere because there is no second source: if a subtree file is missing or
// will not deserialise, block validation fails the block, and its only fallback is
// a memory-mapped read falling back to a heap read of the same local file
// (services/blockvalidation/quick_validate.go:973).
//
// The structure's file type is a claim about trust. Below a checkpoint legacy has
// done the work itself and writes FileTypeSubtree, the already-validated marker;
// otherwise it writes FileTypeSubtreeToCheck and the subtree validation service
// re-checks it.
type subtreeWriter struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	store           blob.Store
	height          uint32
	quickValidation bool
	// heightUnknown is true when this writer was built by
	// newSubtreeWriterUnresolvedHeight rather than newSubtreeWriter: height
	// above is not a real block height (it is never read in that case) and
	// dahOverride is what put() stamps every artefact with instead of
	// height + retention.
	heightUnknown bool
	dahOverride   uint32
	written       []writtenSubtree
	// pending holds each subtree's data file while the builder streams transactions
	// into it, by subtree index. Its name is the subtree root hash, which is unknown
	// until the subtree is complete, so it is written under a temporary name and
	// committed in Emit.
	pending map[int]*blobfile.PendingFile
}

// pendingFileStore is a blob store that can take a file before its key is known. The
// local file store can; a store that cannot gets the subtree data buffered in memory
// and written whole in Emit, as before.
type pendingFileStore interface {
	NewPendingFile(ctx context.Context, fileType fileformat.FileType, opts ...options.FileOption) (*blobfile.PendingFile, error)
}

// pendingDataSink writes a subtree's transactions straight into its pending data file.
type pendingDataSink struct {
	file *blobfile.PendingFile
}

func (s pendingDataSink) Write(p []byte) (int, error) {
	return s.file.Write(p)
}

func (s pendingDataSink) WriteTx(tx *bt.Tx) error {
	// SerializeTo, as the buffered data file does, so both write the same bytes.
	_, err := tx.SerializeTo(s.file)

	return err
}

func (s pendingDataSink) Abort() {
	s.file.Abort()
}

func newSubtreeWriter(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, height uint32, quickValidation bool) *subtreeWriter {
	return &subtreeWriter{
		logger:          logger,
		settings:        tSettings,
		store:           store,
		height:          height,
		quickValidation: quickValidation,
		written:         make([]writtenSubtree, 0, 3),
	}
}

// newSubtreeWriterUnresolvedHeight builds a writer for a block whose parent
// height pipelineParentHeight could not resolve at conversion time.
//
// Both of the height-dependent decisions below get an explicit answer rather
// than being computed from a height that would otherwise default to zero:
//
// The structure file type is unconditionally .subtreeToCheck (quickValidation
// is forced false and never exposed as a constructor argument here). This is
// defence in depth, not a correctness fix: model.BelowCheckpoint
// (model/checkpoint.go) already requires height > 0 before anything else,
// deliberately and documented, and that clause predates this branch — a zero
// height was never going to be read as below any checkpoint, and
// quickValidationAllowed(0) has always been false. Forcing it explicitly here
// means this constructor cannot start writing the unearned, already-validated
// .subtree form if that positivity clause is ever loosened or removed later
// without this file being touched.
//
// The delete-at-height is dah, supplied by the caller rather than computed
// from height + retention (which a zero height would put far below the chain
// tip, collecting the files almost immediately out from under a block still
// waiting — this half IS a real correctness requirement, unlike the file-type
// half above). The caller computes it as the committed tip plus the read-ahead
// depth plus the retention — above any height this block can actually have,
// the same guarantee height + retention gives when the height is real.
func newSubtreeWriterUnresolvedHeight(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, dah uint32) *subtreeWriter {
	return &subtreeWriter{
		logger:        logger,
		settings:      tSettings,
		store:         store,
		heightUnknown: true,
		dahOverride:   dah,
		written:       make([]writtenSubtree, 0, 3),
	}
}

// OpenData returns the function the stream builder calls to open each subtree's data
// file, for withSubtreeDataSink. It returns a nil sink when the store cannot take a
// file before its key is known, and the builder then holds the data as before.
func (w *subtreeWriter) OpenData(ctx context.Context) func(index int) (subtreeDataSink, error) {
	return func(index int) (subtreeDataSink, error) {
		store, ok := w.store.(pendingFileStore)
		if !ok {
			return nil, nil
		}

		file, err := store.NewPendingFile(ctx, fileformat.FileTypeSubtreeData)
		if err != nil {
			return nil, err
		}

		if w.pending == nil {
			w.pending = make(map[int]*blobfile.PendingFile)
		}

		w.pending[index] = file

		return pendingDataSink{file: file}, nil
	}
}

// Emit returns the function the stream builder calls for each completed subtree.
func (w *subtreeWriter) Emit(ctx context.Context) subtreeEmitFunc {
	return func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error {
		root := st.RootHash()
		if root == nil {
			return errors.NewProcessingError("[subtreeWriter] subtree %d has no root hash", index)
		}

		structureType := fileformat.FileTypeSubtreeToCheck
		if w.quickValidation {
			structureType = fileformat.FileTypeSubtree
		}

		metaBytes, err := meta.Serialize()
		if err != nil {
			return errors.NewStorageError("[subtreeWriter][%s] failed to serialize subtree meta", root, err)
		}

		structureBytes, err := st.Serialize()
		if err != nil {
			return errors.NewStorageError("[subtreeWriter][%s] failed to serialize subtree", root, err)
		}

		// Order is load-bearing, see the type comment. The two small artefacts are
		// serialised first so a failure there cannot leave a partial set on disk; the
		// data file is streamed into the store transaction by transaction and never
		// held in memory whole. A failure while streaming aborts that file, and
		// nothing of this subtree has been written before it.
		//
		// It used to be built in one buffer that started at 32 KB and doubled as it
		// grew, each transaction serialized into a fresh slice on the way: on mainnet
		// on 2026-09-24 that was over a third of all the node's allocation.
		writeData := func(dst io.Writer) error {
			return data.WriteTransactionsToWriter(dst, 0, st.Length())
		}

		artefacts := []struct {
			fileType fileformat.FileType
			write    func(io.Writer) error
		}{
			{fileformat.FileTypeSubtreeMeta, writeBytes(metaBytes)},
			{structureType, writeBytes(structureBytes)},
		}

		// The data file was streamed in as the transactions arrived; it only needs its name.
		if file, ok := w.pending[index]; ok {
			delete(w.pending, index)

			if err = file.Commit(ctx, root[:], options.WithDeleteAt(w.deleteAt())); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
				return errors.NewStorageError("[subtreeWriter][%s] failed committing %s", root, fileformat.FileTypeSubtreeData, err)
			}

			w.written = append(w.written, writtenSubtree{Hash: *root, FileType: fileformat.FileTypeSubtreeData})
		} else {
			artefacts = append([]struct {
				fileType fileformat.FileType
				write    func(io.Writer) error
			}{{fileformat.FileTypeSubtreeData, writeData}}, artefacts...)
		}

		for _, artefact := range artefacts {
			if err = w.put(ctx, *root, artefact.fileType, artefact.write); err != nil {
				return err
			}

			w.written = append(w.written, writtenSubtree{Hash: *root, FileType: artefact.fileType})
		}

		return nil
	}
}

// put writes one artefact through the file storer, matching writeSubtree at
// services/legacy/netsync/handle_block.go:876.
//
// A blob that already exists is success, not failure: two peers can deliver blocks
// sharing an identical run of transactions, which produces the same subtree under
// the same key.
// writeBytes is an artefact already serialised, written as it is.
func writeBytes(payload []byte) func(io.Writer) error {
	return func(dst io.Writer) error {
		_, err := dst.Write(payload)

		return err
	}
}

func (w *subtreeWriter) put(ctx context.Context, root chainhash.Hash, fileType fileformat.FileType, write func(io.Writer) error) error {
	storer, err := filestorer.NewFileStorer(ctx, w.logger, w.settings, w.store, root[:], fileType, options.WithDeleteAt(w.deleteAt()))
	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return nil
		}

		return errors.NewStorageError("[subtreeWriter][%s] failed to create %s file", root, fileType, err)
	}

	if err = write(storer); err != nil {
		storer.Abort(errors.NewProcessingError("[subtreeWriter][%s] write failed for %s", root, fileType))

		return errors.NewStorageError("[subtreeWriter][%s] failed writing %s", root, fileType, err)
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("[subtreeWriter][%s] failed closing %s", root, fileType, err)
	}

	return nil
}

// deleteAt is the height at which this block's subtree files may be removed.
func (w *subtreeWriter) deleteAt() uint32 {
	if w.heightUnknown {
		return w.dahOverride
	}

	return w.height + w.settings.GetSubtreeValidationBlockHeightRetention()
}

// Written lists every artefact this writer has put in the store, in write order.
func (w *subtreeWriter) Written() []writtenSubtree {
	return w.written
}

// DeleteAll removes everything this writer wrote. It is what a failed merkle root
// calls: nothing is reading these files, because no block references them until
// the block is handed over.
//
// One known consequence. A subtree file is keyed by its own root hash, so a failed
// block and a good block containing an identical run of transactions produce the
// same file, and this can remove one the good block legitimately wrote. It is
// rebuildable, so that is a performance edge case rather than a correctness one,
// and it is recorded here so it is not a surprise.
func (w *subtreeWriter) DeleteAll(ctx context.Context) error {
	var firstErr error

	// A data file still being streamed was never named, so nothing can read it.
	for index, file := range w.pending {
		file.Abort()
		delete(w.pending, index)
	}

	for _, entry := range w.written {
		if err := w.store.Del(ctx, entry.Hash[:], entry.FileType); err != nil && firstErr == nil {
			firstErr = errors.NewStorageError("[subtreeWriter][%s] failed deleting %s", entry.Hash, entry.FileType, err)
		}
	}

	w.written = w.written[:0]

	return firstErr
}
