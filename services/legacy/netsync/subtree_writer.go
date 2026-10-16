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

// writtenSubtree records one artefact this block created in the store, so a
// failed block knows what to remove. An artefact that was already there is not
// recorded: two blocks, or two copies of one block, can share an identical run
// of transactions and so the same subtree under the same key, and the file then
// belongs to whichever block created it. Removing it would leave that block's
// record pointing at subtree data that is gone, and there is no second source
// (see subtreeWriter). The reverse holds too: a file this block created that
// another block has since adopted is not removed by this block's cleanup (see
// subtreeAdoptions).
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
// The structure's file type is a claim about trust, and this writer always makes
// the weaker one: FileTypeSubtreeToCheck, "still needs validating". Netsync has
// checked the merkle root against the header and nothing else; it has not
// validated a transaction, created a UTXO or spent one. The already-validated
// name, FileTypeSubtree, is written only by the component that did that work,
// after it did it: quick validation's writeSubtreeFilesFromTxs on the unified
// route and SubtreeValidation on the full route, both in services/blockvalidation.
// Stamping FileTypeSubtree here from the header proof alone, as this writer used
// to do below a checkpoint, made CheckBlockSubtrees skip every subtree of a block
// that then took full validation (its missing-subtree gate keys on
// Exists(FileTypeSubtree)), so no transaction of that block was ever created.
type subtreeWriter struct {
	logger   ulogger.Logger
	settings *settings.Settings
	store    blob.Store
	height   uint32
	// heightUnknown is true when this writer was built by
	// newSubtreeWriterUnresolvedHeight rather than newSubtreeWriter: height
	// above is not a real block height (it is never read in that case) and
	// dahOverride is what put() stamps every artefact with instead of
	// height + retention.
	heightUnknown bool
	dahOverride   uint32
	written       []writtenSubtree
	// adoptions is the registry shared by every writer of the same manager, so a file this
	// block adopted is never deleted by its creator's cleanup, nor the reverse. Nil gives the
	// writer a registry of its own (tests that build one writer alone).
	adoptions *subtreeAdoptions
	// adopted is every file this writer found already published while another writer of it
	// was still pending or converting, so it holds the file against that writer's cleanup.
	// DeleteAll gives the holds back; otherwise they end when no writer of the file is left
	// pending or live.
	adopted []subtreeHold
	// live is every file this writer created, until finish closes its window: while it is
	// open, a writer that adopts one of them holds it.
	live []writtenSubtree
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

func newSubtreeWriter(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, height uint32) *subtreeWriter {
	return &subtreeWriter{
		logger:   logger,
		settings: tSettings,
		store:    store,
		height:   height,
		written:  make([]writtenSubtree, 0, 3),
	}
}

// newSubtreeWriterUnresolvedHeight builds a writer for a block whose parent
// height pipelineParentHeight could not resolve at conversion time.
//
// The height feeds one decision, the delete-at-height, and it gets an explicit
// answer rather than being computed from a height that would otherwise default
// to zero. height + retention with a zero height would put the delete far below
// the chain tip, collecting the files almost immediately out from under a block
// still waiting. The caller supplies dah as the committed tip plus the read-ahead
// depth plus the retention, above any height this block can actually have, the
// same guarantee height + retention gives when the height is real.
//
// The structure file type is not a decision here or in newSubtreeWriter: see the
// type comment.
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

		// Always the "still needs validating" name; see the type comment for why
		// netsync never writes the other one.
		structureType := fileformat.FileTypeSubtreeToCheck

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

			key := writtenSubtree{Hash: *root, FileType: fileformat.FileTypeSubtreeData}
			w.registry().attempt(key)

			switch err = file.Commit(ctx, root[:], options.WithDeleteAt(w.deleteAt()), options.WithExclusivePublish()); {
			case err == nil:
				w.created(key)
			case errors.Is(err, errors.ErrBlobAlreadyExists):
				// Another block's file: Commit has discarded ours, and theirs is left alone.
				w.adopt(key)
			default:
				w.registry().abandoned(key)
				return errors.NewStorageError("[subtreeWriter][%s] failed committing %s", root, fileformat.FileTypeSubtreeData, err)
			}
		} else {
			artefacts = append([]struct {
				fileType fileformat.FileType
				write    func(io.Writer) error
			}{{fileformat.FileTypeSubtreeData, writeData}}, artefacts...)
		}

		for _, artefact := range artefacts {
			key := writtenSubtree{Hash: *root, FileType: artefact.fileType}
			w.registry().attempt(key)

			created, putErr := w.put(ctx, *root, artefact.fileType, artefact.write)
			if putErr != nil {
				w.registry().abandoned(key)
				return putErr
			}

			if created {
				w.created(key)
			} else {
				w.adopt(key)
			}
		}

		return nil
	}
}

// registry is the adoption registry this writer marks in: the manager's, or one of its own.
func (w *subtreeWriter) registry() *subtreeAdoptions {
	if w.adoptions == nil {
		w.adoptions = newSubtreeAdoptions()
	}

	return w.adoptions
}

// created records key as this writer's own, and opens its window as the file's live creator.
func (w *subtreeWriter) created(key writtenSubtree) {
	w.written = append(w.written, key)
	w.live = append(w.live, key)
	w.registry().created(key)
}

// adopt records that key was already published, keeping the hold the registry gives while
// another writer of it could still delete it.
func (w *subtreeWriter) adopt(key writtenSubtree) {
	if entry, held := w.registry().adopted(key); held {
		w.adopted = append(w.adopted, subtreeHold{key: key, entry: entry})
	}
}

// finish closes this writer's window on every file it created: its conversion is over, so it
// will not delete them, and a hold another writer took against it is no longer needed.
// pipelineBlockSink calls it when a copy fails; a copy that converted is finished when the wire
// layer settles it (holdUntilSettled). It is safe to call more than once.
func (w *subtreeWriter) finish() {
	for _, key := range w.live {
		w.registry().finished(key)
	}

	w.live = w.live[:0]
}

// writeBytes is an artefact already serialised, written as it is.
func writeBytes(payload []byte) func(io.Writer) error {
	return func(dst io.Writer) error {
		_, err := dst.Write(payload)

		return err
	}
}

// put writes one artefact through the file storer and reports whether this call
// created it.
//
// A blob that already exists is success, not failure: two peers can deliver blocks
// sharing an identical run of transactions, which produces the same subtree under
// the same key. It is reported as not created, so DeleteAll leaves it to the
// block that wrote it. Whether this call created the blob is decided by the
// store's publish, which this writer asks to be exclusive (options.WithExclusivePublish:
// the file store links the temp file to its name and fails if the name exists,
// stores/blob/file/file.go renameTempFile), not by the existence check in
// NewFileStorer, which is only a shortcut. The store can refuse the key at three
// moments, and each is a not-created answer.
func (w *subtreeWriter) put(ctx context.Context, root chainhash.Hash, fileType fileformat.FileType, write func(io.Writer) error) (bool, error) {
	storer, err := filestorer.NewFileStorer(ctx, w.logger, w.settings, w.store, root[:], fileType, options.WithDeleteAt(w.deleteAt()), options.WithExclusivePublish())
	if err != nil {
		// The key was taken before this call started.
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return false, nil
		}

		return false, errors.NewStorageError("[subtreeWriter][%s] failed to create %s file", root, fileType, err)
	}

	if err = write(storer); err != nil {
		storer.Abort(errors.NewProcessingError("[subtreeWriter][%s] write failed for %s", root, fileType))

		// The key was taken between NewFileStorer's existence check and the store's own
		// pre-check at the start of SetFromReader, which closed the pipe with this error;
		// a body larger than the storer's buffer meets it here, mid-write. Theirs stands.
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return false, nil
		}

		return false, errors.NewStorageError("[subtreeWriter][%s] failed writing %s", root, fileType, err)
	}

	if err = storer.Close(ctx); err != nil {
		// Another writer published the same key after the pre-check, while this body was
		// streaming. This writer's publish is exclusive, so exactly one of the two is told
		// it created the blob; this one was not, and the file is the other's to remove.
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return false, nil
		}

		return false, errors.NewStorageError("[subtreeWriter][%s] failed closing %s", root, fileType, err)
	}

	return true, nil
}

// deleteAt is the height at which this block's subtree files may be removed.
func (w *subtreeWriter) deleteAt() uint32 {
	if w.heightUnknown {
		return w.dahOverride
	}

	return w.height + w.settings.GetSubtreeValidationBlockHeightRetention()
}

// DeleteAll removes everything this writer created that no other block has adopted. It is
// what a failed block calls: this block's record is never handed over, so nothing reads these
// files through it. Files that were already in the store when this writer met them are left
// alone, because another block may reference them (see writtenSubtree). So are files this
// writer created that another writer has since adopted, which may belong to a block that has
// already committed (see subtreeAdoptions); they are left to their delete-at-height. The
// holds this writer took as an adopter are given back, since its record will not be read, and
// its window as a creator closes.
func (w *subtreeWriter) DeleteAll(ctx context.Context) error {
	var firstErr error

	// A data file still being streamed was never named, so nothing can read it.
	for index, file := range w.pending {
		file.Abort()
		delete(w.pending, index)
	}

	reg := w.registry()

	// This writer's own holds go first, so they never shield a file from itself.
	for _, hold := range w.adopted {
		reg.release(hold.key, hold.entry)
	}

	w.adopted = w.adopted[:0]

	defer w.finish()

	for _, entry := range w.written {
		left, err := reg.deleteUnlessAdopted(ctx, w.store, entry)
		if err != nil && firstErr == nil {
			firstErr = errors.NewStorageError("[subtreeWriter][%s] failed deleting %s", entry.Hash, entry.FileType, err)
		}

		if left {
			w.logger.Debugf("[subtreeWriter][%s] leaving %s: another block adopted it", entry.Hash, entry.FileType)
		}
	}

	w.written = w.written[:0]

	return firstErr
}
