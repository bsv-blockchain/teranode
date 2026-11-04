package netsync

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
)

// pipelineBlockSink converts a block as it streams off the socket.
//
// It is the receive half of the design: the wire layer hands over the parsed
// header and a reader positioned at the transaction count, and this drives the
// builder, the writer and the merkle accumulator without the block ever being a
// whole object in memory. Resident state is one subtree under construction plus
// one 32-byte hash per completed subtree.
//
// Ordering is load-bearing. The merkle root is compared against the header BEFORE
// the block is offered to anything downstream, because below a checkpoint quick
// validation skips script validation and outpoint-only mode skips the check
// binding a spend to its spender, so the root is the only thing asserting that
// this body is the body that header commits to.
//
// The bool it returns is the ONLY honest source of "did this delivery convert
// the block" anywhere in the system: true only on the path that actually wrote
// a converted record, false on every other return.
// handleBlockOnDiskMsg reads it off BlockBody.Converted rather than inferring
// conversion by asking whether some blob happens to exist for this hash — see
// that field's doc comment for why a blob's mere existence is the wrong
// question.
//
// The error it returns is a verdict on whose fault the failure was, and the
// wrapper around this sink (admitPipelineSink, through absorbLocalSinkFault and
// isLocalSinkFault) reads that verdict from the OUTERMOST teranode code alone,
// because teranode's errors.Is matches any code in the chain and these producers
// wrap foreign causes. So the rule for every producer this sink calls, the stream,
// the builder, the accumulator, the subtree writer and the park:
//
//   - the peer's fault carries ERR_BLOCK_INVALID (a body the header does not
//     commit to, a duplicate transaction, a count the body cannot hold) or
//     ERR_BLOCK_CORRUPT (a delivery whose length and transactions disagree). While
//     sm.ctx is live only these two codes reach the read loop as a read error, and
//     so a reject naming the block and the association dropped
//     (peer.BlockBodyRejectedError); once it is cancelled absorbLocalSinkFault
//     lets any code through to the read loop, see its doc comment.
//   - of those, exactly three refusals also wrap errors.ErrBlockBodyMismatch
//     inside the invalid verdict, and they are the only ones the legacy peer
//     server will ban a host for, once the wire checksum is verified, which the
//     streaming path cannot do yet (peer.streamingBlockHandler), so today none
//     of them bans: a merkle root the header does not carry (this
//     file), a duplicate transaction (block_stream_builder.go AddStreamedTx) and
//     a body with no coinbase (block_tx_stream.go newBlockTxStream). Each judges
//     a body that parsed to its end, which is where SV Node's CheckBlock scores
//     them at 100 points: bad-txnmrklroot and bad-txns-duplicate as
//     CorruptionOrDoS, bad-cb-missing as a plain DoS(100) (validation.cpp). No
//     other producer may raise the marker: a count the body
//     cannot hold, a shape fault of our own builder or a delivery cut short is
//     what SV Node logs as a deserialisation failure and never scores.
//   - a connection that ends mid-body carries no teranode code at all, whatever
//     ended it: a hang-up by FIN before the declared length is returned as the bare
//     io.ErrUnexpectedEOF (block_tx_stream.go endOfBody), and a socket that failed,
//     a reset or a closed connection, as the bare *net.OpError the socket returned
//     (block_tx_stream.go countingSource records it, the stream returns it before
//     judging anything). Both by identity, never wrapped: the read loop compares
//     the sentinel with == and type-asserts the OpError, and on a match logs a
//     disconnect rather than a reject. The wire layer (peer/wire_streaming.go
//     readBlockMessage) wraps only a coded verdict, so an uncoded error keeps its
//     identity all the way up.
//   - everything else is this node's: a store that failed (ERR_STORAGE_ERROR),
//     an invariant of our own builder or accumulator (ERR_PROCESSING,
//     ERR_SUBTREE_ERROR, ERR_TX_ERROR). Those are drained, the peer is kept, and
//     the block is asked for again.
//
// A producer keeps its verdict outermost and never wraps a peer verdict inside a
// local code: NewProcessingError("...", blockInvalidErr) would read as our fault,
// keep the peer and re-download a body that is genuinely bad.
func (sm *SyncManager) pipelineBlockSink(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
	// Every block converts. There is no fallback and nothing writes a whole
	// block body to one file any more.
	//
	// An unknown parent height does not stop it. The height feeds one decision,
	// the subtree files' delete-at-height, and that has an explicit answer
	// without it: the committed tip plus the read-ahead depth plus the
	// retention, which is above any height this block can actually have, so it
	// gives the same guarantee a known height does. Pruning is driven by the
	// committed chain, and a block waiting in the park is above it; nothing
	// else stops a delete computed from height + retention landing far below
	// the tip. The committer re-derives the height from the store before
	// committing anything, so a record carrying zero is corrected there.
	//
	// The subtree structure file type is not a decision this sink makes at all:
	// it is always .subtreeToCheck, see subtreeWriter.
	//
	// Two refusals used to sit here and both were wrong. One declined to convert
	// above the final checkpoint, on the grounds that a converted record carries
	// block ID zero and only quick validation assigns one: but the ordinary route
	// already passes zero up there, because its assignment sits inside a
	// quick-validation-only branch, and zero is the universal "assign
	// server-side" convention that full validation reads too. The other declined
	// when the parent height would not resolve, which was a constructor argument
	// with no answer for "unknown" rather than a rule about anything.
	height, resolved := sm.pipelineParentHeight(header.PrevBlock)

	// n is the block's declared wire payload, header included; r starts after the header, which
	// the wire layer has already read, so the stream is held to what is left.
	//
	// yieldReader lets a faster copy stop this one at its next read (conversion_race.go).
	yr := &yieldReader{r: r}

	stream, err := newBlockTxStream(yr, n-wire.MaxBlockHeaderPayload)
	if err != nil {
		return false, err
	}

	// The coinbase is the first transaction in the stream, and the builder needs
	// it before any other: it occupies slot zero of the first subtree.
	// Returned as it is, like every stream error below: the stream has already
	// said whose fault it was (an invalid encoding, a corrupt delivery, or the
	// bare EOF of a connection that ended), and wrapping it here in the invalid
	// code re-stamped a hang-up as a bad block.
	coinbase, coinbaseHash, err := stream.Next()
	if err != nil {
		return false, err
	}

	var writer *subtreeWriter
	if resolved {
		// The structure files are .subtreeToCheck whatever the header proof and
		// the route flags say. This sink has checked the merkle root and nothing
		// else, so that is the only claim it can make; block validation promotes
		// the file to .subtree after it validates the block, on both routes
		// (quick_validate.go writeSubtreeFilesFromTxs writes it with overwrite
		// allowed whenever the structure it read was not already a .subtree;
		// SubtreeValidation.go writes it on the full route). It used to be
		// stamped .subtree here from the proof alone, and a block that then took
		// full validation (unified flag off, or a store without outpoint-only
		// spends) had every subtree skipped by CheckBlockSubtrees' Exists(
		// FileTypeSubtree) gate, so none of its transactions was ever created.
		//
		// On the unified route the .subtreeToCheck written here stays beside the
		// promoted .subtree until pipelineBlockDelete or its delete-at-height:
		// quick validation's only Del is the forged-blob quarantine
		// (quick_validate_bind.go deleteSubtreeBlobConfirmed), and the full
		// route's removePeerSuppliedSubtreeToCheck skips files that were on disk
		// before the attempt. A retried commit therefore reads this file again
		// (findLocalSubtreeFile prefers .subtreeToCheck), anchors it and the
		// promoted .subtree beside it against their key (readSubtreeStructure),
		// and carries the promoted one so writeSubtreeFilesFromTxs does not write
		// it a second time. Both carry the same transaction hashes.
		writer = newSubtreeWriter(sm.logger, sm.settings, sm.subtreeStore, height)
	} else {
		writer = newSubtreeWriterUnresolvedHeight(sm.logger, sm.settings, sm.subtreeStore, sm.fallbackSubtreeDAH())
	}

	// One registry for every conversion, so no block's cleanup removes a file another adopted.
	// A copy that fails closes its writer's window as a creator here. One that converts keeps
	// it open until the wire layer settles the copy (holdUntilSettled), because a refusal after
	// the sink still runs this copy's cleanup in pipelineBlockDelete.
	writer.adoptions = sm.subtreeAdoptionsFor()

	sinkConverted := false

	defer func() {
		if sinkConverted {
			sm.holdUntilSettled(hash, writer)
		} else {
			writer.finish()
		}
	}()

	var (
		root          *chainhash.Hash
		subtreeHashes []chainhash.Hash
	)

	if stream.TxCount() <= 1 {
		// A coinbase-only block (txCount <= 1) has no transactions to stream, the
		// same shape newBlockStreamBuilder itself refuses (block_stream_builder.go):
		// running the builder anyway would emit one subtree whose root is the
		// go-subtree CoinbasePlaceholder constant, the SAME placeholder root for
		// every coinbase-only block in the chain, so every one of them would write
		// three files under the same three keys, overwriting each other, and hand
		// back a subtree list production never produces.
		//
		// The non-streaming path's own early return for this case
		// (prepareSubtrees, handle_block.go: "if txCount <= 1 { return subtrees,
		// nil, blockID, nil }") produces ZERO subtrees and ZERO files, so that is
		// what this branch matches: no builder, no writer.Emit call, subtreeHashes
		// stays nil. The merkle root is not skipped — for a coinbase-only block it
		// IS the coinbase transaction's own hash, so that is what is checked
		// below, on the same path every other block's root is checked on.
		root = coinbaseHash
	} else {
		// dedup is sized from stream.TxCount() but never past dedupInitialCapacity.
		// See newPipelineDedupMap's doc comment for why: that count is the peer's
		// own declared transaction count, and sizing a map from it without a cap is
		// what let a peer make this node allocate roughly 19 GB by declaring a number.
		// Past legacy_streamDedupMaxInMemoryEntries it moves to disk, so the
		// transactions a peer actually sends cannot grow it on the heap either.
		dedup := newPipelineDedupMap(stream.TxCount(), sm.settings, func(entries int) {
			sm.logger.Infof("[pipelineBlockSink][%s] block of %d declared transactions passed %d in its duplicate map; the map moved to disk", hash, stream.TxCount(), entries)
		})

		// Released before a drain as well as on return: draining a duplicate or the rest of a body
		// that yielded reads up to 4 GB, and the map, perhaps gigabytes of disk table, has no use
		// by then.
		releaseDedup := func() {
			if closeErr := dedup.Close(); closeErr != nil {
				sm.logger.Warnf("[pipelineBlockSink][%s] failed to close the disk-backed duplicate map: %v", hash, closeErr)
			}
		}

		defer releaseDedup()

		builder, buildErr := newBlockStreamBuilder(int(stream.TxCount()), sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree, coinbase, writer.Emit(sm.ctx), dedup,
			withSubtreeDataSink(writer.OpenData(sm.ctx)))
		if buildErr != nil {
			// newBlockStreamBuilder itself never calls Emit, so writer has written
			// nothing yet: this call is a no-op today. It is here anyway because the
			// brief's rule is "every failure path calls DeleteAll" without exception,
			// and leaving this one out is a trap for whoever adds work between
			// newSubtreeWriter and here.
			sm.deleteWrittenOnFailure(hash, writer)

			return false, buildErr
		}

		// A faster copy of this block may complete first and take over (conversion_race.go).
		ctl := sm.startConversion(hash, r)
		if ctl == nil {
			// Another conversion of this block is live: one that took over converts outside the
			// admission slot, so this copy was admitted beside it. Nothing of this copy has been
			// written yet; it is read off the wire and the block is left to the other.
			sm.deleteWrittenOnFailure(hash, writer)
			sm.streams.setStreamPath(r, admitRawDuplicate)
			sm.logger.Infof("[pipelineBlockSink][%s] another copy is converting this block; draining this one", hash)
			releaseDedup()

			return sm.drainDuplicate(hash, r)
		}

		yr.ctl = ctl

		// yield hands the block to a faster copy (conversion_race.go), which drains the rest of
		// this body, so the map goes first.
		yield := func() (bool, error) {
			releaseDedup()

			return sm.yieldToFasterCopy(hash, writer, stream.r, ctl, r)
		}

		defer sm.endConversion(hash, ctl)
		// Every failure path below has removed this copy's files before it returns, so a copy that
		// took over can start once this runs. Without it a failed read after a takeover left that
		// copy waiting until shutdown.
		defer ctl.markCleaned()

		for {
			if ctl.yielding() {
				return yield()
			}

			tx, txHash, size, streamErr := stream.NextStreamed(builder.BeginTx)
			if streamErr != nil {
				if errors.Is(streamErr, errBlockTxStreamDone) {
					break
				}

				// The read stopped for the takeover (yieldReader), maybe in the middle of a
				// transaction. A verdict on the bytes this peer sent is still judged: the encoding
				// is the peer's fault whoever finishes the block. The takeover copy proved the
				// block's body, so only the peer is judged. The read loop drops its association,
				// an encoding fault carries no ErrBlockBodyMismatch so nobody is banned, and a
				// delivery that converted nothing deletes nothing (pipelineBlockDelete). The test
				// is the wire layer's own (peer/wire_streaming.go readBlockMessage).
				if ctl.yielding() && !errors.Is(streamErr, errors.ErrBlockInvalid) && !errors.IsBlockCorrupt(streamErr) {
					return yield()
				}

				sm.deleteWrittenOnFailure(hash, writer)

				return false, streamErr
			}

			if addErr := builder.AddStreamedTx(tx, txHash, uint64(size)); addErr != nil { //nolint:gosec // a size read off the wire is never negative
				sm.deleteWrittenOnFailure(hash, writer)

				return false, addErr
			}
		}

		if !ctl.finish() {
			return yield()
		}

		var finishErr error

		root, subtreeHashes, finishErr = builder.Finish()
		if finishErr != nil {
			sm.deleteWrittenOnFailure(hash, writer)

			return false, finishErr
		}
	}

	// Before the record is written: a body the wire layer would refuse afterwards, one longer or
	// shorter than declared, must not be reported as converted, or pipelineBlockDelete removes what
	// is under this hash, which may be a parked copy of the same block that this conversion has just
	// overwritten.
	if err = stream.RequireEnd(); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, err
	}

	if !root.IsEqual(&header.MerkleRoot) {
		sm.deleteWrittenOnFailure(hash, writer)

		// ErrBlockBodyMismatch inside the verdict: the whole body was read
		// (RequireEnd passed just above) and its root is not the header's, which
		// is SV Node's bad-txnmrklroot, CorruptionOrDoS. The marker is what the
		// legacy peer server will ban on once the wire checksum is verified; see
		// the producer rule in this file's doc comment for the three sites that
		// may raise it.
		return false, errors.NewBlockInvalidError("[pipelineBlockSink][%s] merkle root %s does not match header's %s", hash, root, header.MerkleRoot, errors.ErrBlockBodyMismatch)
	}

	// Convert the wire header into a teranode header the same way
	// HandleBlockDirect does (handle_block.go:209-217): serialise it and
	// parse it back with model.NewBlockHeaderFromBytes, rather than inventing
	// a second conversion.
	var headerBytes bytes.Buffer
	if err = header.Serialize(&headerBytes); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to serialize header", hash, err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to create block header from bytes", hash, err)
	}

	// Finish returns subtree hashes as values; model.NewBlock wants pointers.
	// Take the address of a loop-local copy, never of the range variable —
	// ranging over subtreeHashes would otherwise leave every pointer aliasing
	// the same backing slot.
	subtreeHashPointers := make([]*chainhash.Hash, len(subtreeHashes))

	for i := range subtreeHashes {
		h := subtreeHashes[i]
		subtreeHashPointers[i] = &h
	}

	// Recorded as a model.Block rather than a bare subtree list because that is
	// exactly what the committer needs and exactly what serializes: (*Block).Bytes
	// writes the header, counts, subtree list and coinbase, and NewBlockFromBytes
	// reads them back. Keeping a narrower record would mean inventing a format for
	// it and then converting to this anyway.
	//
	// blockID is 0 on every route, not only below the checkpoint: it is the
	// universal "assign server-side" convention. Below the checkpoint,
	// quickValidateBlock assigns it and does the UTXO create/spend the record
	// carries no other way to do. Above it, buildAddBlockOpts
	// (services/blockvalidation/BlockValidation.go) returns nil for a zero ID
	// and AddBlock behaves exactly as it does for any other peer-fetched block,
	// which is never pre-assigned one either — see Step 1's finding in the task
	// report for the file:line evidence.
	verified, err := model.NewBlock(modelHeader, coinbase, subtreeHashPointers, stream.TxCount(), uint64(n), height, 0)
	if err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewProcessingError("[pipelineBlockSink][%s] failed to build block model", hash, err)
	}

	// The record is now genuinely on disk: a serialized model.Block under
	// fileformat.FileTypeBlock, a few hundred bytes rather than the gigabytes a
	// decoded block would take. This is what closes the defect this task
	// exists for. installStreamingBlockPath installs one gate/sink/delete
	// triple regardless of which sink is active, and after ANY sink returns
	// nil the wire layer still dispatches a blockOnDiskMsg that
	// handleBlockOnDiskMsg turns into a blockPark.AdoptWritten call
	// (streaming_install.go). Before this write existed, no body had ever been
	// written to the park's store for this hash, so AdoptWritten registered an
	// entry and charged the park's byte budget for a blob that did not exist,
	// and the drain later failed to read it. Writing here, before this
	// function can report success, means that by the time AdoptWritten runs
	// there is something real underneath it.
	if err = sm.blockPark.WriteConvertedBlock(sm.ctx, hash, verified); err != nil {
		sm.deleteWrittenOnFailure(hash, writer)

		return false, errors.NewStorageError("[pipelineBlockSink][%s] failed to write the converted record", hash, err)
	}

	sinkConverted = true

	return true, nil
}

// pipelineBlockDelete is the pipeline path's orphan-delete callback, installed
// alongside pipelineBlockSink, for a body converted and only then found unusable
// by the wire layer's own post-sink checks (the short-body check or the
// transaction-count read in readBlockMessage, services/legacy/peer/wire_streaming.go).
// With converted true that should no longer happen: the sink refuses both length
// cases before writing its record (see the end of this comment), so this is the
// backstop, not the expected route.
//
// converted is THIS call's own answer to "did the delivery that just failed
// actually convert anything" — blockBodySink's own return value for that one
// call, threaded through unchanged by the wire layer (BlockBody.Converted,
// deleteOrphanedBody). It is NOT re-derived by asking whether a converted
// record happens to exist for hash right now, which a fix round after this
// one's first cut found was still wrong: a hash can be re-requested and
// re-delivered while an EARLIER delivery for it is still genuinely parked,
// waiting on its own parent — ownership is released as soon as a delivery's
// sink call finishes, and the streaming gate accepts any hash the download
// ledger still holds a request for — so a second, unrelated delivery's own failure
// (a body that ends short, for example) must not read that earlier delivery's
// record back and delete the subtree files it names out from under it. Only
// when THIS call is known, from its own return value, to have written a
// record does this function go looking for what it wrote.
//
// The converting copy's writer, which holdUntilSettled keeps open until this
// verdict or the copy's acceptance, says exactly which subtree files this copy
// created, and DeleteAll removes those that no other block holds. The writers
// wait in a queue keyed by hash, which is not a delivery identity: a second
// copy of the block can convert after this copy's sink returned and before
// this verdict arrived. When two wait, this function cannot tell which copy it
// was called for, and it deletes nothing at all, the record included, rather
// than risk the copy about to be accepted. If both copies are then refused,
// the record stays on disk, and a restart's park recovery picks it up; its
// subtree files may have reached their delete-at-height by then, and the
// commit's completeness check drops the record so the block is downloaded
// again (FilesGone). A copy accepted and not yet parked counts as parked
// (settleAccepted), so a refusal of the other copy in that gap keeps the
// accepted copy's record too. Without a waiting writer (a record
// not written by this process's sink just now) it falls back to the record:
// ReadConverted hands back the subtrees the record names, and a file a writer
// is writing or holds is skipped.
//
// The delete probes all four file types under each root rather than
// recomputing which structure type the sink chose. subtreeWriter always writes
// FileTypeSubtreeToCheck now, but a record parked by an earlier build may
// name roots stamped FileTypeSubtree, and recomputing the type from the live
// header cache, as this used to, could pick the wrong one: the cache can lose
// a proof on a refill (header_cache.go Proven), so the type derived at delete
// time need not be the type derived at write time. Deleting a file that is not
// there is a no-op in every blob store this can run over (memory, file, S3 and
// HTTP each treat a missing key as deleted), and ErrNotFound is tolerated for
// any that does not.
//
// That guarantee has one hole, and two things close it. The record is written
// with overwrite allowed, so a redelivery of a block already parked converts,
// overwrites the parked record with an identical one, and reports converted. If
// the wire layer then refused it, deleting under the hash would take the parked
// block's record and every subtree file it names. The refusals left after a
// successful sink are both about length: declared bytes after the last
// transaction, and a body that stops short of its declared length and then hits
// EOF (the peer declares more than it sends and closes). The sink refuses both
// itself, before writing the record, by holding the stream to exactly the
// declared body length (blockTxStream.RequireEnd). The wire layer's
// transaction-count read cannot fail after a successful sink, because the sink
// has already read the same count. As a backstop for any refusal that still
// gets through, this function returns early, deleting nothing, while the park
// holds the block or has handed it out, so a refused copy cannot remove the
// parked copy's files.
//
// The converted record itself is deleted by blockPark.Delete, which every path
// that retires a park entry goes through, not only this discard path.
func (sm *SyncManager) pipelineBlockDelete(hash chainhash.Hash, converted bool) error {
	// A delivery that converted nothing wrote nothing: it was a drained copy of a block
	// another copy is converting or has converted. Deleting anything under the hash here
	// used to remove that other copy's record, and the block was then downloaded again.
	if !converted {
		return nil
	}

	// A copy of a block the park holds converts over the parked record with an identical one
	// and finds every subtree file already on disk, so it creates none. The files the record
	// names are the parked copy's, and deleting them left a record whose files were gone: the
	// parent's commit then dropped it (970,503 on 2026-10-10). A file this copy did create is
	// left to its delete-at-height.
	// The copy's writer, kept open by the sink until now (holdUntilSettled).
	writer, ambiguous := sm.takeSettling(hash)

	// Two converted copies of the block are waiting for their verdicts, and nothing says which
	// one this refusal is about. Both wrote the same record under the hash, so deleting it, or
	// either copy's files, could remove the copy that is about to be accepted. Leave all of it:
	// the files keep their delete-at-height, and the other verdict or the park sweep settles
	// both writers.
	if ambiguous {
		sm.logger.Warnf("[pipelineBlockDelete][%s] two converted copies are waiting and this refusal cannot say which; deleting nothing", hash)

		return nil
	}

	// A copy accepted but not yet parked (settleAccepted) counts as parked: the record under the
	// hash is the accepted copy's.
	//
	// The acceptance mark is read first. The consumer parks an accepted copy and only then clears
	// its mark, so a mark found clear means the park already holds the block, or has handed it
	// out, or is done with it. Read the other way round, the park could take the block and clear
	// the mark between the two reads, and the refusal deleted the accepted copy's record (review
	// of 76be891d6). The park's two states are read under one lock (Holds) for the same reason.
	if sm.acceptancePending(hash) || sm.blockPark.Holds(hash) {
		sm.logger.Infof("[pipelineBlockDelete][%s] a refused copy of a parked or accepted block; keeping the other copy's subtree files", hash)

		if writer != nil {
			writer.finish()
		}

		return nil
	}

	// The writer knows exactly which files this copy created, and its window as a creator is
	// still open, so holds other blocks took on them still stand: DeleteAll removes the files
	// this copy created that nobody holds, and nothing this copy merely adopted.
	if writer != nil {
		firstErr := writer.DeleteAll(sm.ctx)
		if firstErr != nil {
			firstErr = errors.NewStorageError("[pipelineBlockDelete][%s] failed deleting the subtree files this copy wrote", hash, firstErr)
		}

		sm.blockPark.Delete(sm.ctx, parkedBlock{hash: hash})

		return firstErr
	}

	// No writer is waiting: the record was not written by a sink of this process just now.
	// Fall back to what the record names, skipping any file a writer is writing or holds.
	var firstErr error

	record, err := sm.blockPark.ReadConverted(sm.ctx, hash)

	switch {
	case err == nil && record != nil:
		for _, root := range record.Subtrees {
			for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtree} {
				delErr := sm.deleteSubtreeFileUnlessAdopted(*root, ft)
				if delErr != nil && !errors.Is(delErr, errors.ErrNotFound) && firstErr == nil {
					firstErr = errors.NewStorageError("[pipelineBlockDelete][%s] failed deleting %s for subtree %s", hash, ft, root, delErr)
				}
			}
		}

	case err != nil && !errors.Is(err, errors.ErrNotFound):
		// converted being true means THIS call's own sink wrote a record,
		// so a not-found error here is not the ordinary case it would be
		// otherwise — it means the write this call just made cannot be
		// read back. Either way, this is a store timeout, a permit-pool
		// wait that ran out, or ReadConverted's own hash-mismatch refusal
		// — every one of which means the subtree files this block's sink
		// actually wrote are NOT being deleted here, exactly the failure
		// mode deleteWrittenOnFailure's own comment warns about for the
		// same reason: an unlogged cleanup failure is indistinguishable
		// from a cleanup that never needed to run.
		sm.logger.Warnf("[pipelineBlockDelete][%s] could not read back the record this delivery just converted, so its subtree files were not deleted: %v", hash, err)
	}

	// This delivery's own record, and the park's entry for it if it has one.
	sm.blockPark.Delete(sm.ctx, parkedBlock{hash: hash})

	return firstErr
}

// pipelineParentHeight resolves a block's height from its parent's hash,
// without requiring the parent to be a COMMITTED block.
//
// The ordinary out-of-order case is a parent that is still only in the
// header cache (sm.headerCache) — measured at roughly 91% of blocks on this
// node — because the header round can outrun body delivery. Asking only
// sm.blockchainClient.GetBlockHeader, which answers only for a committed
// block (parentIsInChain in streaming_install.go makes the identical call for
// exactly that meaning), treated that ordinary case as a fault.
//
// The cache is checked first because it is the common case and never blocks:
// it holds its own lock, released before the fallback runs, because a
// blockchain client call can take an unbounded time.
func (sm *SyncManager) pipelineParentHeight(parent chainhash.Hash) (uint32, bool) {
	if height, ok := sm.headerCache.HeightOf(parent); ok && height >= 0 {
		return uint32(height) + 1, true
	}

	if sm.blockchainClient == nil {
		return 0, false
	}

	_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)
	if err != nil || meta == nil {
		return 0, false
	}

	return meta.Height + 1, true
}

// fallbackSubtreeDAH returns the delete-at-height newSubtreeWriterUnresolvedHeight
// stamps every artefact with, for a block whose parent height
// pipelineParentHeight could not resolve.
//
// committedTip's height is the chain's actual tip right now — never negative
// in practice, floored at 0 defensively. Added to it is the widest the
// download walk can read ahead of that tip: legacy_blockDownloadWindow, the
// node-wide ceiling on outstanding block requests and wantedBlocks' own
// read-ahead depth (wanted_range_assign.go), floored at 1 so a misconfigured
// 0 cannot zero the whole sum. No block this node holds — parked,
// mid-conversion, or committed — can have a
// height above tip+BlockDownloadWindow, so adding the configured retention on
// top gives a delete-at-height strictly above any height this specific block
// can actually turn out to have, exactly the guarantee height + retention
// gives when the height is known.
//
// That guarantee assumes tip only moves forward. A reorg of depth R can drop
// the reported tip by R while a block already in hand was fetched against the
// old chain, so it can sit as high as (old tip) + BlockDownloadWindow, i.e.
// (new tip) + R + BlockDownloadWindow — R above what this function accounts
// for. The bound still holds whenever the configured retention exceeds R,
// since retention is the only slack in the sum; it stops holding once a reorg
// goes deeper than the retention this function reads, which tracks
// global_blockHeightRetention (288 by default) plus this service's own
// adjustment. Nothing here detects or guards against that; it is a known gap,
// not a bug to fix in this function.
func (sm *SyncManager) fallbackSubtreeDAH() uint32 {
	tip, _, _ := sm.committedTip()
	if tip < 0 {
		tip = 0
	}

	depth := 1

	var retention uint32

	// One guard covering both settings reads, not just the first: reading
	// GetSubtreeValidationBlockHeightRetention through a nil sm.settings is
	// the same guard-page hazard this package already tests for elsewhere
	// (settings/legacy_settings.go's fields sit well past the 4096-byte page a
	// Go process leaves unmapped at low addresses, so a nil-settings read this
	// far in can fault outside what the runtime turns back into an ordinary,
	// catchable panic — see TestInstallStreamingBlockPath_NilSettingsDoesNotPanic).
	if sm.settings != nil {
		if sm.settings.Legacy.BlockDownloadWindow > depth {
			depth = sm.settings.Legacy.BlockDownloadWindow
		}

		retention = sm.settings.GetSubtreeValidationBlockHeightRetention()
	}

	return uint32(tip) + uint32(depth) + retention
}

// dedupInitialCapacity caps the pipeline dedup map's pre-sizing hint, so a
// peer's declared transaction count can never size it past this — see
// newPipelineDedupMap.
//
// ~1,048,576 slots costs roughly 50 MB with NewSplitSwissMapUint64's own 20%
// per-bucket headroom (go-tx-map tx_map.go). A block larger than this still
// dedups correctly: Put is what makes the CVE-2012-2459 check exact, not the
// pre-sizing, so a map that starts smaller than the block just grows through
// its own ordinary rehashing as real transactions arrive, the same as any Go
// map given a low capacity hint.
const dedupInitialCapacity uint32 = 1 << 20

// newPipelineDedupMap returns a fresh duplicate-transaction map for the
// pipeline sink, pre-sized for the block's declared transaction count but never
// past dedupInitialCapacity or the in-memory cap.
//
// Sizing for the declared count is what keeps an ordinary block cheap. Every
// block used to get the full dedupInitialCapacity map, about 48 MB: on mainnet
// on 2026-09-25 that was 16 GB of every 90 GB the node allocated, for blocks
// of a few thousand transactions. It cannot undersize the map for an honest
// block, because the stream stops at the declared count, and a map smaller
// than its block would still only grow.
//
// The declared count is never trusted beyond the cap because it is checked only
// against the wire payload ceiling (services/legacy/config.go
// maxWireBlockPayload, 4,000,000,000 bytes) against minSerializedTxSize — so a
// peer may declare tens of millions of transactions in a body it then never
// sends. txmap.NewSplitSwissMapUint64 pre-sizes all 1024 buckets eagerly from
// whatever length it is given (go-tx-map tx_map.go NewSplitSwissMapUint64), so
// sizing this map from that declared count would allocate gigabytes before a
// single transaction byte arrives — gated by nothing but a hash and a
// proof-of-work check that any sync peer already passes.
//
// The cap on the initial size does not bound what the map grows to, and the
// transactions the peer does send all go into it before the merkle root can be
// checked, which needs the whole body. A 4 GB body of 10-byte transactions grew
// it to about 19 GB (issue 4945). So past maxInMemory entries
// (legacy_streamDedupMaxInMemoryEntries, see pipelineDedupMap) the map moves to
// a disk-backed table under <dataFolder>/legacy-dedup. tSettings may be nil, as
// on struct-literal managers in tests; the map then keeps the default cap and
// spills under os.TempDir. onSpill, when set, is called once, with the entry
// count, as the map moves.
func newPipelineDedupMap(declared uint64, tSettings *settings.Settings, onSpill func(entries int)) *pipelineDedupMap {
	m := &pipelineDedupMap{
		maxInMemory: int(dedupInitialCapacity),
		onSpill:     onSpill,
	}

	if tSettings != nil {
		if tSettings.Legacy.StreamDedupMaxInMemoryEntries > 0 {
			m.maxInMemory = tSettings.Legacy.StreamDedupMaxInMemoryEntries
		}

		m.dir = filepath.Join(tSettings.DataFolder, "legacy-dedup")
	}

	m.mem = txmap.NewSplitSwissMapUint64(uint32(min(declared, uint64(dedupInitialCapacity), uint64(m.maxInMemory)))) //nolint:gosec // capped at dedupInitialCapacity, which fits uint32

	return m
}

const (
	// dedupSpillGrowAt is the load at which a spilled table grows. The mmaphash default is to grow
	// only when a segment is full, which suits block validation's tables, sized from the real
	// transaction count; a spilled table starts at the cap and grows with the body, and probing a
	// nearly full segment cost about a microsecond a Put on the peer's read loop.
	dedupSpillGrowAt = 0.75

	// dedupDiskCheckEvery is how many Puts a spilled map makes between checks of the free disk.
	// Each Put writes at most one 4 KiB page, so 16,384 of them write at most 64 MiB.
	dedupDiskCheckEvery = 1 << 14

	// dedupDiskSpare is the disk a spilled map leaves free beyond what every spilled map has
	// claimed: room for the pages written between checks, and for everything else on that disk.
	dedupDiskSpare = 1 << 30
)

// dedupDiskClaimed is the disk claimed by the spilled duplicate maps of every block converting
// at once: twice each map's backing files, the size of its next grow. A map refuses to go on when
// the free disk does not cover all of it, because a write to a sparse mmap file on a full disk is
// a SIGBUS, which kills the process (stores/mmaphash mapRegion), and the bytes driving the table
// come from a peer whose body has not yet been checked against its header.
var dedupDiskClaimed atomic.Int64

// dedupDiskFree reports the bytes of dir's filesystem available to this process. A variable so a
// test can stand in for a full disk.
var dedupDiskFree = func(dir string) (uint64, error) {
	if dir == "" {
		dir = os.TempDir()
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}

	return uint64(stat.Bavail) * uint64(stat.Bsize), nil //nolint:gosec,unconvert // field types differ by platform; neither is negative
}

// pipelineDedupMap is the pipeline sink's duplicate-transaction map: in memory up to maxInMemory
// entries, then a memory-mapped table on disk (model.DiskTxMapUint64) for the rest of the block.
//
// Put is its only write, and is also the duplicate check: the in-memory map refuses a repeat with
// txmap.ErrHashAlreadyExists and the disk table with errors.ErrTxExists, and the builder reads both
// as a duplicate. It is deliberately not a txmap.TxMap: a write that bypassed Put would bypass the
// cap, and the disk table does not implement Keys, Iter or Clear.
//
// Not safe for concurrent use: one block's builder is its only caller.
type pipelineDedupMap struct {
	mem  txmap.TxMap
	disk *model.DiskTxMapUint64

	// puts is the entries put so far. Counted here rather than read from Length, which on the split
	// map sums 1,025 bucket counts and would run once per transaction.
	puts        int
	maxInMemory int
	dir         string
	onSpill     func(entries int)

	// claimed is this map's share of dedupDiskClaimed.
	claimed int64
	closed  bool
}

// Put records hash, moving the map to disk first when it already holds maxInMemory entries. A
// repeat returns the map's duplicate error. Any other error is this node's fault.
func (m *pipelineDedupMap) Put(hash chainhash.Hash, value uint64) error {
	if m.closed {
		return errors.NewProcessingError("[pipelineDedupMap] Put after Close")
	}

	if m.disk == nil && m.puts >= m.maxInMemory {
		if err := m.spill(); err != nil {
			return err
		}
	}

	var err error
	if m.disk == nil {
		err = m.mem.Put(hash, value)
	} else {
		err = m.disk.Put(hash, value)
	}

	if err != nil {
		return err
	}

	m.puts++

	if m.disk != nil && m.puts%dedupDiskCheckEvery == 0 {
		return m.claimDisk()
	}

	return nil
}

// spill copies every entry to a new disk table and makes it the map in use, letting the in-memory
// one go. A failure is this node's (ERR_STORAGE_ERROR), never the peer's: the block is drained and
// asked for again, and the peer is kept.
func (m *pipelineDedupMap) spill() error {
	disk, err := model.NewDiskTxMapUint64(model.DiskTxMapUint64Options{
		BasePaths: []string{m.dir},
		Prefix:    "legacy-dedup",
		// Sized for what moves, and grown from there by the entries that arrive; never from the
		// declared count, which is the peer's to choose.
		FilterCapacity: uint(m.maxInMemory), //nolint:gosec // maxInMemory is positive
		GrowAt:         dedupSpillGrowAt,
	})
	if err != nil {
		return errors.NewStorageError("[pipelineDedupMap] failed creating the disk-backed duplicate map in %q", m.dir, err)
	}

	m.disk = disk

	// On a failure below the table is released here, and a release that fails leaves m.disk set
	// for the sink's deferred Close to retry and log.
	if err = m.claimDisk(); err != nil {
		_ = m.releaseDisk()

		return err
	}

	var putErr error

	m.mem.Iter(func(hash chainhash.Hash, value uint64) bool {
		putErr = disk.Put(hash, value)

		return putErr != nil
	})

	if putErr != nil {
		_ = m.releaseDisk()

		return errors.NewStorageError("[pipelineDedupMap] failed moving the duplicate map to disk", putErr)
	}

	m.mem = nil

	if m.onSpill != nil {
		m.onSpill(m.puts)
	}

	return nil
}

// claimDisk updates this map's claim on the disk, twice its backing files, and fails with a
// storage error when the free disk does not cover every spilled map's claim plus dedupDiskSpare.
func (m *pipelineDedupMap) claimDisk() error {
	want := 2 * m.disk.FileBytes()
	total := dedupDiskClaimed.Add(want - m.claimed)
	m.claimed = want

	free, err := dedupDiskFree(m.dir)
	if err != nil {
		return errors.NewStorageError("[pipelineDedupMap] failed reading the free disk in %q", m.dir, err)
	}

	if free < uint64(total)+dedupDiskSpare { //nolint:gosec // total is a sum of positive claims
		return errors.NewStorageError("[pipelineDedupMap] %d bytes free in %q, short of the %d the disk-backed duplicate maps claim plus %d spare", free, m.dir, total, dedupDiskSpare)
	}

	return nil
}

// Close releases both maps and this map's disk claim. Safe to call more than once; a failed
// release of the disk table can be retried.
func (m *pipelineDedupMap) Close() error {
	m.closed = true
	m.mem = nil

	return m.releaseDisk()
}

// releaseDisk closes the disk table, if there is one, and gives up this map's disk claim. The
// in-memory map, if still held, stays in use.
func (m *pipelineDedupMap) releaseDisk() error {
	if m.disk == nil {
		return nil
	}

	if err := m.disk.Close(); err != nil {
		return errors.NewStorageError("[pipelineDedupMap] failed closing the disk-backed duplicate map", err)
	}

	m.disk = nil
	dedupDiskClaimed.Add(-m.claimed)
	m.claimed = 0

	return nil
}

// deleteWrittenOnFailure calls writer.DeleteAll and, if the delete itself
// fails, logs a single-line warning naming the block hash rather than
// discarding the error silently. subtreeWriter's own doc comment explains why
// an orphaned subtree file is dangerous (it is content-addressed and shared
// with any other block carrying the same run of transactions, which is also
// why DeleteAll leaves a file another block adopted); a cleanup
// failure that goes unlogged defeats the point of calling DeleteAll at all,
// since nothing else will ever tell an operator those files are still there.
func (sm *SyncManager) deleteWrittenOnFailure(hash chainhash.Hash, writer *subtreeWriter) {
	if delErr := writer.DeleteAll(sm.ctx); delErr != nil {
		sm.logger.Warnf("[pipelineBlockSink][%s] failed to delete subtree files after a failed block: %v", hash, delErr)
	}
}

// isLocalSinkFault reports whether an error from pipelineBlockSink is this node's
// own fault rather than the peer's. It reads the verdict pipelineBlockSink's doc
// comment says every producer keeps outermost.
//
// The outermost teranode code is used, not errors.Is, because teranode's Is
// matches any code in the chain and the producers wrap foreign causes: an fs
// error inside a StorageError, io.ErrUnexpectedEOF inside a BlockInvalid. The
// outermost code is the verdict; what it wraps is the evidence.
//
// A non-teranode error (a bare io.EOF or io.ErrUnexpectedEOF from a connection
// that ended, a net.OpError) is not ours: it is returned to the read loop
// unchanged so peer.shouldHandleReadError can see it by identity. ERR_BLOCK_INVALID
// and ERR_BLOCK_CORRUPT are the peer's. Every other code is ours, and so is any
// code added later: the default keeps the peer, which is the safe direction.
//
// errors.IsTransientLocalError is not used because it would miss ERR_PROCESSING,
// ERR_SUBTREE_ERROR and ERR_TX_ERROR, which the stream builder and the merkle
// accumulator return for faults of this node's own.
func isLocalSinkFault(err error) bool {
	if err == nil {
		return false
	}

	var tErr *errors.Error
	if !errors.As(err, &tErr) || tErr == nil {
		return false
	}

	switch tErr.Code() {
	case errors.ERR_BLOCK_INVALID, errors.ERR_BLOCK_CORRUPT:
		return false
	default:
		return true
	}
}
