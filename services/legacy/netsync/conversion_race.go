package netsync

import (
	"bufio"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// When two copies of a block arrive at once, the first to complete is kept.
//
// The first copy to start takes the block's conversion slot and converts as it arrives. A second
// copy used to be read off the wire and thrown away, however fast it was: on 2026-09-25 a complete
// 4 GB copy of block 760,331 that arrived in 1m44s was drained because a copy at 2.7 MB/s had
// started first, and the chain waited 26 minutes more.
//
// Now the second copy is written to a side file. If it completes while the first is still
// converting, it takes over: the first stops at its next read of its peer's bytes or its next
// transaction, removes the subtree files it wrote, and drains the rest of its peer's bytes, and
// only then is the block converted from the side file. A first copy whose peer sends no byte for
// takeoverStallTimeout is disconnected (awaitTakeover). Two conversions of one block never write at the same time, because they share
// content-addressed subtree files; that is what stopped the chain at 707,177. If the first has
// already read its last transaction when the second completes, the first wins and the side file
// is deleted.

// duplicateCopySuffix names a side file in the park directory. Recover removes any left by a
// crash.
const duplicateCopySuffix = ".copy"

const (
	conversionRunning int32 = iota
	conversionFinishing
	conversionYielding
)

// admitKeptFaster is the admission path of a second copy that completed first and was converted.
const admitKeptFaster = "converted from disk: it completed before the copy that started first"

// takeoverStallTimeout is how long a copy that took over waits for the copy it took over while
// that copy waits for its peer's next byte. The converting copy stops at its next read, so only a
// peer that sends no byte keeps it. At the timeout that peer is disconnected, which ends the read.
// SV Node disconnects a peer that stalls the block download for DEFAULT_BLOCK_STALLING_TIMEOUT,
// 10 s, at less than DEFAULT_MIN_BLOCK_STALLING_RATE (validation.h:124-129,
// net_processing.cpp:5453-5467).
const takeoverStallTimeout = 10 * time.Second

// errYieldedToFasterCopy is what a converting copy's reader gives at its first read after a
// faster copy took over (yieldReader).
var errYieldedToFasterCopy = stderrors.New("a faster copy of the block took over")

// conversionCtl lets a faster copy of a block stop the copy being converted.
type conversionCtl struct {
	state atomic.Int32
	// inRead is true while the converting copy waits in a read of its peer's bytes (yieldReader).
	inRead atomic.Bool
	// sender is the peer the converting copy comes from, resolved to the peer that owes the
	// block when there is one, or nil when the reader names no peer. Set before the conversion
	// is registered.
	sender *peerpkg.Peer
	// cleaned is closed once the conversion will write nothing more and has removed what it
	// wrote: on the yield path, and on every other exit, since a conversion can be taken over and
	// then fail its own read before it gets back to the yield check.
	cleaned   chan struct{}
	cleanOnce sync.Once
}

// markCleaned releases a copy waiting to take over. Safe to call more than once.
func (c *conversionCtl) markCleaned() {
	c.cleanOnce.Do(func() { close(c.cleaned) })
}

// reading reports whether the converting copy waits in a read of its peer's bytes.
func (c *conversionCtl) reading() bool {
	return c.inRead.Load()
}

// yielding reports whether a faster copy has taken over.
func (c *conversionCtl) yielding() bool {
	return c.state.Load() == conversionYielding
}

// finish claims the block for this conversion once its last transaction is read. False means a
// faster copy took over first.
func (c *conversionCtl) finish() bool {
	return c.state.CompareAndSwap(conversionRunning, conversionFinishing)
}

// takeOver asks the conversion to yield. False means it has already claimed the finish.
func (c *conversionCtl) takeOver() bool {
	return c.state.CompareAndSwap(conversionRunning, conversionYielding)
}

// startConversion registers a conversion of hash, from the copy r reads, replacing any earlier
// one's registration.
func (sm *SyncManager) startConversion(hash chainhash.Hash, r io.Reader) *conversionCtl {
	c := &conversionCtl{cleaned: make(chan struct{})}

	if c.sender = sm.owingSender(r, hash); c.sender == nil {
		c.sender = deliveringPeer(r)
	}

	sm.conversionsMu.Lock()
	defer sm.conversionsMu.Unlock()

	if sm.conversions == nil {
		sm.conversions = make(map[chainhash.Hash]*conversionCtl)
	}

	sm.conversions[hash] = c

	return c
}

// endConversion removes c's registration, unless a later conversion of the block has replaced it.
func (sm *SyncManager) endConversion(hash chainhash.Hash, c *conversionCtl) {
	sm.conversionsMu.Lock()
	defer sm.conversionsMu.Unlock()

	if sm.conversions[hash] == c {
		delete(sm.conversions, hash)
	}
}

// conversionOf returns the conversion of hash in progress, or nil.
func (sm *SyncManager) conversionOf(hash chainhash.Hash) *conversionCtl {
	sm.conversionsMu.Lock()
	defer sm.conversionsMu.Unlock()

	return sm.conversions[hash]
}

// yieldReader reads a converting copy's bytes and gives errYieldedToFasterCopy at its first read
// after a faster copy took over, and passes each read after that, so the rest can be drained. The
// takeover used to be examined only between transactions: a peer that trickled the middle of a
// large transaction kept a complete copy waiting until that transaction ended. ctl is nil until
// the conversion is registered, and then reads pass. One goroutine reads it.
type yieldReader struct {
	r       io.Reader
	ctl     *conversionCtl
	yielded bool
}

func (y *yieldReader) Read(p []byte) (int, error) {
	if y.ctl == nil {
		return y.r.Read(p)
	}

	if !y.yielded && y.ctl.yielding() {
		y.yielded = true

		return 0, errYieldedToFasterCopy
	}

	y.ctl.inRead.Store(true)
	defer y.ctl.inRead.Store(false)

	return y.r.Read(p)
}

// awaitTakeover waits until the copy ctl controls has stopped and removed what it wrote. While
// that copy waits for its peer's next byte for takeoverStallTimeout, its peer's association is
// disconnected, which ends the read. The association goes through its primary
// (peer.DisconnectAssociation), as the park's drop does: when the copy's owner cannot be resolved
// ctl.sender is the delivering sub-peer, and disconnecting it alone left the primary connected.
// A wait while the copy does this node's own work, such as a store write, is not the peer's, and
// the wait continues. It gives an error only when sm.ctx ends.
func (sm *SyncManager) awaitTakeover(hash chainhash.Hash, ctl *conversionCtl) error {
	after := sm.takeoverAfter
	if after == nil {
		after = time.After
	}

	dropped := false

	for {
		var timeout <-chan time.Time
		if !dropped {
			timeout = after(takeoverStallTimeout)
		}

		select {
		case <-ctl.cleaned:
			return nil
		case <-sm.ctx.Done():
			return sm.ctx.Err()
		case <-timeout:
			if !ctl.reading() || ctl.sender == nil {
				continue
			}

			dropped = true

			sm.logger.Infof("[blockOnDisk][%s] a complete copy waited %s for the copy it took over, whose peer %s sent no byte; disconnecting that peer", hash, takeoverStallTimeout, ctl.sender)
			ctl.sender.DisconnectAssociation(fmt.Sprintf("sent no byte of block %s for %s while a complete copy waited", hash, takeoverStallTimeout))
		}
	}
}

// yieldToFasterCopy stops a conversion a faster copy has taken over: it removes what this copy
// wrote, lets the faster copy start, and reads the rest of this copy off the wire so the peer's
// connection stays in step.
func (sm *SyncManager) yieldToFasterCopy(hash chainhash.Hash, writer *subtreeWriter, rest io.Reader, ctl *conversionCtl, r io.Reader) (bool, error) {
	sm.deleteWrittenOnFailure(hash, writer)
	sm.streams.setStreamPath(r, admitRawDuplicate)
	ctl.markCleaned()

	sm.logger.Infof("[pipelineBlockSink][%s] another copy completed first; stopped converting this one and draining the rest", hash)

	// yieldReader gives errYieldedToFasterCopy once, at the first read after the takeover, and then
	// passes each read. When the sink saw the takeover before that read (the check at the top of
	// its loop, or a finish the takeover won), or when a read already waiting at the takeover ended
	// with an error of its own, that first read is the drain's. rest is a bufio.Reader, whose
	// WriteTo gives io.Discard's ReadFrom the reader under it, so the first drain stops at the
	// sentinel and a second one reads the rest. When the sink's own read already met the sentinel,
	// the buffer keeps it as its stored error, which WriteTo ignores, and the first drain reads it all.
	_, err := io.Copy(io.Discard, rest)
	if stderrors.Is(err, errYieldedToFasterCopy) {
		_, err = io.Copy(io.Discard, rest)
	}

	if err != nil {
		return false, err
	}

	sm.noteDrainedDuplicate(hash)

	return false, nil
}

// raceDuplicateCopy handles a copy that arrives while another converts: it keeps the bytes in a
// side file and, if this copy completes first, converts from it.
//
// Only a copy from a peer the download ledger says owes the block reaches here: admitPipelineSink
// drains every other copy, first or racing, before it asks for an admission slot.
func (sm *SyncManager) raceDuplicateCopy(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64,
	convert func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) (bool, error) {
	ctl := sm.conversionOf(hash)

	var dir string
	if sm.blockPark != nil {
		dir = sm.blockPark.dir
	}

	if ctl == nil || dir == "" {
		return sm.drainDuplicate(hash, r)
	}

	f, err := os.CreateTemp(dir, hash.String()+"-*"+duplicateCopySuffix)
	if err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not keep a second copy on disk, draining it: %v", hash, err)

		return sm.drainDuplicate(hash, r)
	}

	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	// The side file lives on the park's disk, so a failed write to it is the same class of
	// fault as a failed record write: ours, and never the peer's. The recorder tells the two
	// sides of the copy apart; a read failure is the peer's socket and is returned as it is
	// for the read loop, a write failure drops this copy and drains the rest, as the Flush
	// branch below already does.
	w := bufio.NewWriterSize(f, 1<<20)
	rec := &errRecordingWriter{w: w}

	if _, err = io.Copy(rec, r); err != nil {
		if rec.err == nil {
			return false, err
		}

		sm.logger.Warnf("[blockOnDisk][%s] could not write a second copy to disk, dropping it: %v", hash, rec.err)

		return sm.drainDuplicate(hash, r)
	}

	if err = w.Flush(); err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not write a second copy to disk, dropping it: %v", hash, err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	// The side copy's merkle root is checked before it stops the copy converting now. A block can
	// have two owners (ForgiveOwners keeps a forgiven owner), and an owner's corrupt copy used to
	// take over an honest conversion, fail its own root check after the honest copy had stopped,
	// and leave the block to be asked for again. The check reads the side file once more, the
	// same transactions the sink would hash, keeping O(log n) hashes.
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not reread a second copy from disk, dropping it: %v", hash, err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	// The side file is on this node's disk, so a failed read of it is ours and never the peer's.
	// The block stream returns the bare *fs.PathError (countingSource), which isLocalSinkFault
	// does not know as ours, and the read loop then rejected and disconnected the peer. The
	// recorder tells a read fault from a body the peer got wrong.
	side := sm.readSideCopy(f)

	root, mutated, err := streamedMerkleRoot(bufio.NewReaderSize(side, 1<<20), n)
	if err != nil {
		if side.err == nil {
			return false, err
		}

		sm.logger.Warnf("[blockOnDisk][%s] could not read a second copy back from disk, dropping it: %v", hash, side.err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	if !root.IsEqual(&header.MerkleRoot) {
		// The marker as the sink raises it for the same verdict (pipelineBlockSink).
		return false, errors.NewBlockInvalidError("[blockOnDisk][%s] a second copy's merkle root %s does not match header's %s; the copy converting now keeps the block", hash, root, header.MerkleRoot, errors.ErrBlockBodyMismatch)
	}

	// SV Node checks the root and then the flag, in this sequence (validation.cpp:5618-5627).
	if mutated {
		// A body with a repeated transaction can keep the header's root (CVE-2012-2459). The sink
		// refuses it on the duplicate (blockStreamBuilder.AddTx), but only after this copy has
		// stopped the honest one, which loses both. The marker is the one the sink raises.
		return false, errors.NewBlockInvalidError("[blockOnDisk][%s] a second copy repeats a transaction (CVE-2012-2459); the copy converting now keeps the block", hash, errors.ErrBlockBodyMismatch)
	}

	if !ctl.takeOver() {
		// The copy that started first has read its last transaction: it wins.
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	if err = sm.awaitTakeover(hash, ctl); err != nil {
		return false, err
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not reread a second copy from disk, dropping it: %v", hash, err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	sm.streams.setStreamPath(r, admitKeptFaster)
	sm.logger.Infof("[blockOnDisk][%s] a second copy completed before the copy being converted; converting it from disk", hash)

	side = sm.readSideCopy(f)

	converted, err := convert(hash, header, bufio.NewReaderSize(side, 1<<20), n)
	if err != nil && side.err != nil {
		// The copy that started first has stopped, so the block is not held. A storage error is
		// this node's fault (isLocalSinkFault): absorbLocalSinkFault keeps the peer and the
		// block is asked for again.
		return false, errors.NewStorageError("[blockOnDisk][%s] could not read a second copy back from disk", hash, side.err)
	}

	return converted, err
}

// readSideCopy reads a side file through a recorder of its read errors (errRecordingReader).
func (sm *SyncManager) readSideCopy(f io.Reader) *errRecordingReader {
	var r io.Reader = f
	if sm.sideCopyReader != nil {
		r = sm.sideCopyReader(f)
	}

	return &errRecordingReader{r: r}
}

// errRecordingReader remembers the first error its reader returned other than io.EOF, so a failed
// read of a local file can be told apart from a body the peer got wrong.
type errRecordingReader struct {
	r   io.Reader
	err error
}

func (e *errRecordingReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && err != io.EOF && e.err == nil {
		e.err = err
	}

	return n, err
}

// streamedMerkleRoot reads a block body, from the transaction count on, whose wire payload is n
// bytes with the header, and returns its merkle root. It holds one transaction and O(log n) hashes
// at a time: SV Node's ComputeMerkleRoot arithmetic (consensus/merkle.cpp:47-157),
// which hashes the last node of an odd level with itself. A body that does not parse, or that is
// longer or shorter than declared, is refused as the sink refuses it (blockTxStream).
//
// mutated is SV Node's flag of the same name (consensus/merkle.cpp:86): two equal nodes joined in
// the leaf pass. A body that repeats the last transactions of an odd level keeps the root of the
// honest body and sets it, for example [cb, b, c, c] for [cb, b, c].
func streamedMerkleRoot(r io.Reader, n int64) (root *chainhash.Hash, mutated bool, err error) {
	stream, err := newBlockTxStream(r, n-wire.MaxBlockHeaderPayload)
	if err != nil {
		return nil, false, err
	}

	var (
		inner [64]chainhash.Hash
		count uint64
	)

	join := func(left, right chainhash.Hash) chainhash.Hash {
		var buf [2 * chainhash.HashSize]byte

		copy(buf[:chainhash.HashSize], left[:])
		copy(buf[chainhash.HashSize:], right[:])

		return chainhash.DoubleHashH(buf[:])
	}

	for {
		_, txHash, nextErr := stream.Next()
		if errors.Is(nextErr, errBlockTxStreamDone) {
			break
		}

		if nextErr != nil {
			return nil, false, nextErr
		}

		h := *txHash
		count++

		level := 0
		for ; count&(1<<level) == 0; level++ {
			mutated = mutated || inner[level] == h
			h = join(inner[level], h)
		}

		inner[level] = h
	}

	if err = stream.RequireEnd(); err != nil {
		return nil, false, err
	}

	level := 0
	for count&(1<<level) == 0 {
		level++
	}

	h := inner[level]

	for count != 1<<level {
		h = join(h, h)
		count += 1 << level
		level++

		for count&(1<<level) == 0 {
			h = join(inner[level], h)
			level++
		}
	}

	return &h, mutated, nil
}

// deliveringPeerOwes reports whether the peer r's bytes come from is one the download ledger says
// owes hash. The ledger is keyed by association primaries, so a stream sub-peer is resolved first,
// as handleBlockOnDiskMsg resolves it. A reader that names no peer owes nothing.
func (sm *SyncManager) deliveringPeerOwes(r io.Reader, hash chainhash.Hash) bool {
	return sm.owingSender(r, hash) != nil
}

// owingSender returns the peer r's bytes come from, resolved to its association primary as the
// ledger keys it, when the ledger says that peer owes hash. It returns nil for a reader that names
// no peer and for a sender that does not owe the block.
func (sm *SyncManager) owingSender(r io.Reader, hash chainhash.Hash) *peerpkg.Peer {
	from := deliveringPeer(r)
	if from == nil || sm.blockDownloads == nil {
		return nil
	}

	owner := from
	if sm.peerStates != nil {
		_, owner, _ = sm.peerStateResolvingPrimary(from)
	}

	if !sm.blockDownloads.HasOwner(owner, hash) {
		return nil
	}

	return owner
}

// deliveringPeer returns the peer a sink's reader is reading from, through the stream tracker's
// wrapper when there is one, or nil when none is known (peerpkg.DeliveredBy).
func deliveringPeer(r io.Reader) *peerpkg.Peer {
	if c, ok := r.(countingReader); ok {
		r = c.r
	}

	return peerpkg.DeliveredBy(r)
}

// drainDuplicate reads a copy off the wire unwritten, as every second copy used to be.
func (sm *SyncManager) drainDuplicate(hash chainhash.Hash, r io.Reader) (bool, error) {
	if _, err := io.Copy(io.Discard, r); err != nil {
		return false, err
	}

	sm.noteDrainedDuplicate(hash)

	return false, nil
}

// setStreamPath records on a tracked stream which path its bytes finally took. r is the reader
// trackBlockStreams handed down; any other reader is left alone.
func (r *streamRegistry) setStreamPath(reader io.Reader, path string) {
	c, ok := reader.(countingReader)
	if r == nil || !ok || c.s == nil {
		return
	}

	r.mu.Lock()
	c.s.path = path
	r.mu.Unlock()
}

// isDuplicateCopyFile reports whether a park directory entry is a side file a crash left behind.
func isDuplicateCopyFile(name string) bool {
	return strings.HasSuffix(name, duplicateCopySuffix)
}
