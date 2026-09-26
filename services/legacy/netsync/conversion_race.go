package netsync

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// When two copies of a block arrive at once, the first to complete is kept.
//
// The first copy to start takes the block's conversion slot and converts as it arrives. A second
// copy used to be read off the wire and thrown away, however fast it was: on 2026-09-25 a complete
// 4 GB copy of block 760,331 that arrived in 1m44s was drained because a copy at 2.7 MB/s had
// started first, and the chain waited 26 minutes more.
//
// Now the second copy is written to a side file. If it completes while the first is still
// converting, it takes over: the first stops at its next transaction, removes the subtree files it
// wrote, and drains the rest of its peer's bytes, and only then is the block converted from the
// side file. Two conversions of one block never write at the same time, because they share
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

// conversionCtl lets a faster copy of a block stop the copy being converted.
type conversionCtl struct {
	state atomic.Int32
	// cleaned is closed once a yielding conversion has removed everything it wrote.
	cleaned chan struct{}
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

// startConversion registers a conversion of hash, replacing any earlier one's registration.
func (sm *SyncManager) startConversion(hash chainhash.Hash) *conversionCtl {
	c := &conversionCtl{cleaned: make(chan struct{})}

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

// yieldToFasterCopy stops a conversion a faster copy has taken over: it removes what this copy
// wrote, lets the faster copy start, and reads the rest of this copy off the wire so the peer's
// connection stays in step.
func (sm *SyncManager) yieldToFasterCopy(hash chainhash.Hash, writer *subtreeWriter, rest io.Reader, ctl *conversionCtl, r io.Reader) (bool, error) {
	sm.deleteWrittenOnFailure(hash, writer)
	sm.streams.setStreamPath(r, admitRawDuplicate)
	close(ctl.cleaned)

	sm.logger.Infof("[pipelineBlockSink][%s] another copy completed first; stopped converting this one and draining the rest", hash)

	if _, err := io.Copy(io.Discard, rest); err != nil {
		return false, err
	}

	sm.noteDrainedDuplicate(hash)

	return false, nil
}

// raceDuplicateCopy handles a copy that arrives while another converts: it keeps the bytes in a
// side file and, if this copy completes first, converts from it.
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

	w := bufio.NewWriterSize(f, 1<<20)
	if _, err = io.Copy(w, r); err != nil {
		return false, err
	}

	if err = w.Flush(); err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not write a second copy to disk, dropping it: %v", hash, err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	if !ctl.takeOver() {
		// The copy that started first has read its last transaction: it wins.
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	select {
	case <-ctl.cleaned:
	case <-sm.ctx.Done():
		return false, sm.ctx.Err()
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		sm.logger.Warnf("[blockOnDisk][%s] could not reread a second copy from disk, dropping it: %v", hash, err)
		sm.noteDrainedDuplicate(hash)

		return false, nil
	}

	sm.streams.setStreamPath(r, admitKeptFaster)
	sm.logger.Infof("[blockOnDisk][%s] a second copy completed before the copy being converted; converting it from disk", hash)

	return convert(hash, header, bufio.NewReaderSize(f, 1<<20), n)
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
