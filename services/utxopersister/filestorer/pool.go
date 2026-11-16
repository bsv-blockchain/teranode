package filestorer

import (
	"bufio"
	"io"
	"sync"
)

// writerPool and readerPool recycle the bufio buffers this package hands out.
//
// Why they exist: bufio.NewWriterSize and bufio.NewReaderSize allocate the whole buffer up
// front, whether or not the payload ever fills it. At the shipped utxoPersister_buffer_size
// that is a quarter of a megabyte per buffer, and the utxo-persister path builds several
// writers and readers per consolidated block plus one of each per subtree. Without a pool
// every one of those is garbage as soon as the blob is published.
//
// Neither pool sets New. Get on an empty pool then returns a nil any, which the comma-ok
// assertions below handle; a New function would have to guess a buffer size, and every
// caller here needs a specific one.
var (
	writerPool sync.Pool
	readerPool sync.Pool
)

// acquireWriter returns a *bufio.Writer of size bytes writing to w, reusing a pooled buffer
// when one of exactly that size is available. bufio cannot resize a buffer, so the caller's
// size is never silently widened or narrowed.
//
// A pooled writer of another size goes back in the pool rather than being dropped: it is
// still the right buffer for whoever put it there. One Get inspects one buffer, so a
// mismatch still allocates; it no longer costs the pool the buffer it missed on.
func acquireWriter(w io.Writer, size int) *bufio.Writer {
	if bw, ok := writerPool.Get().(*bufio.Writer); ok {
		if bw.Size() == size {
			bw.Reset(w)
			return bw
		}

		writerPool.Put(bw)
	}

	return bufio.NewWriterSize(w, size)
}

// resetForPool detaches a writer from its destination before the buffer goes back in the
// pool. bufio.Writer.Reset clears the buffered bytes and the sticky error along with the
// destination, so neither the previous blob's data nor its failure can reach the next
// caller, and the pool retains no reference to the previous destination.
func resetForPool(bw *bufio.Writer) {
	bw.Reset(nil)
}

// AcquireReader returns a *bufio.Reader of size bytes reading from r, reusing a pooled
// buffer when one of exactly that size is available. Same sizing rule as acquireWriter,
// including the return of a size-mismatched buffer to the pool.
//
// Note that bufio.NewReaderSize enforces a small minimum buffer, so a size below it yields a
// reader whose Size is that minimum and which therefore never matches on the next acquire.
// That is a missed reuse at pathological sizes, not a correctness problem.
func AcquireReader(r io.Reader, size int) *bufio.Reader {
	if br, ok := readerPool.Get().(*bufio.Reader); ok {
		if br.Size() == size {
			br.Reset(r)
			return br
		}

		readerPool.Put(br)
	}

	return bufio.NewReaderSize(r, size)
}

// ReleaseReader returns a reader's buffer to the pool. The caller must be finished with br:
// once released the buffer can be handed to another goroutine, so releasing the same reader
// twice hands one buffer to two owners. Callers that can reach this from more than one path
// guard the call with a sync.Once.
func ReleaseReader(br *bufio.Reader) {
	if br == nil {
		return
	}

	resetReaderForPool(br)
	readerPool.Put(br)
}

// resetReaderForPool is the read-side counterpart to resetForPool. bufio.Reader.Reset
// overwrites the whole reader bar its buffer, so the unread bytes, the read position and any
// sticky error are discarded while the allocation - and therefore Size - survives.
func resetReaderForPool(br *bufio.Reader) {
	br.Reset(nil)
}
