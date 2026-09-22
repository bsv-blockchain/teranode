package filestorer

import (
	"bufio"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// stickyErrReader yields its payload and then fails every subsequent read with the same
// error, so a bufio.Reader over it can be driven into an error state while the bytes it
// already pulled are still buffered.
type stickyErrReader struct {
	data []byte
	err  error
}

func (r *stickyErrReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}

	n := copy(p, r.data)
	r.data = r.data[n:]

	return n, nil
}

// TestResetReaderForPoolClearsReader checks the release-time reset before anything else
// touches the reader. Checking after a Reset would prove nothing - Reset clears the buffered
// bytes on its own, so a helper that did nothing would pass.
func TestResetReaderForPoolClearsReader(t *testing.T) {
	src := &stickyErrReader{data: []byte("ABCDEFGH"), err: errors.NewProcessingError("source is down")}
	br := bufio.NewReaderSize(src, 16)

	// One byte consumed, the rest still buffered.
	first, err := br.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('A'), first)
	require.Positive(t, br.Buffered())

	// Drive the reader onto the source's error while those bytes are still buffered.
	_, err = br.Peek(16)
	require.Error(t, err)
	require.Positive(t, br.Buffered(), "the failed fill must leave the buffered bytes alone")

	resetReaderForPool(br)

	require.Zero(t, br.Buffered(), "release must drop the previous source's buffered bytes")

	// Acquisition-time check, separate from the above: the recycled reader serves the new
	// source and nothing else.
	br.Reset(strings.NewReader("ZY"))

	rest, err := io.ReadAll(br)
	require.NoError(t, err, "the previous source's error must not survive the release")
	require.Equal(t, "ZY", string(rest))
}

// TestAcquireReaderRespectsSize pins that a pooled buffer of one size is never handed to a
// caller that asked for another.
func TestAcquireReaderRespectsSize(t *testing.T) {
	large := AcquireReader(strings.NewReader("large"), 256*1024)
	require.Equal(t, 256*1024, large.Size())
	ReleaseReader(large)

	small := AcquireReader(strings.NewReader("small"), 4*1024)
	require.Equal(t, 4*1024, small.Size())
	ReleaseReader(small)
}

// poolReuseAttempts bounds the retries below. An attempt succeeds only if both Puts survive,
// which is 0.75 * 0.75 = 0.5625 under -race (sync/pool.go drops one Put in four there), so 24
// attempts put a false failure at about 1e-9. The pre-fix code cannot pass on any attempt: it
// drops the mismatched buffer every time, so there is nothing left in the pool to come back.
const poolReuseAttempts = 24

// drainPools empties both pools so an attempt can only get back its own buffer. Two
// collections are required - the first moves live items to the victim cache, the second drops
// them - and an explicit runtime.GC still collects while GOGC is off.
func drainPools() {
	runtime.GC()
	runtime.GC()
}

// pinPoolsForTest keeps a background collection from emptying a pool in the middle of an
// attempt and leaves one P for the per-P private slot to live on. Neither guarantees reuse -
// nothing can, see poolReuseAttempts - they only lower the miss rate.
func pinPoolsForTest(t *testing.T) {
	t.Helper()

	prevProcs := runtime.GOMAXPROCS(1)
	prevGC := debug.SetGCPercent(-1)

	t.Cleanup(func() {
		debug.SetGCPercent(prevGC)
		runtime.GOMAXPROCS(prevProcs)
	})
}

// TestAcquireReaderKeepsMismatchedBufferInPool pins that asking for a size the pool cannot
// serve does not destroy the buffer it holds. The consumers of this package use different
// sizes, so a mismatch is an ordinary event, not a pathological one.
func TestAcquireReaderKeepsMismatchedBufferInPool(t *testing.T) {
	pinPoolsForTest(t)

	const (
		largeSize = 256 * 1024
		smallSize = 4 * 1024
	)

	reused := false

	for attempt := 0; attempt < poolReuseAttempts && !reused; attempt++ {
		drainPools()

		large := AcquireReader(strings.NewReader("large"), largeSize)
		require.Equal(t, largeSize, large.Size())
		ReleaseReader(large)

		// Wrong size for this caller: the pooled buffer cannot be served and must go back.
		small := AcquireReader(strings.NewReader("small"), smallSize)
		require.Equal(t, smallSize, small.Size())

		again := AcquireReader(strings.NewReader("large again"), largeSize)
		require.Equal(t, largeSize, again.Size())
		reused = again == large

		ReleaseReader(small)
		ReleaseReader(again)
	}

	require.True(t, reused, "a size-mismatched buffer must go back in the pool, not be dropped")
}

// TestAcquireWriterKeepsMismatchedBufferInPool is the write-side counterpart, on the same
// terms.
func TestAcquireWriterKeepsMismatchedBufferInPool(t *testing.T) {
	pinPoolsForTest(t)

	const (
		largeSize = 256 * 1024
		smallSize = 4 * 1024
	)

	reused := false

	for attempt := 0; attempt < poolReuseAttempts && !reused; attempt++ {
		drainPools()

		large := acquireWriter(io.Discard, largeSize)
		require.Equal(t, largeSize, large.Size())
		resetForPool(large)
		writerPool.Put(large)

		small := acquireWriter(io.Discard, smallSize)
		require.Equal(t, smallSize, small.Size())

		again := acquireWriter(io.Discard, largeSize)
		require.Equal(t, largeSize, again.Size())
		reused = again == large

		resetForPool(small)
		writerPool.Put(small)
		resetForPool(again)
		writerPool.Put(again)
	}

	require.True(t, reused, "a size-mismatched buffer must go back in the pool, not be dropped")
}
