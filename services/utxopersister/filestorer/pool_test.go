package filestorer

import (
	"bufio"
	"io"
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
