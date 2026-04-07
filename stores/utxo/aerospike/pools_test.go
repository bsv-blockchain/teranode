package aerospike

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrChPool_AcquireRelease(t *testing.T) {
	ch := acquireErrCh()
	require.NotNil(t, ch)
	assert.Equal(t, 1, cap(ch))

	// Simulate normal usage: send then release
	ch <- nil
	releaseErrCh(ch)

	// Acquire again — should get a drained channel
	ch2 := acquireErrCh()
	require.NotNil(t, ch2)
	assert.Equal(t, 1, cap(ch2))

	// Channel should be empty (drained by releaseErrCh)
	select {
	case <-ch2:
		t.Fatal("channel should be empty after release")
	default:
		// expected
	}

	releaseErrCh(ch2)
}

func TestErrChPool_ReleaseWithoutSend(t *testing.T) {
	ch := acquireErrCh()
	// Release without sending — should not block or panic
	releaseErrCh(ch)
}

func TestBatchGetChPool_AcquireRelease(t *testing.T) {
	ch := acquireBatchGetCh()
	require.NotNil(t, ch)
	assert.Equal(t, 1, cap(ch))

	ch <- batchGetItemData{Err: nil}
	releaseBatchGetCh(ch)

	ch2 := acquireBatchGetCh()
	select {
	case <-ch2:
		t.Fatal("channel should be empty after release")
	default:
	}
	releaseBatchGetCh(ch2)
}

func BenchmarkErrCh_Pool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ch := acquireErrCh()
		ch <- nil
		<-ch
		releaseErrCh(ch)
	}
}

func BenchmarkErrCh_Alloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ch := make(chan error, 1)
		ch <- nil
		<-ch
		_ = ch
	}
}
