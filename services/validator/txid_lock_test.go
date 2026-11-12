package validator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestTxidLocks_SameIDExcludesAndOtherIDDoesNot(t *testing.T) {
	var locks txidLocks

	a := chainhash.Hash{1}
	b := chainhash.Hash{2}

	unlockA := locks.lock(&a)

	var entered atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		unlock := locks.lock(&a)
		entered.Store(true)
		unlock()
	}()

	otherDone := make(chan struct{})

	go func() {
		defer close(otherDone)

		locks.lock(&b)()
	}()

	select {
	case <-otherDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a lock on a different id must not wait for the first")
	}

	time.Sleep(50 * time.Millisecond)
	require.False(t, entered.Load(), "the second holder of the same id must wait")

	unlockA()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter must proceed once the holder releases")
	}

	require.True(t, entered.Load())
	require.Equal(t, 0, locks.size(), "no entry outlives its holders")
}

func TestTxidLocks_ProtectsACriticalSectionUnderContention(t *testing.T) {
	var locks txidLocks

	id := chainhash.Hash{7}
	inside := 0
	maxInside := 0
	total := 0

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			unlock := locks.lock(&id)
			inside++
			maxInside = max(maxInside, inside)
			total++
			time.Sleep(time.Millisecond)
			inside--
			unlock()
		}()
	}

	wg.Wait()

	require.Equal(t, 1, maxInside, "never more than one holder at a time")
	require.Equal(t, 50, total)
	require.Equal(t, 0, locks.size())
}

func (l *txidLocks) size() int {
	n := 0

	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].entries)
		l.shards[i].mu.Unlock()
	}

	return n
}
