package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAMutatorsRebuildNeverReusesAnEarlierSnapshot pins the one property the forked-set
// accept depends on that singleflight quietly removes.
//
// rebuildOffChainSet reads the blocks table. singleflight.Group.Do hands a joining caller
// the in-flight leader's result rather than running the work again, which is right for the
// background refresh and wrong for a caller that has just changed on_main_chain: the
// leader's read began before that change committed, so the joiner installs a set that does
// not contain the block it just moved off the chain. The joiner then stamps
// lastSuccessfulRebuild and drops mainChainRebuilding over that set, and
// CheckBlockIsInCurrentChain answers "on the main chain" for a block that is not.
//
// The test drives the wrapper directly with a work function it can block, because the
// collision is a scheduling window that a store-level test cannot open deterministically.
// It asserts the end state a mutator needs: at least one rebuild BEGAN after the mutator
// asked for one. Reverting runRebuildObservingCallersWrite to a plain Do fails it.
func TestAMutatorsRebuildNeverReusesAnEarlierSnapshot(t *testing.T) {
	s := &SQL{}

	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})

	var (
		mu      sync.Mutex
		starts  int
		mutator time.Time // when the mutator asked for a rebuild
		begins  []time.Time
	)

	work := func() error {
		mu.Lock()
		starts++
		first := starts == 1
		begins = append(begins, time.Now())
		mu.Unlock()

		if first {
			close(leaderStarted)
			<-releaseLeader
		}

		return nil
	}

	var wg sync.WaitGroup

	// The background refresh gets in first and is still reading.
	wg.Add(1)

	go func() {
		defer wg.Done()
		require.NoError(t, s.runRebuild(context.Background(), work))
	}()

	<-leaderStarted

	// The mutator has just committed its on_main_chain change and now wants a rebuild.
	mu.Lock()
	mutator = time.Now()
	mu.Unlock()

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.NoError(t, s.runRebuildObservingCallersWrite(context.Background(), work))
	}()

	// Let the leader finish only once the mutator has had a chance to join it.
	time.Sleep(50 * time.Millisecond)
	close(releaseLeader)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	var beganAfterMutator bool

	for _, b := range begins {
		if b.After(mutator) {
			beganAfterMutator = true
		}
	}

	require.True(t, beganAfterMutator,
		"the mutator's rebuild reused a read that started before its own write: %d rebuild(s) ran, none began after the mutator asked", starts)
}

// TestTheBackgroundRefreshStillDeduplicates guards the other half. The retry must not turn
// every concurrent background tick into an extra full-chain walk, which on mainnet is a
// multi-second recursive query.
func TestTheBackgroundRefreshStillDeduplicates(t *testing.T) {
	s := &SQL{}

	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})

	var (
		mu     sync.Mutex
		starts int
	)

	work := func() error {
		mu.Lock()
		starts++
		first := starts == 1
		mu.Unlock()

		if first {
			close(leaderStarted)
			<-releaseLeader
		}

		return nil
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.NoError(t, s.runRebuild(context.Background(), work))
	}()

	<-leaderStarted

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.NoError(t, s.runRebuild(context.Background(), work))
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseLeader)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 1, starts, "two background refreshes should collapse into one read")
}
