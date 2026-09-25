package blockvalidation

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/stretchr/testify/require"
)

func TestCatchupPeerSnapshot_ConcurrentOutageSharesFailure(t *testing.T) {
	var loads, reports atomic.Int32
	outage := errors.NewServiceError("registry unavailable")
	snapshot := &catchupPeerSnapshot{
		load: func() ([]*p2p.PeerInfo, bool, error) {
			loads.Add(1)
			return nil, false, outage
		},
		onError: func(error) { reports.Add(1) },
	}

	const workers = 32
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			_, _, err := snapshot.get()
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.Same(t, outage, err)
	}
	require.Equal(t, int32(1), loads.Load(), "subtree workers must share one failed discovery")
	require.Equal(t, int32(1), reports.Load())
}

func TestCatchupPeerSnapshot_FailureExpiresAfterLoadCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		loads := 0
		outage := errors.NewServiceError("registry unavailable")
		wantPeers := []*p2p.PeerInfo{{DataHubURL: "http://archival"}}
		snapshot := &catchupPeerSnapshot{
			load: func() ([]*p2p.PeerInfo, bool, error) {
				loads++
				if loads == 1 {
					// A slow RPC must not consume the negative-cache window itself.
					time.Sleep(2 * time.Second)
					return nil, false, outage
				}
				return wantPeers, true, nil
			},
		}
		_, _, err := snapshot.get()
		require.Same(t, outage, err)
		time.Sleep(time.Second - time.Nanosecond)
		peers, primaryPruned, err := snapshot.get()
		require.Error(t, err, "the failed lookup must stay cached for one second after completion")
		require.Same(t, outage, err)
		require.Nil(t, peers)
		require.False(t, primaryPruned)
		require.Equal(t, 1, loads)

		time.Sleep(time.Nanosecond)
		peers, primaryPruned, err = snapshot.get()
		require.NoError(t, err)
		require.Equal(t, wantPeers, peers)
		require.True(t, primaryPruned)
		require.Equal(t, 2, loads, "discovery must resume when the failure cache expires")

		time.Sleep(time.Hour)
		peers, primaryPruned, err = snapshot.get()
		require.NoError(t, err)
		require.Equal(t, wantPeers, peers)
		require.True(t, primaryPruned)
		require.Equal(t, 2, loads, "successful discovery stays cached for the block")
	})
}
