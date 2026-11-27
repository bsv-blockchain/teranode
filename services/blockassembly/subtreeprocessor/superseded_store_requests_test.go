package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// clearObserver records whether a request was already superseded when the map is first cleared.
type clearObserver struct {
	TxInpointsMap
	req               NewSubtreeRequest
	clears            int
	supersededAtClear bool
}

func (m *clearObserver) Clear() {
	if m.clears == 0 {
		m.supersededAtClear = m.req.Superseded()
	}

	m.clears++
	m.TxInpointsMap.Clear()
}

// A meta build that misses a parent because reset cleared the map must find its request already
// superseded, so reset flips the flag before it clears currentTxMap.
func TestResetSupersedesStoreRequestsBeforeClearingTheTxMap(t *testing.T) {
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blob_memory.New(),
		&blockchain.Mock{}, utxoStore, make(chan NewSubtreeRequest, 10))
	require.NoError(t, err)

	observer := &clearObserver{TxInpointsMap: stp.currentTxMap, req: NewSubtreeRequest{superseded: stp.supersedeToken()}}
	stp.currentTxMap = observer

	require.NoError(t, stp.reset(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2200000003,
		Nonce:          9203,
	}, nil, nil, false, func() error { return nil }))

	require.NotZero(t, observer.clears, "reset must clear currentTxMap")
	require.True(t, observer.supersededAtClear, "the request must already be superseded when currentTxMap is cleared")
}

// Every place that makes a store request stamps it with the current token, so a reset supersedes it.
func TestSupersedeStoreRequests_EveryRequestSiteIsStamped(t *testing.T) {
	addTxs := func(t *testing.T, stp *SubtreeProcessor, n int) {
		t.Helper()

		nodes := make([]subtreepkg.Node, n)
		inpoints := make([]*subtreepkg.TxInpoints, n)

		for i := range nodes {
			nodes[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 'x'}), Fee: 1, SizeInBytes: 100}
			inpoints[i] = &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{1}}}
		}

		stp.AddBatch(nodes, inpoints)
	}

	tests := []struct {
		name    string
		started bool
		trigger func(t *testing.T, stp *SubtreeProcessor)
	}{
		{"getSubtrees snapshot", true, func(t *testing.T, stp *SubtreeProcessor) {
			addTxs(t, stp, 3)

			require.Eventually(t, func() bool { return stp.TxCount() >= 3 }, 5*time.Second, time.Millisecond)

			go stp.GetChainedSubtrees()
		}},
		{"announcement ticker", true, func(t *testing.T, stp *SubtreeProcessor) {
			addTxs(t, stp, 3)
			stp.announcementTicker.Reset(10 * time.Millisecond)
		}},
		{"storeIncompleteSnapshot", false, func(t *testing.T, stp *SubtreeProcessor) {
			go stp.storeIncompleteSnapshot(t.Context(), stp.currentSubtree.Load(), stp.currentTxMap)
		}},
		{"storeAndAnnounceChainedSubtrees", false, func(t *testing.T, stp *SubtreeProcessor) {
			stp.chainedSubtrees = []*subtreepkg.Subtree{stp.currentSubtree.Load()}

			go func() { _ = stp.storeAndAnnounceChainedSubtrees(t.Context(), "test", 1, false, 0) }()
		}},
		{"parallelBuildRemainderSubtrees", false, func(t *testing.T, stp *SubtreeProcessor) {
			kept := make([]subtreepkg.Node, 2*stp.currentSubtree.Load().Size())
			for i := range kept {
				kept[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), byte(i >> 8), 'y'}), Fee: 1, SizeInBytes: 100}
			}

			go func() { _ = stp.parallelBuildRemainderSubtrees(t.Context(), kept, nil, false) }()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			utxoStoreURL, err := url.Parse("sqlitememory:///test")
			require.NoError(t, err)

			settings := test.CreateBaseTestSettings(t)

			utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
			require.NoError(t, err)

			requests := make(chan NewSubtreeRequest, 100)
			first := make(chan NewSubtreeRequest, 1)

			stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blob_memory.New(),
				&blockchain.Mock{}, utxoStore, requests)
			require.NoError(t, err)

			// the store side: answers every request and hands the first to the test
			go func() {
				for {
					select {
					case req := <-requests:
						select {
						case first <- req:
						default:
						}

						if req.ErrChan != nil {
							req.ErrChan <- nil
						}
					case <-ctx.Done():
						return
					}
				}
			}()

			if tt.started {
				stp.Start(ctx)
				t.Cleanup(func() { stp.Stop(context.Background()) })
			}

			tt.trigger(t, stp)

			select {
			case req := <-first:
				require.False(t, req.Superseded(), "a request of the current state is not superseded")

				stp.supersedeStoreRequests()

				require.True(t, req.Superseded(), "the request carries no token, so a reset cannot supersede it")
			case <-time.After(10 * time.Second):
				t.Fatal("no store request was made")
			}
		})
	}
}
