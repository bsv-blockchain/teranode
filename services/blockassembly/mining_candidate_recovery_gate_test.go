package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// Pause only snapshot acquisition, retaining the real dispatcher, storage,
// publication lock and mmap lease lifecycle underneath it.
type recoveryCandidateSnapshotBarrier struct {
	subtreeprocessor.Interface
	beforePrecomputed func()
	beforeIncomplete  func()
	acquiredLease     *subtreeprocessor.MiningSnapshotLease
}

func (s *recoveryCandidateSnapshotBarrier) GetPrecomputedMiningData() *subtreeprocessor.PrecomputedMiningData {
	if s.beforePrecomputed != nil {
		s.beforePrecomputed()
	}
	data := s.Interface.GetPrecomputedMiningData()
	if data != nil {
		s.acquiredLease = data.Lease
	}
	return data
}

func (s *recoveryCandidateSnapshotBarrier) GetIncompleteSubtreeMiningData(ctx context.Context) *subtreeprocessor.PrecomputedMiningData {
	if s.beforeIncomplete != nil {
		s.beforeIncomplete()
	}
	return s.Interface.GetIncompleteSubtreeMiningData(ctx)
}

func TestMiningCandidateCanAcquireSnapshotAcrossRepair(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "incomplete"
		if complete {
			name = "completed mmap"
		}
		t.Run(name, func(t *testing.T) {
			b, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING, func(s *settings.Settings) {
				s.BlockAssembly.InitialMerkleItemsPerSubtree = 8
				if complete {
					s.BlockAssembly.InitialMerkleItemsPerSubtree = 2
					s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
				}
				s.BlockAssembly.SubtreeMmapDir = t.TempDir()
			})
			hashes := storeRecoverySelectionChain(t, b, 1)
			processor := b.subtreeProcessor
			if complete {
				require.True(t, b.AddTxBatchIfRoom([]subtree.Node{{Hash: hashes[0], Fee: 1, SizeInBytes: 100}}, []*subtree.TxInpoints{{}}))
				require.Eventually(t, func() bool {
					data := processor.GetPrecomputedMiningData()
					if data == nil {
						return false
					}
					defer data.Lease.Release()
					return len(data.Subtrees) == 1 && data.Subtrees[0].IsMmapBacked()
				}, time.Second, time.Millisecond, "fixture must complete a real mmap subtree before snapshot acquisition")
			}
			entered, release := make(chan struct{}), make(chan struct{})
			barrier := &recoveryCandidateSnapshotBarrier{Interface: processor}
			wait := func() { close(entered); <-release }
			if complete {
				barrier.beforePrecomputed = wait
			} else {
				barrier.beforeIncomplete = wait
			}
			b.subtreeProcessor = barrier
			type result struct {
				candidate *model.MiningCandidate
				trees     []*subtree.Subtree
				lease     *subtreeprocessor.MiningSnapshotLease
				err       error
			}
			finished := make(chan result, 1)
			go func() {
				candidate, trees, lease, err := b.GetMiningCandidate(t.Context())
				finished <- result{candidate: candidate, trees: trees, lease: lease, err: err}
			}()
			// Always release the blocked reader before the real processor's cleanup.
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("candidate did not reach snapshot acquisition")
			}
			header, _ := b.CurrentBlock()
			require.NoError(t, processor.RecoverUnmined(t.Context(), header, hashes,
				func(ctx context.Context, candidates []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxo.UnminedTransaction, error) {
					return b.prepareUnminedRecovery(ctx, candidates, accepted)
				}))
			require.False(t, processor.RecoveryPending(), "nondestructive repair keeps mining available")
			close(release)
			released = true
			var got result
			select {
			case got = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("candidate did not finish after snapshot acquisition")
			}
			defer got.lease.Release()
			require.NoError(t, got.err, "mining must remain available when snapshot acquisition spans repair")
			require.NotNil(t, got.candidate)
			require.NotEmpty(t, got.trees)
			if complete {
				require.NotNil(t, barrier.acquiredLease, "fixture must exercise real mmap ownership")
				retained, ok := barrier.acquiredLease.Retain()
				require.True(t, ok, "returned candidate must retain its acquired mmap lease")
				retained.Release()
			}
		})
	}
}
