//go:build linux

package blockassembly

// With blockassembly_subtreeMmapDir set, the subtrees of a cached mining job and of a
// subtree whose storage has not finished are read after the processor has retired them.
// Closing a subtree unmaps its nodes and removes its backing file, so these tests ask
// whether the file of every subtree a job or a pending callback holds is still there
// (found through /proc/self/maps, so this is Linux only). That is deterministic where the
// fault is not: an unmapped range can be remapped and
// read without faulting, and a real fault kills the whole test binary.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// blockingSubtreeStore blocks every subtree write while gate is set, until release is
// closed. It ignores the context: a file write in progress cannot be cancelled either.
type blockingSubtreeStore struct {
	blob.Store

	gate    atomic.Bool
	once    sync.Once
	release chan struct{}
	entered chan chainhash.Hash
}

func (s *blockingSubtreeStore) open() { s.once.Do(func() { close(s.release) }) }

func (s *blockingSubtreeStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeSubtree && s.gate.Load() {
		select {
		case s.entered <- chainhash.Hash(key):
		default:
		}

		<-s.release
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// hookedClient runs a function inside GetNextWorkRequired, which a mining candidate calls after
// it has loaded the processor's subtrees and before it returns them.
type hookedClient struct {
	blockchain.ClientI

	hook atomic.Pointer[func()]
}

func (c *hookedClient) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	if fn := c.hook.Swap(nil); fn != nil {
		(*fn)()
	}

	return c.ClientI.GetNextWorkRequired(ctx, hash, currentBlockTime)
}

// mmapJobServer is a started server whose processor holds complete mmap-backed subtrees.
type mmapJobServer struct {
	ba      *BlockAssembly
	mmap    string
	blocks  *blockingSubtreeStore
	genesis *chainhash.Hash
	client  *hookedClient
}

// newMmapJobServer starts a server with mmap subtrees of four leaves. With gated set,
// the subtree store blocks every subtree write, so each stored subtree's completion
// callback stays pending. Nine transactions fill two chained subtrees.
func newMmapJobServer(t *testing.T, gated bool) *mmapJobServer {
	t.Helper()

	mmapDir := t.TempDir()

	ba, store := setupServerWithSettings(t, func(s *settings.Settings) {
		s.BlockAssembly.SubtreeMmapDir = mmapDir
		s.BlockAssembly.InitialMerkleItemsPerSubtree = 4
		s.BlockAssembly.UseDynamicSubtreeSize = false
		s.BlockAssembly.SubtreeProcessorBatcherSize = 1
		s.BlockAssembly.DoubleSpendWindow = 0

		if gated {
			// one worker, so the second subtree waits in the queue while the first write is blocked
			s.BlockAssembly.SubtreeStorageWorkers = 1
		}
	})

	client := &hookedClient{ClientI: ba.blockchainClient}
	ba.blockchainClient = client
	ba.blockAssembler.blockchainClient = client

	blocks := &blockingSubtreeStore{Store: store, release: make(chan struct{}), entered: make(chan chainhash.Hash, 16)}
	blocks.gate.Store(gated)
	ba.subtreeStore = blocks

	// Cleanups run last in, first out: this one runs before the server's own. A failed test
	// leaves the writes blocked, so the callbacks do not read what the failure left unmapped.
	t.Cleanup(func() {
		if !t.Failed() {
			blocks.open()
		}
	})

	require.NoError(t, ba.blockAssembler.Start(t.Context()))

	require.Eventually(t, func() bool {
		return ba.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// The transactions are in the UTXO store, as a mempool transaction is, so a reorg keeps
	// them instead of dropping what it cannot reload.
	for i := uint32(0); i < 9; i++ {
		tx := newTx(i + 1)
		inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		_, _, err = ba.utxoStore.SpendAndCreate(t.Context(), tx, 0, utxostore.WithCreateOnly())
		require.NoError(t, err)

		require.Equal(t, subtreeprocessor.Admitted, ba.blockAssembler.AddTxBatchBounded(
			[]subtreepkg.Node{{Hash: *tx.TxIDChainHash(), Fee: uint64(i) + 1, SizeInBytes: 100}},
			[]*subtreepkg.TxInpoints{&inpoints}, false))
	}

	require.Eventually(t, func() bool {
		return len(chainedSubtrees(ba)) >= 2
	}, 5*time.Second, 50*time.Millisecond, "the processor must hold complete subtrees")

	for _, st := range chainedSubtrees(ba) {
		require.True(t, st.IsMmapBacked(), "the subtrees must be mmap-backed for these tests to mean anything")
	}

	return &mmapJobServer{ba: ba, mmap: mmapDir, blocks: blocks, client: client}
}

// storeJob caches a mining candidate built from the processor's current subtrees, as
// GetMiningCandidate does, and returns its id and a handle on its subtrees.
func (m *mmapJobServer) storeJob(t *testing.T) (chainhash.Hash, heldSubtrees) {
	t.Helper()

	candidate, subtrees, err := m.ba.blockAssembler.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(subtrees), 2)

	id, err := chainhash.NewHash(candidate.Id)
	require.NoError(t, err)

	m.ba.storeJobWithCoinbase(id, candidate, subtrees)

	return *id, m.hold(t, subtrees)
}

// heldSubtrees remembers each subtree's backing file while it is still mapped.
type heldSubtrees struct {
	subtrees []*subtreepkg.Subtree
	files    []string
}

func (m *mmapJobServer) hold(t *testing.T, subtrees []*subtreepkg.Subtree) heldSubtrees {
	t.Helper()

	h := heldSubtrees{subtrees: subtrees}

	for _, st := range subtrees {
		require.True(t, st.IsMmapBacked())
		h.files = append(h.files, backingFile(t, st))
	}

	return h
}

// backingFile finds the file a subtree's nodes are mapped from.
func backingFile(t *testing.T, st *subtreepkg.Subtree) string {
	t.Helper()

	addr := uint64(uintptr(unsafe.Pointer(&st.Nodes[0]))) //nolint:gosec

	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)

	for _, line := range strings.Split(string(maps), "\n") {
		var start, end uint64

		if n, _ := fmt.Sscanf(line, "%x-%x", &start, &end); n == 2 && addr >= start && addr < end {
			fields := strings.Fields(line)
			require.GreaterOrEqual(t, len(fields), 6, "the subtree is not mapped from a file")

			return fields[5]
		}
	}

	require.FailNow(t, "the subtree's nodes are not in /proc/self/maps")

	return ""
}

// requireStillMapped fails when the backing file of any held subtree is gone, which is
// what closing the subtree does.
func (m *mmapJobServer) requireStillMapped(t *testing.T, h heldSubtrees, why string) {
	t.Helper()

	for i, file := range h.files {
		require.True(t, fileExists(file), "subtree %d was closed while %s still holds it", i, why)
	}
}

func (m *mmapJobServer) requireClosed(t *testing.T, h heldSubtrees) {
	t.Helper()

	for i, file := range h.files {
		require.False(t, fileExists(file), "subtree %d is still mapped after nothing holds it", i)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// requireNotInstalled checks that the processor no longer holds the subtrees, so closing
// them is what the reset or reorg was allowed to do.
func (m *mmapJobServer) requireNotInstalled(t *testing.T, h heldSubtrees) {
	t.Helper()

	installed := map[*subtreepkg.Subtree]struct{}{}
	for _, st := range m.ba.blockAssembler.subtreeProcessor.GetChainedSubtrees() {
		installed[st] = struct{}{}
	}

	for _, st := range h.subtrees {
		_, ok := installed[st]
		require.False(t, ok, "the reset or reorg did not replace the subtrees")
	}
}

// requireGuardLetsJobThrough checks that SubmitMiningSolution's stale-job guard, which
// rejects a job whose parent is the parent of the best block, does not catch this job.
func (m *mmapJobServer) requireGuardLetsJobThrough(t *testing.T, id chainhash.Hash) {
	t.Helper()

	best, _ := m.ba.blockAssembler.CurrentBlock()
	require.NotNil(t, best)

	prev, err := chainhash.NewHash(m.ba.jobStore.Get(id).Value().MiningCandidate.PreviousHash)
	require.NoError(t, err)
	require.False(t, best.HashPrevBlock.IsEqual(prev), "the stale-job guard would have rejected the job")
}

// tryReset runs the assembler's reset on the current tip and waits for it.
func (m *mmapJobServer) tryReset() error {
	errCh := make(chan error, 1)
	m.ba.blockAssembler.resetCh <- resetRequest{ErrCh: errCh}

	select {
	case err := <-errCh:
		return err
	case <-time.After(10 * time.Second):
		return errors.NewProcessingError("reset did not finish")
	}
}

func (m *mmapJobServer) reset(t *testing.T) {
	t.Helper()

	require.NoError(t, m.tryReset())
}

// addTip adds a block on the genesis block, with the given work, and waits until block
// assembly builds on it and holds at least wantSubtrees complete subtrees. After the
// reorg the processor's mining data holds none until a subtree completes again.
func (m *mmapJobServer) addTip(t *testing.T, nonce uint32, bits string, wantSubtrees int) {
	t.Helper()

	ctx := t.Context()

	if m.genesis == nil {
		genesis, _, err := m.ba.blockchainClient.GetBestBlockHeader(ctx)
		require.NoError(t, err)

		m.genesis = genesis.Hash()
	}

	coinbase := coinbaseEncoding(t, 1, 5000000000+uint64(nonce))

	nBits, err := model.NewNBitFromString(bits)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  m.genesis,
		HashMerkleRoot: coinbase.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          nonce,
	}

	require.NoError(t, m.ba.blockchainClient.AddBlock(ctx, &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Height:           1,
		Subtrees:         []*chainhash.Hash{},
	}, ""))

	require.Eventually(t, func() bool {
		best, _ := m.ba.blockAssembler.CurrentBlock()
		return best != nil && best.Hash().IsEqual(header.Hash()) &&
			len(chainedSubtrees(m.ba)) >= wantSubtrees
	}, 5*time.Second, 50*time.Millisecond, "block assembly did not move to the new tip")
}

// jobThenSiblingReorg caches a job on a tip, then reorgs to a sibling of that tip. The
// job's parent is then not the parent of the best block, so the stale-job guard, which
// only rejects a job whose parent is exactly one block behind, lets it through.
func (m *mmapJobServer) jobThenSiblingReorg(t *testing.T) (chainhash.Hash, heldSubtrees) {
	t.Helper()

	m.addTip(t, 1, "207fffff", 2)

	id, job := m.storeJob(t)

	m.addTip(t, 2, "1d00ffff", 0)

	m.requireNotInstalled(t, job)
	m.requireGuardLetsJobThrough(t, id)

	return id, job
}

// readLikeSubmit reads a job's subtrees the way SubmitMiningSolution does.
func readLikeSubmit(t *testing.T, h heldSubtrees) {
	t.Helper()

	for _, st := range h.subtrees {
		require.NotNil(t, st.RootHash())
		require.Positive(t, st.Duplicate().Length())
		require.Positive(t, st.Length())
	}
}

func TestJobSubtreesStayMapped_OlderJobAfterNoOpResetOnTheSameTip(t *testing.T) {
	m := newMmapJobServer(t, false)

	id, job := m.storeJob(t)

	m.reset(t)

	m.requireNotInstalled(t, job)
	m.requireGuardLetsJobThrough(t, id)
	m.requireStillMapped(t, job, "a cached job")
	readLikeSubmit(t, job)
}

func TestJobSubtreesStayMapped_GetCandidateBlockAfterAReset(t *testing.T) {
	m := newMmapJobServer(t, false)

	id, job := m.storeJob(t)

	m.reset(t)
	m.requireNotInstalled(t, job)

	resp, err := m.ba.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: id[:]})
	require.NoError(t, err)
	require.NotEmpty(t, resp.SubtreeHashes)

	m.requireStillMapped(t, job, "a cached job")
}

// A job lives as long as it keeps being read, because every Get extends its TTL, so ten
// minutes is a floor and not a bound. Once nothing reads it, it expires and releases its
// subtrees without anything deleting it.
func TestJobSubtreesStayMappedWhileTheJobIsReadAndAreClosedAfterItExpires(t *testing.T) {
	m := newMmapJobServer(t, false)

	id, job := m.storeJob(t)

	const ttl = 3 * time.Second

	m.ba.jobStore.Set(id, m.ba.jobStore.Get(id).Value(), ttl)

	// read for longer than the TTL: every Get extends it
	for range 5 {
		time.Sleep(time.Second)
		require.NotNil(t, m.ba.jobStore.Get(id), "the job expired although it was read")
	}

	m.reset(t)
	m.requireNotInstalled(t, job)
	m.requireStillMapped(t, job, "a job that is still being read")

	time.Sleep(2 * ttl)

	m.reset(t)
	m.requireClosed(t, job)
}

// The mining candidate loads the processor's subtrees, then reads them, then stores its
// job. A reset that retires them in between, here run from inside the difficulty lookup,
// must not unmap what the candidate is about to hand out.
func TestCandidateSubtreesStayMappedBetweenTheSnapshotAndTheJobStore(t *testing.T) {
	m := newMmapJobServer(t, false)

	before := map[*subtreepkg.Subtree]string{}
	for _, st := range chainedSubtrees(m.ba) {
		before[st] = backingFile(t, st)
	}

	var ran atomic.Bool

	hook := func() {
		ran.Store(true)
		m.reset(t)
	}
	m.client.hook.Store(&hook)

	candidate, err := m.ba.GetMiningCandidate(t.Context(), &blockassembly_api.GetMiningCandidateRequest{})
	require.NoError(t, err)
	require.True(t, ran.Load(), "the reset did not run inside the candidate")

	id, err := chainhash.NewHash(candidate.Id)
	require.NoError(t, err)

	job := m.ba.jobStore.Get(*id).Value()
	require.GreaterOrEqual(t, len(job.Subtrees), 2)

	for i, st := range job.Subtrees {
		file, ok := before[st]
		if !ok {
			file = backingFile(t, st)
		}

		require.True(t, fileExists(file), "job subtree %d was closed before the job was stored", i)
	}
}

func TestJobSubtreesAreClosedOnceTheJobIsGone(t *testing.T) {
	m := newMmapJobServer(t, false)

	id, job := m.storeJob(t)

	m.reset(t)
	m.requireStillMapped(t, job, "a cached job")

	m.ba.jobStore.Delete(id)
	m.reset(t)

	m.requireClosed(t, job)
}

// One worker is blocked in a write, so the other chained subtrees wait in the queue, and
// none of them has reached the storage code yet. They must stay mapped all the same.
func TestSubtreesStayMappedWhileTheirStoreIsQueuedOrPending(t *testing.T) {
	m := newMmapJobServer(t, true)

	select {
	case <-m.blocks.entered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "no subtree write started")
	}

	queued := chainedSubtrees(m.ba)
	require.GreaterOrEqual(t, len(queued), 2)

	stored := m.hold(t, queued)

	m.reset(t)

	m.requireNotInstalled(t, stored)
	m.requireStillMapped(t, stored, "a queued or pending store")

	// the writes finish, the callbacks run, and the next reset may close them
	m.blocks.open()

	require.Eventually(t, func() bool {
		if m.tryReset() != nil {
			return false
		}

		for _, file := range stored.files {
			if fileExists(file) {
				return false
			}
		}

		return true
	}, 10*time.Second, 100*time.Millisecond, "the subtrees are never closed once their stores have ended")
}

func TestJobSubtreesStayMapped_OlderJobAfterASiblingReorgCommit(t *testing.T) {
	m := newMmapJobServer(t, false)

	_, job := m.jobThenSiblingReorg(t)

	m.requireStillMapped(t, job, "a cached job")
	readLikeSubmit(t, job)
}

func TestJobSubtreesStayMapped_GetCandidateBlockAfterASiblingReorg(t *testing.T) {
	m := newMmapJobServer(t, false)

	id, job := m.jobThenSiblingReorg(t)

	resp, err := m.ba.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: id[:]})
	require.NoError(t, err)
	require.NotEmpty(t, resp.SubtreeHashes)

	m.requireStillMapped(t, job, "a cached job")
}

// chainedSubtrees is what a mining candidate would be built from now.
func chainedSubtrees(ba *BlockAssembly) []*subtreepkg.Subtree {
	if data := ba.blockAssembler.subtreeProcessor.GetPrecomputedMiningData(); data != nil {
		return data.Subtrees
	}

	return nil
}
