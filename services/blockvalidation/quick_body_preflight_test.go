package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

type preflightReadStore struct {
	blob.Store
	nodeReads   atomic.Int32
	gateHash    *chainhash.Hash
	gateEntered chan struct{}
	gateRelease chan struct{}
	gateOnce    sync.Once
}

func (s *preflightReadStore) GetIoReader(ctx context.Context, key []byte, kind fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	if kind == fileformat.FileTypeSubtreeData && s.gateHash != nil && bytes.Equal(key, s.gateHash[:]) {
		s.gateOnce.Do(func() { close(s.gateEntered) })
		<-s.gateRelease
	}
	reader, err := s.Store.GetIoReader(ctx, key, kind, opts...)
	if err == nil && (kind == fileformat.FileTypeSubtree || kind == fileformat.FileTypeSubtreeToCheck) {
		s.nodeReads.Add(1)
	}
	return reader, err
}

type preflightMutationStore struct {
	utxo.Store
	created chan struct{}
	once    sync.Once
}

func (s *preflightMutationStore) Create(ctx context.Context, tx *bt.Tx, height uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	data, err := s.Store.Create(ctx, tx, height, opts...)
	if err == nil {
		s.once.Do(func() { close(s.created) })
	}
	return data, err
}

func newQuickBodyFixture(t *testing.T, mode string) (*BlockValidation, *model.Block, []*bt.Tx, *preflightReadStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := test.CreateBaseTestSettings(t)
	cfg.BlockValidation.SubtreeBatchSize = 1
	cfg.BlockValidation.SubtreeBatchPrefetchDepth = 0
	if mode == "pipeline" {
		cfg.BlockValidation.SubtreeBatchPrefetchDepth = 2
	}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	chain, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chain.(interface{ Close() error }).Close()) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, cfg, chain, nil, nil)
	require.NoError(t, err)
	utxos, err := sql.New(ctx, ulogger.TestLogger{}, cfg, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, utxos.Close(context.Background())) })
	require.NoError(t, utxos.SetBlockHeight(200))
	blobs := &preflightReadStore{Store: memory.New()}
	u := NewBlockValidation(ctx, ulogger.TestLogger{}, cfg, client, blobs, memory.New(), utxos, nil, nil)
	t.Cleanup(func() { cancel(); u.StopCaches() })
	txs := transactions.CreateTestTransactionChainWithCount(t, 8)[1:]
	require.Len(t, txs, 6)
	_, err = utxos.Create(ctx, txs[0], 1)
	require.NoError(t, err)
	block := testhelpers.CreateTestBlocksWithPrev(t, 1, cfg.ChainCfgParams.GenesisHash)[0]
	block.Height = 1
	block.TransactionCount = 6
	block.Subtrees = nil
	block.SubtreeSlices = nil
	for i := 0; i < 3; i++ {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		} else {
			require.NoError(t, st.AddNode(*txs[i*2].TxIDChainHash(), 0, uint64(txs[i*2].Size())))
		}
		require.NoError(t, st.AddNode(*txs[i*2+1].TxIDChainHash(), 0, uint64(txs[i*2+1].Size())))
		data := subtreepkg.NewSubtreeData(st)
		if i > 0 {
			require.NoError(t, data.AddTx(txs[i*2], 0))
		}
		require.NoError(t, data.AddTx(txs[i*2+1], 1))
		raw, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, blobs.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, raw))
		raw, err = data.Serialize()
		require.NoError(t, err)
		require.NoError(t, blobs.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, raw))
		block.Subtrees = append(block.Subtrees, st.RootHash())
		block.SubtreeSlices = append(block.SubtreeSlices, st)
	}
	roots := make([]*chainhash.Hash, len(block.Subtrees))
	copy(roots, block.Subtrees)
	roots[0], err = block.SubtreeSlices[0].RootHashWithReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	combined, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	for _, root := range roots {
		require.NoError(t, combined.AddNode(*root, 0, 0))
	}
	block.Header.HashMerkleRoot = combined.RootHash()
	require.NoError(t, block.CheckMerkleRoot(ctx))
	block.SubtreeSlices = nil
	return u, block, txs, blobs
}

func runQuickBody(u *BlockValidation, block *model.Block, mode string, jobs chan *SubtreeWriteJob) error {
	if mode == "async" {
		return u.quickValidateBlockAsync(context.Background(), block, "test", "", jobs)
	}
	return u.quickValidateBlock(context.Background(), block, "test", "")
}

func TestQuickBodyPreflightRejectsMalformedData(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		for _, mutation := range []string{"early hash mismatch", "late hash mismatch", "late missing", "late truncated"} {
			t.Run(mode+"/"+mutation, func(t *testing.T) {
				u, block, txs, blobs := newQuickBodyFixture(t, mode)
				index := 2
				if mutation == "early hash mismatch" {
					index = 0
				}
				payload := txs[1].Bytes()
				if mutation == "early hash mismatch" {
					payload = txs[2].Bytes()
				}
				if mutation == "late missing" {
					payload = nil
				}
				if mutation == "late truncated" {
					payload = txs[4].Bytes()[:12]
				}
				require.NoError(t, blobs.Set(context.Background(), block.Subtrees[index][:], fileformat.FileTypeSubtreeData, payload, bloboptions.WithAllowOverwrite(true)))
				jobs := make(chan *SubtreeWriteJob, 10)
				// Hold the final data read so earlier pipeline stages have time
				// to mutate state if the whole-body preflight is removed.
				watched := &preflightMutationStore{Store: u.utxoStore, created: make(chan struct{})}
				u.utxoStore = watched
				blobs.gateHash = block.Subtrees[index]
				blobs.gateEntered = make(chan struct{})
				blobs.gateRelease = make(chan struct{})
				finished := make(chan error, 1)
				go func() { finished <- runQuickBody(u, block, mode, jobs) }()
				select {
				case <-blobs.gateEntered:
				case <-time.After(5 * time.Second):
					close(blobs.gateRelease)
					t.Fatal("transaction data read was not reached")
				}
				mutated := false
				select {
				case <-watched.created:
					mutated = true
				case <-time.After(100 * time.Millisecond):
				}
				close(blobs.gateRelease)
				err := <-finished
				require.False(t, mutated, "no UTXOs may be created while later transaction data is unauthenticated")
				require.True(t, errors.Is(err, errors.ErrBlockInvalid), "%v", err)
				require.Zero(t, block.ID, "malformed later data must be rejected before the first batch mutates state")
				require.Empty(t, jobs)
				for _, tx := range txs[1:] {
					_, err := u.utxoStore.Get(context.Background(), tx.TxIDChainHash())
					require.True(t, errors.Is(err, errors.ErrTxNotFound), "no child transaction may be created: %v", err)
				}
				spend, err := u.utxoStore.GetSpend(context.Background(), &utxo.Spend{TxID: txs[0].TxIDChainHash(), Vout: txs[1].Inputs[0].PreviousTxOutIndex})
				require.NoError(t, err)
				require.Equal(t, int(utxo.Status_OK), spend.Status)
				for _, hash := range block.Subtrees {
					exists, err := blobs.Exists(context.Background(), hash[:], fileformat.FileTypeSubtree)
					require.NoError(t, err)
					require.False(t, exists)
				}
			})
		}
	}
}

func TestQuickBodyPreflightTransactionCount(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		for _, coinbaseOnly := range []bool{false, true} {
			for _, count := range []uint64{0, 5, 7} {
				t.Run(fmt.Sprintf("%s/coinbase=%v/count=%d", mode, coinbaseOnly, count), func(t *testing.T) {
					u, block, _, _ := newQuickBodyFixture(t, mode)
					if coinbaseOnly {
						block.Subtrees = nil
						block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()
					}
					block.TransactionCount = count
					jobs := make(chan *SubtreeWriteJob, 10)
					require.ErrorContains(t, runQuickBody(u, block, mode, jobs), "transaction count")
					require.Zero(t, block.ID)
					require.Empty(t, jobs)
				})
			}
		}
	}
}

func TestQuickBodyPreflightReusesNodes(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		t.Run(mode, func(t *testing.T) {
			u, block, _, blobs := newQuickBodyFixture(t, mode)
			jobs := make(chan *SubtreeWriteJob, 10)
			require.NoError(t, runQuickBody(u, block, mode, jobs))
			require.EqualValues(t, len(block.Subtrees), blobs.nodeReads.Load(), "each subtree's nodes should only be deserialized once")
			stored, err := u.blockchainClient.GetBlock(context.Background(), block.Hash())
			require.NoError(t, err)
			require.EqualValues(t, 6, stored.TransactionCount)
		})
	}
}

func TestQuickBodyPreflightRequiresCoinbasePlaceholder(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		t.Run(mode, func(t *testing.T) {
			u, block, txs, blobs := newQuickBodyFixture(t, mode)
			// A different coinbase in the first slot must not be silently replaced
			// when checking the header's merkle root or parsing transaction data.
			otherCoinbase := block.CoinbaseTx.Clone()
			otherCoinbase.Outputs[0].Satoshis++
			st, err := subtreepkg.NewTreeByLeafCount(2)
			require.NoError(t, err)
			require.NoError(t, st.AddNode(*otherCoinbase.TxIDChainHash(), 0, uint64(otherCoinbase.Size())))
			require.NoError(t, st.AddNode(*txs[1].TxIDChainHash(), 0, uint64(txs[1].Size())))
			block.Subtrees[0] = st.RootHash()
			raw, err := st.Serialize()
			require.NoError(t, err)
			require.NoError(t, blobs.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, raw))
			raw = append(otherCoinbase.Bytes(), txs[1].Bytes()...)
			require.NoError(t, blobs.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtreeData, raw))
			jobs := make(chan *SubtreeWriteJob, 10)
			require.ErrorContains(t, runQuickBody(u, block, mode, jobs), "coinbase placeholder")
			require.Zero(t, block.ID)
			require.Empty(t, jobs)
			for _, tx := range txs[1:] {
				_, err := u.utxoStore.Get(context.Background(), tx.TxIDChainHash())
				require.True(t, errors.Is(err, errors.ErrTxNotFound), "%v", err)
			}
		})
	}
}

func TestQuickBodyPreflightMmapOwnership(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		t.Run(mode, func(t *testing.T) {
			u, block, _, blobs := newQuickBodyFixture(t, mode)
			u.mmapDir = t.TempDir()
			cleanup, err := u.authenticateQuickBlockBody(context.Background(), block)
			require.NoError(t, err)
			defer cleanup()
			for _, st := range block.SubtreeSlices {
				require.True(t, st.IsMmapBacked())
			}
			batch, err := u.prefetchSubtreeBatch(context.Background(), block, 0, len(block.Subtrees))
			require.NoError(t, err)
			batch.Close()
			// A failed intermediate batch must not unmap the block's borrowed nodes.
			require.NoError(t, block.CheckMerkleRoot(context.Background()))
			require.EqualValues(t, len(block.Subtrees), blobs.nodeReads.Load())
			cleanup()
			for _, st := range block.SubtreeSlices {
				require.Nil(t, st, "fallback must not see an unmapped subtree")
			}
			files, err := os.ReadDir(u.mmapDir)
			require.NoError(t, err)
			require.Empty(t, files)
			jobs := make(chan *SubtreeWriteJob, 10)
			require.NoError(t, runQuickBody(u, block, mode, jobs), "a fresh attempt must reload after cleanup")
		})
	}
}

func TestQuickBodyPreflightRecomputesStoredRoot(t *testing.T) {
	for _, mmap := range []bool{false, true} {
		t.Run(fmt.Sprintf("mmap=%v", mmap), func(t *testing.T) {
			u, block, txs, blobs := newQuickBodyFixture(t, "sequential")
			if mmap {
				u.mmapDir = t.TempDir()
			}
			hash := block.Subtrees[2]
			raw, err := blobs.Get(context.Background(), hash[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, err)
			// Keep the serialized root and store key, replace a node and its data.
			copy(raw[56:88], txs[0].TxIDChainHash()[:])
			require.NoError(t, blobs.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeToCheck, raw, bloboptions.WithAllowOverwrite(true)))
			data := append(txs[0].Bytes(), txs[5].Bytes()...)
			require.NoError(t, blobs.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeData, data, bloboptions.WithAllowOverwrite(true)))
			cleanup, err := u.authenticateQuickBlockBody(context.Background(), block)
			defer cleanup()
			require.ErrorContains(t, err, "nodes do not match its root")
			require.True(t, errors.Is(err, errors.ErrBlockInvalid))
			require.Zero(t, block.ID)
		})
	}
}

func TestQuickBodyPreflightMissingFileIsNotInvalid(t *testing.T) {
	u, block, _, blobs := newQuickBodyFixture(t, "sequential")
	require.NoError(t, blobs.Del(context.Background(), block.Subtrees[2][:], fileformat.FileTypeSubtreeData))
	cleanup, err := u.authenticateQuickBlockBody(context.Background(), block)
	defer cleanup()
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a missing file is not a verdict on the body")
	require.Zero(t, block.ID)
}

type failingPreflightCreateStore struct{ utxo.Store }

func (s *failingPreflightCreateStore) Create(context.Context, *bt.Tx, uint32, ...utxo.CreateOption) (*meta.Data, error) {
	return nil, errors.NewStorageError("injected UTXO write failure")
}

func TestQuickBodyPreflightMmapProcessingFailure(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		t.Run(mode, func(t *testing.T) {
			u, block, _, _ := newQuickBodyFixture(t, mode)
			u.mmapDir = t.TempDir()
			utxos := u.utxoStore
			u.utxoStore = &failingPreflightCreateStore{Store: utxos}
			jobs := make(chan *SubtreeWriteJob, 10)
			require.Error(t, runQuickBody(u, block, mode, jobs))
			for _, st := range block.SubtreeSlices {
				if st != nil {
					require.False(t, st.IsMmapBacked(), "failed processing must not leave an unmapped reference")
				}
			}
			files, err := os.ReadDir(u.mmapDir)
			require.NoError(t, err)
			require.Empty(t, files)
			u.utxoStore = utxos
			require.NoError(t, runQuickBody(u, block, mode, make(chan *SubtreeWriteJob, 10)))
		})
	}
}

type parallelPreflightStore struct {
	blob.Store
	entered chan struct{}
	release chan struct{}
}

func (s *parallelPreflightStore) GetIoReader(ctx context.Context, key []byte, kind fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	if kind == fileformat.FileTypeSubtreeData {
		select {
		case s.entered <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.GetIoReader(ctx, key, kind, opts...)
}

func TestQuickBodyPreflightBoundedParallelReads(t *testing.T) {
	u, block, _, blobs := newQuickBodyFixture(t, "sequential")
	u.settings.Block.GetAndValidateSubtreesConcurrency = 2
	u.settings.BlockValidation.SubtreeBatchSize = 100
	reads := &parallelPreflightStore{Store: blobs, entered: make(chan struct{}, 3), release: make(chan struct{})}
	u.subtreeStore = reads
	finished := make(chan error, 1)
	go func() {
		cleanup, err := u.authenticateQuickBlockBody(context.Background(), block)
		cleanup()
		finished <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-reads.entered:
		case <-time.After(5 * time.Second):
			close(reads.release)
			t.Fatal("preflight did not read two subtrees concurrently")
		}
	}
	select {
	case <-reads.entered:
		t.Error("preflight exceeded the configured read concurrency")
	case <-time.After(50 * time.Millisecond):
	}
	close(reads.release)
	require.NoError(t, <-finished)
}

type reopenFailureStore struct {
	blob.Store
	reads atomic.Int32
}

func (s *reopenFailureStore) GetIoReader(ctx context.Context, key []byte, kind fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	if kind == fileformat.FileTypeSubtreeToCheck && s.reads.Add(1) > 1 {
		return nil, errors.NewStorageError("injected reopen failure")
	}
	return s.Store.GetIoReader(ctx, key, kind, opts...)
}

func TestQuickBodyPreflightMmapFallback(t *testing.T) {
	for _, reopenFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopenFails=%v", reopenFails), func(t *testing.T) {
			u, block, _, blobs := newQuickBodyFixture(t, "sequential")
			u.mmapDir = t.TempDir() + "/missing/directory"
			if reopenFails {
				u.subtreeStore = &reopenFailureStore{Store: blobs}
			}
			result := u.readSubtree(context.Background(), block, 0, block.Subtrees[0])
			if reopenFails {
				require.Error(t, result.err)
				require.True(t, errors.Is(result.err, errors.ErrStorageError))
				return
			}
			require.NoError(t, result.err, "fallback must reopen at byte zero")
			defer result.subtree.Close()
			require.False(t, result.subtree.IsMmapBacked())
			require.Equal(t, *block.Subtrees[0], *result.subtree.RootHash())
		})
	}
}
