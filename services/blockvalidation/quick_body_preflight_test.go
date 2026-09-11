package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
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
				require.Error(t, err)
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
