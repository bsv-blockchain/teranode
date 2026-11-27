package testhelpers

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test/fault"
	"github.com/stretchr/testify/require"
)

func TestNewStack_StoresAreRealAndUsable(t *testing.T) {
	s := NewStack(t)

	header, meta, err := s.Blockchain.GetBestBlockHeader(s.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), meta.Height)
	require.Equal(t, s.Settings.ChainCfgParams.GenesisHash.String(), header.Hash().String())

	require.NoError(t, s.UTXOStore.SetBlockHeight(7))
	require.Equal(t, uint32(7), s.UTXOStore.GetBlockHeight())

	require.NoError(t, s.SubtreeStore.Set(s.Ctx, []byte("k"), fileformat.FileTypeSubtree, []byte("v")))
	v, err := s.SubtreeStore.Get(s.Ctx, []byte("k"), fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	require.Nil(t, s.Plan)
	require.Same(t, s.InnerBlockchain, s.Blockchain, "without faults the stack hands out the real client")
}

func TestNewStack_StacksAreIsolated(t *testing.T) {
	a := NewStack(t)
	b := NewStack(t)

	require.NoError(t, a.UTXOStore.SetBlockHeight(11))
	require.NoError(t, b.UTXOStore.SetBlockHeight(22))

	require.Equal(t, uint32(11), a.UTXOStore.GetBlockHeight())
	require.Equal(t, uint32(22), b.UTXOStore.GetBlockHeight())
}

func TestNewStack_ClosesBlockchainStore(t *testing.T) {
	var store blockchainstore.Store
	t.Run("stack lifetime", func(t *testing.T) {
		s := NewStack(t)
		store = s.InnerBlockchainStore
		_, _, err := store.Health(s.Ctx, true)
		require.NoError(t, err)
	})

	_, _, err := store.Health(context.Background(), true)
	require.Error(t, err, "the blockchain store must close with the test")
}

func TestNewStack_WithSettings(t *testing.T) {
	s := NewStack(t, WithSettings(func(st *settings.Settings) { st.BlockValidation.OptimisticMining = true }))
	require.True(t, s.Settings.BlockValidation.OptimisticMining)
}

func TestNewStack_FaultsSurfaceThroughTheClientButNotTheInnerClient(t *testing.T) {
	injected := errors.NewStorageError("injected")

	plan := fault.NewPlan()
	plan.On("GetBestBlockHeader").In(fault.StoreBlockchainStore).Fail(injected)

	s := NewStack(t, WithFaults(plan))

	_, _, err := s.Blockchain.GetBestBlockHeader(s.Ctx)
	require.ErrorIs(t, err, injected, "a store fault surfaces through the decorated client")

	_, _, err = s.InnerBlockchain.GetBestBlockHeader(s.Ctx)
	require.NoError(t, err, "setup through the inner client is never faulted")

	require.Equal(t, 2, plan.Count("GetBestBlockHeader"), "client call and store call, nothing from the inner client")
}

func TestNewStack_FaultsCoverEveryStore(t *testing.T) {
	plan := fault.NewPlan()
	injected := errors.NewStorageError("injected")
	plan.On("Set").In(fault.StoreBlob).Fail(injected)
	s := NewStack(t, WithFaults(plan))

	_, _ = s.UTXOStore.Get(s.Ctx, s.Settings.ChainCfgParams.GenesisHash)
	_, _ = s.SubtreeStore.Exists(s.Ctx, []byte("k"), fileformat.FileTypeSubtree)
	_, _ = s.TxStore.Exists(s.Ctx, []byte("k"), fileformat.FileTypeTx)
	_, _ = s.BlockStore.Exists(s.Ctx, []byte("k"), fileformat.FileTypeBlock)

	var stores []string
	for _, c := range plan.Calls() {
		stores = append(stores, c.Store)
	}

	require.Equal(t, []string{fault.StoreUTXO, fault.StoreBlob, fault.StoreBlob, fault.StoreBlob}, stores)

	for _, tc := range []struct {
		name  string
		kind  fileformat.FileType
		inner blob.Store
		outer blob.Store
	}{
		{"transaction", fileformat.FileTypeTx, s.InnerTxStore, s.TxStore},
		{"block", fileformat.FileTypeBlock, s.InnerBlockStore, s.BlockStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte(tc.name)
			require.ErrorIs(t, tc.outer.Set(s.Ctx, key, tc.kind, []byte("blocked")), injected)
			require.NoError(t, tc.inner.Set(s.Ctx, key, tc.kind, []byte("seeded")))
			got, err := tc.outer.Get(s.Ctx, key, tc.kind)
			require.NoError(t, err)
			require.Equal(t, []byte("seeded"), got)
		})
	}
	require.Equal(t, 2, plan.Count("Set"), "only decorated writes enter the fault plan")
}

func TestNewStack_InvalidBlocksTopicCapturesTheRealProducer(t *testing.T) {
	s := NewStack(t, WithInvalidBlocksTopic())

	require.Equal(t, "memory", s.Settings.Kafka.InvalidBlocksConfig.Scheme)

	producer, err := kafka.NewKafkaAsyncProducerFromURL(s.Ctx, s.Logger, s.Settings.Kafka.InvalidBlocksConfig, &s.Settings.Kafka)
	require.NoError(t, err)

	ch := make(chan *kafka.Message, 1)
	go producer.Start(s.Ctx, ch)

	t.Cleanup(func() { _ = producer.Stop() })

	ch <- &kafka.Message{Key: []byte("hash"), Value: []byte("reason")}

	msgs := s.InvalidBlocks.WaitFor(t, 1, 5*time.Second)
	require.Equal(t, []byte("hash"), msgs[0].Key)
	require.Equal(t, []byte("reason"), msgs[0].Value)
}
