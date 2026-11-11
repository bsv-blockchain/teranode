package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// faultyHeaderStore wraps a real store and fails the two calls under test, so the handlers'
// error paths run against a real store everywhere else.
type faultyHeaderStore struct {
	blockchain_store.Store
	err error
}

func (f *faultyHeaderStore) GetBlockHeadersByParentLinks(context.Context, *chainhash.Hash, uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, f.err
}

func (f *faultyHeaderStore) GetBestBlockHeaderUncached(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, f.err
}

// inProcessAPIClient is a BlockchainAPIClient whose two calls under test go straight to a
// real Blockchain server's handlers, so Client is exercised against the real handlers and
// their gRPC status errors without a network listener.
type inProcessAPIClient struct {
	blockchain_api.BlockchainAPIClient
	server *Blockchain
}

func (c *inProcessAPIClient) GetBlockHeadersByParentLinks(ctx context.Context, in *blockchain_api.GetBlockHeadersRequest, _ ...grpc.CallOption) (*blockchain_api.GetBlockHeadersResponse, error) {
	return c.server.GetBlockHeadersByParentLinks(ctx, in)
}

func (c *inProcessAPIClient) GetBestBlockHeaderUncached(ctx context.Context, in *emptypb.Empty, _ ...grpc.CallOption) (*blockchain_api.GetBlockHeaderResponse, error) {
	return c.server.GetBestBlockHeaderUncached(ctx, in)
}

// parentLinkFixture builds a server over a real sqlitememory store holding a 10 block chain.
func parentLinkFixture(t *testing.T) (*Blockchain, *Client, []*model.Block) {
	t.Helper()

	store, blocks := newChainStore(t, 10)
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	server, err := New(context.Background(), logger, tSettings, store, nil)
	require.NoError(t, err)

	client := &Client{client: &inProcessAPIClient{server: server}, logger: logger, settings: tSettings}

	return server, client, blocks
}

func TestGetBlockHeadersByParentLinksHandler(t *testing.T) {
	ctx := context.Background()
	server, _, blocks := parentLinkFixture(t)
	tip := blocks[len(blocks)-1]

	t.Run("returns the headers newest first with matching metas", func(t *testing.T) {
		resp, err := server.GetBlockHeadersByParentLinks(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash:       tip.Hash().CloneBytes(),
			NumberOfHeaders: 4,
		})
		require.NoError(t, err)
		require.Len(t, resp.BlockHeaders, 4)
		require.Len(t, resp.Metas, 4)

		for i := range resp.BlockHeaders {
			want := blocks[len(blocks)-1-i]

			header, err := model.NewBlockHeaderFromBytes(resp.BlockHeaders[i])
			require.NoError(t, err)
			require.Equal(t, want.Hash().String(), header.Hash().String())

			meta, err := model.NewBlockHeaderMetaFromBytes(resp.Metas[i])
			require.NoError(t, err)
			require.Equal(t, uint32(len(blocks)-i), meta.Height) //nolint:gosec // small test heights
		}
	})

	t.Run("a short chain ends at genesis without an error", func(t *testing.T) {
		resp, err := server.GetBlockHeadersByParentLinks(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash:       tip.Hash().CloneBytes(),
			NumberOfHeaders: 100,
		})
		require.NoError(t, err)
		require.Len(t, resp.BlockHeaders, len(blocks)+1, "ten blocks and genesis")
	})

	t.Run("an unknown start hash answers empty, not an error", func(t *testing.T) {
		unknown := chainhash.DoubleHashH([]byte("not stored"))

		resp, err := server.GetBlockHeadersByParentLinks(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash:       unknown.CloneBytes(),
			NumberOfHeaders: 5,
		})
		require.NoError(t, err)
		require.Empty(t, resp.BlockHeaders)
		require.Empty(t, resp.Metas)
	})

	t.Run("a malformed start hash is a block-not-found error", func(t *testing.T) {
		resp, err := server.GetBlockHeadersByParentLinks(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash:       []byte("short"),
			NumberOfHeaders: 1,
		})
		require.Nil(t, resp)
		require.Error(t, err)
		require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrBlockNotFound), "got %v", err)
		require.Contains(t, err.Error(), "not valid")
	})

	t.Run("a store failure comes back wrapped for gRPC", func(t *testing.T) {
		faulty := &faultyHeaderStore{Store: server.store, err: errors.NewStorageError("disk gone")}
		failing, err := New(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), faulty, nil)
		require.NoError(t, err)

		resp, err := failing.GetBlockHeadersByParentLinks(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash:       tip.Hash().CloneBytes(),
			NumberOfHeaders: 1,
		})
		require.Nil(t, resp)
		require.Error(t, err)
		require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrStorageError), "got %v", err)
	})
}

func TestGetBestBlockHeaderUncachedHandlerStoreError(t *testing.T) {
	ctx := context.Background()
	store, _ := newChainStore(t, 1)
	faulty := &faultyHeaderStore{Store: store, err: errors.NewStorageError("disk gone")}

	server, err := New(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), faulty, nil)
	require.NoError(t, err)

	resp, err := server.GetBestBlockHeaderUncached(ctx, &emptypb.Empty{})
	require.Nil(t, resp)
	require.Error(t, err)
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrStorageError), "got %v", err)
}

func TestClientGetBlockHeadersByParentLinks(t *testing.T) {
	ctx := context.Background()
	_, client, blocks := parentLinkFixture(t)
	tip := blocks[len(blocks)-1]

	t.Run("returns the headers in order with metas", func(t *testing.T) {
		headers, metas, err := client.GetBlockHeadersByParentLinks(ctx, tip.Hash(), 5)
		require.NoError(t, err)
		require.Len(t, headers, 5)
		require.Len(t, metas, 5)

		for i, header := range headers {
			require.Equal(t, blocks[len(blocks)-1-i].Hash().String(), header.Hash().String())
			require.Equal(t, uint32(len(blocks)-i), metas[i].Height) //nolint:gosec // small test heights
		}

		for i := 0; i+1 < len(headers); i++ {
			require.Equal(t, headers[i+1].Hash(), headers[i].HashPrevBlock)
		}
	})

	t.Run("a server error is unwrapped to its kind", func(t *testing.T) {
		store, _ := newChainStore(t, 1)
		faulty := &faultyHeaderStore{Store: store, err: errors.NewStorageError("disk gone")}
		server, err := New(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), faulty, nil)
		require.NoError(t, err)

		c := &Client{client: &inProcessAPIClient{server: server}, logger: ulogger.NewErrorTestLogger(t), settings: test.CreateBaseTestSettings(t)}

		headers, metas, err := c.GetBlockHeadersByParentLinks(ctx, tip.Hash(), 1)
		require.Nil(t, headers)
		require.Nil(t, metas)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
	})
}

func TestClientGetBestBlockHeaderUncached(t *testing.T) {
	ctx := context.Background()
	server, client, blocks := parentLinkFixture(t)
	tip := blocks[len(blocks)-1]

	t.Run("returns the tip header with its id and every meta field", func(t *testing.T) {
		header, meta, err := client.GetBestBlockHeaderUncached(ctx)
		require.NoError(t, err)
		require.Equal(t, tip.Hash().String(), header.Hash().String())

		_, wantMeta, err := server.store.GetBestBlockHeaderUncached(ctx)
		require.NoError(t, err)
		require.NotZero(t, meta.ID, "the uncached call carries the block id")
		require.Equal(t, wantMeta, meta)
		require.Equal(t, uint32(len(blocks)), meta.Height) //nolint:gosec // small test heights
	})

	t.Run("a server error is unwrapped to its kind", func(t *testing.T) {
		store, _ := newChainStore(t, 1)
		faulty := &faultyHeaderStore{Store: store, err: errors.NewStorageError("disk gone")}
		failing, err := New(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), faulty, nil)
		require.NoError(t, err)

		c := &Client{client: &inProcessAPIClient{server: failing}, logger: ulogger.NewErrorTestLogger(t), settings: test.CreateBaseTestSettings(t)}

		header, meta, err := c.GetBestBlockHeaderUncached(ctx)
		require.Nil(t, header)
		require.Nil(t, meta)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
	})

	t.Run("an undecodable header is an error", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClientUncached{resp: &blockchain_api.GetBlockHeaderResponse{BlockHeader: []byte{1, 2, 3}}},
			logger:   ulogger.NewErrorTestLogger(t),
			settings: test.CreateBaseTestSettings(t),
		}

		header, meta, err := c.GetBestBlockHeaderUncached(ctx)
		require.Error(t, err)
		require.Nil(t, header)
		require.Nil(t, meta)
	})
}

// mockBlockClientUncached fakes the wire only to deliver bytes no real server would send.
type mockBlockClientUncached struct {
	blockchain_api.BlockchainAPIClient
	resp *blockchain_api.GetBlockHeaderResponse
}

func (m *mockBlockClientUncached) GetBestBlockHeaderUncached(context.Context, *emptypb.Empty, ...grpc.CallOption) (*blockchain_api.GetBlockHeaderResponse, error) {
	return m.resp, nil
}

// LocalClient hands both calls to the store untouched, so a real store's answers must come
// through in order and with the block id.
func TestLocalClientParentLinksAndUncachedBest(t *testing.T) {
	ctx := context.Background()
	store, blocks := newChainStore(t, 6)

	c, err := NewLocalClient(ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), store, nil, nil)
	require.NoError(t, err)

	tip := blocks[len(blocks)-1]

	headers, metas, err := c.GetBlockHeadersByParentLinks(ctx, tip.Hash(), 3)
	require.NoError(t, err)
	require.Equal(t, []string{
		blocks[5].Hash().String(), blocks[4].Hash().String(), blocks[3].Hash().String(),
	}, headerHashes(t, headers))
	require.Len(t, metas, 3)
	require.Equal(t, uint32(6), metas[0].Height)

	best, meta, err := c.GetBestBlockHeaderUncached(ctx)
	require.NoError(t, err)
	require.Equal(t, tip.Hash().String(), best.Hash().String())
	require.NotZero(t, meta.ID)
}
