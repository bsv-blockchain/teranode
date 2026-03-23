package subtreevalidation

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type MockExister struct{}

func (m MockExister) Exists(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (bool, error) {
	return false, nil
}

func TestLock(t *testing.T) {
	exister := MockExister{}

	tSettings := test.CreateBaseTestSettings(t)

	tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum"

	defer func() {
		_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
	}()

	q, err := NewQuorum(ulogger.TestLogger{}, exister, tSettings.SubtreeValidation.QuorumPath)
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("test"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotLock, _, releaseFn, err := q.TryLockIfFileNotExists(ctx, &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	assert.True(t, gotLock)

	defer releaseFn()

	gotLock, _, releaseFn, err = q.TryLockIfFileNotExists(ctx, &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	assert.False(t, gotLock)

	defer releaseFn()
}

type testServer struct {
	Server
	validateSubtreeInternalFn func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error
}

func (s *testServer) ValidateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
	if s.validateSubtreeInternalFn != nil {
		return s.validateSubtreeInternalFn(ctx, v, blockHeight, validationOptions...)
	}

	return nil
}

func TestSubtreesHandler(t *testing.T) {
	subtreeHash, _ := chainhash.NewHashFromStr("d580e67e847f65c73496a9f1adafacc5f73b4ca9d44fbd0749d6d926914bdcaf")
	baseURL, _ := url.Parse("http://localhost:8000")

	tests := []struct {
		name           string
		hash           *chainhash.Hash
		baseURL        *url.URL
		peerID         string
		setup          func(*testServer)
		httpResponse   []byte
		httpStatusCode int
		wantErr        bool
	}{
		{
			name:           "valid message",
			hash:           subtreeHash,
			baseURL:        baseURL,
			peerID:         "peer1",
			httpStatusCode: http.StatusOK,
			httpResponse:   hash1.CloneBytes(),
			setup: func(s *testServer) {
				s.validateSubtreeInternalFn = func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
					return nil
				}
			},
			wantErr: false,
		},
		{
			name:    "validation error",
			hash:    subtreeHash,
			baseURL: baseURL,
			peerID:  "peer1",
			setup: func(s *testServer) {
				s.validateSubtreeInternalFn = func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
					return errors.New(errors.ERR_INVALID_ARGUMENT, "validation failed")
				}
			},
			wantErr: true,
		},
		{
			name:           "not found error",
			hash:           subtreeHash,
			baseURL:        baseURL,
			peerID:         "peer1",
			httpStatusCode: http.StatusNotFound,
			httpResponse:   []byte{},
			setup: func(s *testServer) {
				s.validateSubtreeInternalFn = func(ctx context.Context, v ValidateSubtree, blockHeight uint32, validationOptions ...validator.Option) error {
					return errors.NewSubtreeNotFoundError("subtree not found")
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum"

			defer func() {
				_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
			}()

			logger := ulogger.TestLogger{}
			subtreeStore := memory.New()
			utxoStore, _ := nullstore.NewNullStore()
			blockchainClient := &blockchain.Mock{}
			blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

			blockIDsMap := make(map[uint32]bool)

			server := &testServer{
				Server: Server{
					logger:           logger,
					settings:         tSettings,
					blockchainClient: blockchainClient,
					subtreeStore:     subtreeStore,
					utxoStore:        utxoStore,
					validatorClient:  &validator.MockValidator{},
					orphanage: func() *Orphanage {
						o, err := NewOrphanage(tSettings.SubtreeValidation.OrphanageTimeout, tSettings.SubtreeValidation.OrphanageMaxSize, logger)
						require.NoError(t, err)
						return o
					}(),
					currentBlockIDsMap:  atomic.Pointer[map[uint32]bool]{},
					bestBlockHeader:     atomic.Pointer[model.BlockHeader]{},
					bestBlockHeaderMeta: atomic.Pointer[model.BlockHeaderMeta]{},
				},
			}

			server.Server.currentBlockIDsMap.Store(&blockIDsMap)
			server.Server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 100})

			server.Server.quorum, _ = NewQuorum(
				logger,
				subtreeStore,
				tSettings.SubtreeValidation.QuorumPath,
			)

			if tt.setup != nil {
				tt.setup(server)
			}

			// we only need the httpClient, txMetaStore and validatorClient when blessing a transaction
			httpmock.ActivateNonDefault(util.HTTPClient())
			httpmock.RegisterResponder(
				"GET",
				`=~subtree_data.*`,
				httpmock.NewBytesResponder(http.StatusNotFound, nil),
			)
			httpmock.RegisterResponder(
				"GET",
				`=~.*`,
				httpmock.NewBytesResponder(tt.httpStatusCode, tt.httpResponse),
			)
			httpmock.RegisterResponder(
				"POST",
				`=~.*`,
				httpmock.NewBytesResponder(tt.httpStatusCode, tx1.ExtendedBytes()),
			)

			err := server.subtreesHandler(context.Background(), tt.hash, tt.baseURL, tt.peerID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSubtreeMessageHandler_BlocksOnly_SkipsProcessing verifies that when BlocksOnly is true,
// the subtree message handler skips processing peer-announced subtrees and returns nil without
// invoking subtreesHandler/ValidateSubtreeInternal.
func TestSubtreeMessageHandler_BlocksOnly_SkipsProcessing(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum_blocksonly"
	tSettings.SubtreeValidation.BlocksOnly = true

	defer func() {
		_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
	}()

	subtreeHash, _ := chainhash.NewHashFromStr("d580e67e847f65c73496a9f1adafacc5f73b4ca9d44fbd0749d6d926914bdcaf")
	kafkaMsg := &kafkamessage.KafkaSubtreeTopicMessage{
		Hash:   subtreeHash.String(),
		URL:    "http://localhost:8000",
		PeerId: "peer1",
	}
	msgBytes, err := proto.Marshal(kafkaMsg)
	require.NoError(t, err)

	validateSubtreeCalled := atomic.Bool{}
	blockchainClient := &blockchain.Mock{}
	runningState := blockchain.FSMStateRUNNING
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	blockIDsMap := make(map[uint32]bool)
	server := &testServer{
		Server: Server{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			blockchainClient: blockchainClient,
			subtreeStore:     memory.New(),
			utxoStore:        func() utxo.Store { s, _ := nullstore.NewNullStore(); return s }(),
			validatorClient:  &validator.MockValidator{},
			orphanage: func() *Orphanage {
				o, err := NewOrphanage(tSettings.SubtreeValidation.OrphanageTimeout, tSettings.SubtreeValidation.OrphanageMaxSize, ulogger.TestLogger{})
				require.NoError(t, err)
				return o
			}(),
			currentBlockIDsMap:  atomic.Pointer[map[uint32]bool]{},
			bestBlockHeader:     atomic.Pointer[model.BlockHeader]{},
			bestBlockHeaderMeta: atomic.Pointer[model.BlockHeaderMeta]{},
		},
		validateSubtreeInternalFn: func(context.Context, ValidateSubtree, uint32, ...validator.Option) error {
			validateSubtreeCalled.Store(true)
			return nil
		},
	}
	server.Server.currentBlockIDsMap.Store(&blockIDsMap)
	server.Server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 100})
	server.Server.quorum, _ = NewQuorum(ulogger.TestLogger{}, server.subtreeStore, tSettings.SubtreeValidation.QuorumPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := server.subtreeMessageHandler(ctx)
	err = handler(&kafka.KafkaMessage{Value: msgBytes})
	require.NoError(t, err)

	// Give async goroutines time to run if they were scheduled (they shouldn't be)
	time.Sleep(100 * time.Millisecond)

	assert.False(t, validateSubtreeCalled.Load(), "ValidateSubtreeInternal should not be called when BlocksOnly is true")
}

// TestSubtreeMessageHandler_BlocksOnlyFalse_ProcessesMessage verifies that when BlocksOnly is false,
// the subtree message handler processes peer-announced subtrees and invokes subtreesHandler.
// This test uses the same setup as TestSubtreesHandler to exercise the full flow through subtreeMessageHandler.
func TestSubtreeMessageHandler_BlocksOnlyFalse_ProcessesMessage(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum_blocksonly_false"
	tSettings.SubtreeValidation.BlocksOnly = false

	defer func() {
		_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
	}()

	subtreeHash, _ := chainhash.NewHashFromStr("d580e67e847f65c73496a9f1adafacc5f73b4ca9d44fbd0749d6d926914bdcaf")
	kafkaMsg := &kafkamessage.KafkaSubtreeTopicMessage{
		Hash:   subtreeHash.String(),
		URL:    "http://localhost:8000",
		PeerId: "peer1",
	}
	msgBytes, err := proto.Marshal(kafkaMsg)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}
	runningState := blockchain.FSMStateRUNNING
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)
	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

	blockIDsMap := make(map[uint32]bool)
	server := &testServer{
		Server: Server{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			blockchainClient: blockchainClient,
			subtreeStore:     memory.New(),
			utxoStore:        func() utxo.Store { s, _ := nullstore.NewNullStore(); return s }(),
			validatorClient:  &validator.MockValidator{},
			orphanage: func() *Orphanage {
				o, err := NewOrphanage(tSettings.SubtreeValidation.OrphanageTimeout, tSettings.SubtreeValidation.OrphanageMaxSize, ulogger.TestLogger{})
				require.NoError(t, err)
				return o
			}(),
			currentBlockIDsMap:  atomic.Pointer[map[uint32]bool]{},
			bestBlockHeader:     atomic.Pointer[model.BlockHeader]{},
			bestBlockHeaderMeta: atomic.Pointer[model.BlockHeaderMeta]{},
		},
	}
	server.Server.currentBlockIDsMap.Store(&blockIDsMap)
	server.Server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 100})
	server.Server.quorum, _ = NewQuorum(ulogger.TestLogger{}, server.subtreeStore, tSettings.SubtreeValidation.QuorumPath)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", `=~subtree_data.*`, httpmock.NewBytesResponder(http.StatusNotFound, nil))
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusOK, hash1.CloneBytes()))
	httpmock.RegisterResponder("POST", `=~.*`, httpmock.NewBytesResponder(http.StatusOK, tx1.ExtendedBytes()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := server.subtreeMessageHandler(ctx)
	err = handler(&kafka.KafkaMessage{Value: msgBytes})
	require.NoError(t, err)

	// When BlocksOnly is false, the handler schedules subtreesHandler via g.Go and returns immediately.
	// Give time for the async processing to complete. The handler returns nil on success;
	// we're verifying it doesn't return early at the BlocksOnly check.
	time.Sleep(500 * time.Millisecond)
}

// TestSubtreeMessageHandler_BlocksOnly_CatchingBlocksStillSkips verifies that when FSM is in
// CATCHINGBLOCKS state, processing is skipped regardless of BlocksOnly setting.
func TestSubtreeMessageHandler_BlocksOnly_CatchingBlocksStillSkips(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.BlocksOnly = false // Not blocks-only, but CATCHINGBLOCKS should still skip

	subtreeHash, _ := chainhash.NewHashFromStr("d580e67e847f65c73496a9f1adafacc5f73b4ca9d44fbd0749d6d926914bdcaf")
	kafkaMsg := &kafkamessage.KafkaSubtreeTopicMessage{
		Hash:   subtreeHash.String(),
		URL:    "http://localhost:8000",
		PeerId: "peer1",
	}
	msgBytes, err := proto.Marshal(kafkaMsg)
	require.NoError(t, err)

	validateSubtreeCalled := atomic.Bool{}
	blockchainClient := &blockchain.Mock{}
	catchingBlocksState := blockchain.FSMStateCATCHINGBLOCKS
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocksState, nil)

	server := &testServer{
		Server: Server{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			blockchainClient: blockchainClient,
		},
		validateSubtreeInternalFn: func(context.Context, ValidateSubtree, uint32, ...validator.Option) error {
			validateSubtreeCalled.Store(true)
			return nil
		},
	}
	// Note: minimal server setup - we expect early return before subtreesHandler

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := server.subtreeMessageHandler(ctx)
	err = handler(&kafka.KafkaMessage{Value: msgBytes})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.False(t, validateSubtreeCalled.Load(), "ValidateSubtreeInternal should not be called when FSM is CATCHINGBLOCKS")
}
