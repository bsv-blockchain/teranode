package pruner

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// prunerFSMClient uses a real local blockchain client, injecting only the FSM
// reply and completed mined status needed to exercise pruning admission.
type prunerFSMClient struct {
	blockchain.ClientI
	state *blockchain.FSMStateType
	err   error
}

func (c *prunerFSMClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	return c.state, c.err
}

func (c *prunerFSMClient) GetBlockIsMined(context.Context, *chainhash.Hash) (bool, error) {
	return true, nil
}

func TestPrunerSkipDuringCatchupRequiresRunning(t *testing.T) {
	state := func(s blockchain.FSMStateType) *blockchain.FSMStateType { return &s }
	for _, tt := range []struct {
		name      string
		state     *blockchain.FSMStateType
		err       error
		enabled   bool
		wantPrune bool
	}{
		{"running", state(blockchain.FSMStateRUNNING), nil, true, true},
		{"catchup", state(blockchain.FSMStateCATCHINGBLOCKS), nil, true, false},
		{"idle", state(blockchain.FSMStateIDLE), nil, true, false},
		{"unknown", state(blockchain.FSMStateType(99)), nil, true, false},
		{"missing", nil, nil, true, false},
		{"read error", nil, errors.NewProcessingError("FSM unavailable"), true, false},
		{"disabled in idle", state(blockchain.FSMStateIDLE), nil, false, true},
		{"disabled with read error", nil, errors.NewProcessingError("FSM unavailable"), false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			initPrometheusMetrics()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Pruner.SkipDuringCatchup = tt.enabled
			tSettings.Pruner.MinBlockHeight = 0
			tSettings.Pruner.BlockAssemblyWaitTimeout = time.Second
			logger := ulogger.TestLogger{}
			storeURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			store, err := blockchainstore.NewStore(logger, storeURL, tSettings)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
			require.NoError(t, err)
			assembly := &blockassembly.Mock{}
			assembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{
				BlockAssemblyState: "running", CurrentHeight: 2,
			}, nil).Maybe()
			server := &Server{
				ctx: ctx, logger: logger, settings: tSettings,
				blockchainClient:    &prunerFSMClient{ClientI: client, state: tt.state, err: tt.err},
				blockAssemblyClient: assembly,
				pruneNotify:         make(chan pruneSignal, 1), blobNotify: make(chan pruneSignal, 1),
			}
			skipReason := "fsm_not_running"
			if tt.err != nil {
				skipReason = "fsm_error"
			}
			skipsBefore := getCounterValue(t, prunerSkipped, skipReason)
			done := make(chan struct{})
			go func() { defer close(done); server.prunerProcessor(ctx) }()
			t.Cleanup(func() { cancel(); <-done })
			server.pruneNotify <- pruneSignal{blockHeight: 2}
			require.Eventually(t, func() bool {
				return server.lastProcessedHeight.Load() == 2 ||
					getCounterValue(t, prunerSkipped, skipReason) > skipsBefore
			}, time.Second, 10*time.Millisecond)
			if tt.wantPrune {
				require.Equal(t, uint32(2), server.lastProcessedHeight.Load())
				require.Len(t, server.blobNotify, 1)
			} else {
				require.Zero(t, server.lastProcessedHeight.Load(), "FSM must deny the whole pruning cycle even when assembly is ready")
				require.Empty(t, server.blobNotify)
			}
		})
	}
}
