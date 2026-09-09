package blockvalidation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type promotionLogRecorder struct {
	ulogger.TestLogger
	warnings []string
	infos    []string
}

func (l *promotionLogRecorder) Warnf(format string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}
func (l *promotionLogRecorder) Infof(format string, args ...interface{}) {
	l.infos = append(l.infos, fmt.Sprintf(format, args...))
}

func TestRestoreFSMState_CancelledBeforePromotionIsQuiet(t *testing.T) {
	server, client, store, catchup := newPromotionAuthority(t)
	logs := &promotionLogRecorder{}
	server.logger = logs
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.restoreFSMState(ctx, catchup)
	require.Zero(t, client.runCalls)
	require.Empty(t, logs.warnings, "normal shutdown must not warn about a failed zero-attempt promotion")
	requirePromotionState(t, client, store, blockchain.FSMStateCATCHINGBLOCKS)
}

func TestRestoreFSMState_OperatorIdleIsNotAnIncident(t *testing.T) {
	server, client, store, catchup := newPromotionAuthority(t)
	ctx := context.Background()
	_, err := client.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	_, err = client.authority.Idle(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	logs := &promotionLogRecorder{}
	server.logger = logs
	server.restoreFSMState(ctx, catchup)
	require.Equal(t, 1, client.runCalls)
	require.Empty(t, logs.warnings, "an operator's IDLE refusal is expected")
	require.Len(t, logs.infos, 1)
	require.Contains(t, logs.infos[0], "IDLE")
	requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
}

func TestRestoreFSMState_ReconciliationWarningDoesNotInventState(t *testing.T) {
	server, client, store, catchup := newPromotionAuthority(t)
	ctx := context.Background()
	_, err := client.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	store.fail = func() error { return errors.NewStorageError("lost STOP acknowledgement") }
	_, err = client.authority.Idle(ctx, &emptypb.Empty{})
	require.Error(t, err)
	store.fail = nil
	params := *server.settings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1}}
	server.settings.ChainCfgParams = &params
	logs := &promotionLogRecorder{}
	server.logger = logs
	server.restoreFSMState(ctx, catchup)
	require.Equal(t, 1, client.runCalls)
	require.Len(t, logs.warnings, 1)
	require.Contains(t, logs.warnings[0], "not durably confirmed")
	require.NotContains(t, logs.warnings[0], "CATCHINGBLOCKS")
	requirePromotionState(t, client, store, blockchain.FSMStateRUNNING)
}
