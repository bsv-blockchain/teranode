package blockchain

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func requireIdleBoundaryState(t *testing.T, b *Blockchain, store *fsmPersistenceStore, want FSMStateType) {
	t.Helper()
	require.Equal(t, want.String(), b.finiteStateMachine.Current())
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, want.String(), persisted)
}

func TestIdle_AutomaticCatchupCannotEscapeOperatorStop(t *testing.T) {
	ctx := context.Background()
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	_, err := b.Idle(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	requireIdleBoundaryState(t, b, store, FSMStateIDLE)
	require.Len(t, b.notifications, 1)
	<-b.notifications // Consume the successful operator STOP notification.

	_, catchupErr := b.CatchUpBlocks(ctx, &emptypb.Empty{})
	_, runErr := b.Run(ctx, &emptypb.Empty{})
	require.Error(t, catchupErr, "automatic catchup must not escape operator IDLE")
	require.Error(t, runErr, "automatic RUN must not complete an escape through catchup")
	requireIdleBoundaryState(t, b, store, FSMStateIDLE)
	require.Empty(t, b.notifications, "refused automatic events must not notify")

	_, err = b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err, "explicit operator catchup must still resume a parked node")
	requireIdleBoundaryState(t, b, store, FSMStateCATCHINGBLOCKS)
	_, err = b.Run(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	requireIdleBoundaryState(t, b, store, FSMStateRUNNING)
}

func TestIdle_AutomaticCatchupFromRunningStillWorks(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	_, err := b.CatchUpBlocks(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	requireIdleBoundaryState(t, b, store, FSMStateCATCHINGBLOCKS)
	_, err = b.Run(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	requireIdleBoundaryState(t, b, store, FSMStateRUNNING)
}

func TestIdle_StopSerializesAutomaticCatchup(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var entered, released sync.Once
	t.Cleanup(func() { released.Do(func() { close(releaseWrite) }) })
	store.beforeWrite = func(context.Context) {
		entered.Do(func() { close(writeStarted) })
		<-releaseWrite
	}
	stopResult := make(chan error, 1)
	go func() {
		_, err := b.Idle(context.Background(), &emptypb.Empty{})
		stopResult <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("operator STOP did not reach its persistence boundary")
	}
	catchupStarted := make(chan struct{})
	catchupResult := make(chan error, 1)
	go func() {
		close(catchupStarted)
		_, err := b.CatchUpBlocks(context.Background(), &emptypb.Empty{})
		catchupResult <- err
	}()
	<-catchupStarted
	released.Do(func() { close(releaseWrite) })
	select {
	case err := <-stopResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("operator STOP did not complete")
	}
	select {
	case err := <-catchupResult:
		require.Error(t, err, "catchup waiting behind STOP must observe durable IDLE")
	case <-time.After(time.Second):
		t.Fatal("automatic catchup did not complete")
	}
	requireIdleBoundaryState(t, b, store, FSMStateIDLE)
	require.Len(t, b.notifications, 1, "only operator STOP should notify")
}
