package blockchain

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fsmReadTimeout bounds the whole RPC, including connection establishment.
const fsmReadTimeout = 5 * time.Second

// ReadFSMState returns a ready, persistence-confirmed snapshot without mutating
// the FSM. It must not wait behind a persistence write or notification delivery:
// both can hold fsmMu longer than a caller's deadline.
func (b *Blockchain) ReadFSMState(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !b.subscriptionManagerReady.Load() {
		return nil, status.Error(codes.Unavailable, "FSM authority is not ready")
	}
	if !b.fsmMu.TryLock() {
		return nil, status.Error(codes.Unavailable, "FSM authority is busy")
	}
	defer b.fsmMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if b.finiteStateMachine == nil || !b.subscriptionManagerReady.Load() || b.fsmPersistenceUncertain {
		return nil, status.Error(codes.Unavailable, "FSM authority is not ready or persistence is uncertain")
	}
	state, ok := blockchain_api.FSMStateType_value[b.finiteStateMachine.Current()]
	if !ok {
		return nil, status.Error(codes.Unavailable, "FSM authority has an unknown state")
	}
	return &blockchain_api.GetFSMStateResponse{State: FSMStateType(state)}, nil
}

// ReadFSMState bypasses the subscription cache. Transport status is preserved,
// including Unimplemented from older servers; no cached-state fallback is safe.
func (c *Client) ReadFSMState(ctx context.Context) (FSMStateType, error) {
	ctx, cancel := context.WithTimeout(ctx, fsmReadTimeout)
	defer cancel()
	response, err := c.client.ReadFSMState(ctx, &emptypb.Empty{})
	if err != nil {
		return FSMStateIDLE, err
	}
	if response == nil {
		return FSMStateIDLE, status.Error(codes.Unavailable, "FSM authority returned no state")
	}
	if _, ok := blockchain_api.FSMStateType_name[int32(response.State)]; !ok {
		return FSMStateIDLE, status.Error(codes.Unavailable, "FSM authority returned an unknown state")
	}
	return response.State, nil
}

// ReadFSMState cannot certify authority through LocalClient: it has no FSM or
// subscription readiness state. Its legacy getters remain compatible.
func (c *LocalClient) ReadFSMState(ctx context.Context) (FSMStateType, error) {
	if err := ctx.Err(); err != nil {
		return FSMStateIDLE, status.FromContextError(err).Err()
	}
	return FSMStateIDLE, status.Error(codes.Unavailable, "local blockchain client has no authoritative FSM")
}
