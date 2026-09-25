package blockchain

import (
	"context"
	stderrors "errors" //nolint:depguard // Pause needs identity matching; domain errors match every error sharing a code.

	"github.com/bsv-blockchain/teranode/errors"
	"google.golang.org/grpc/status"
)

// ErrCatchupPaused means a ready, persistence-confirmed authority reported IDLE.
// It never represents unavailable authority or a generic FSM transition error.
var ErrCatchupPaused = stderrors.New("catchup paused by blockchain authority")

// catchupTransitionError preserves transport codes and distinguishes an operator
// pause from other rejected transitions. A separate authoritative read is needed
// because STATE_ERROR also covers permanent invalid transitions.
func catchupTransitionError(ctx context.Context, rpcErr error, read func(context.Context) (FSMStateType, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rpcErr == nil {
		return nil
	}
	st, ok := status.FromError(rpcErr)
	if !ok || len(st.Details()) == 0 {
		return rpcErr
	}
	err := errors.UnwrapGRPC(rpcErr)
	if err == nil {
		return rpcErr
	}
	if !errors.Is(err, errors.ErrStateError) {
		return err
	}
	state, readErr := read(ctx)
	if readErr != nil {
		return readErr
	}
	if state == FSMStateIDLE {
		return ErrCatchupPaused
	}
	return err
}
