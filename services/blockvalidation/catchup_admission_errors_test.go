package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCatchupAdmission_PermanentErrorsReturnImmediately(t *testing.T) {
	for _, failure := range []error{errors.NewStateError("invalid transition"), errors.NewConfigurationError("invalid configuration"), status.Error(codes.PermissionDenied, "denied")} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			attempts := 0
			err := waitForCatchupAdmission(ctx, func(context.Context) error { attempts++; return failure }, time.Second, time.Millisecond)
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, attempts)
		})
	}
}

func TestCatchupAdmission_TransientErrorsExhaustBudget(t *testing.T) {
	for _, failure := range []error{status.Error(codes.Unavailable, "restarting"), status.Error(codes.Unimplemented, "rolling upgrade"), context.DeadlineExceeded, errors.NewStorageError("unavailable")} {
		t.Run(failure.Error(), func(t *testing.T) {
			attempts := 0
			err := waitForCatchupAdmissionWithBudget(context.Background(), func(context.Context) error { attempts++; return failure }, time.Second, time.Millisecond, 8*time.Millisecond)
			require.ErrorIs(t, err, failure)
			require.Greater(t, attempts, 1)
		})
	}
}

func TestCatchupAdmission_ConfirmedPauseDoesNotConsumeFailureBudget(t *testing.T) {
	attempts := 0
	err := waitForCatchupAdmission(context.Background(), func(context.Context) error {
		attempts++
		if attempts <= 10 {
			return blockchain.ErrCatchupPaused
		}
		return nil
	}, time.Second, time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 11, attempts)
}

func TestCatchupAdmission_PauseDoesNotEraseTransientFailureBudget(t *testing.T) {
	attempts := 0
	failure := status.Error(codes.Unavailable, "authority unavailable")
	err := waitForCatchupAdmissionWithBudget(context.Background(), func(context.Context) error {
		attempts++
		if attempts%2 == 0 {
			return blockchain.ErrCatchupPaused
		}
		return failure
	}, time.Second, time.Millisecond, 8*time.Millisecond)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, errors.ErrServiceError, "authority outages must remain local failures, not peer faults")
	require.Greater(t, attempts, 3)
}

func TestCatchupAdmission_CancellationInterruptsLongPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	err := waitForCatchupAdmission(ctx, func(context.Context) error {
		attempts++
		if attempts == 10 {
			cancel()
		}
		return blockchain.ErrCatchupPaused
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 10, attempts)
}

func TestCatchupAdmission_PauseFreezesSpentRecoveryBudget(t *testing.T) {
	attempts := 0
	err := waitForCatchupAdmissionWithBudget(context.Background(), func(context.Context) error {
		attempts++
		switch attempts {
		case 1:
			return status.Error(codes.Unavailable, "restarting")
		case 2:
			return blockchain.ErrCatchupPaused
		case 3:
			time.Sleep(15 * time.Millisecond)
			return blockchain.ErrCatchupPaused
		case 4:
			return status.Error(codes.Unavailable, "restarting")
		default:
			return nil
		}
	}, time.Second, time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 5, attempts)
}
