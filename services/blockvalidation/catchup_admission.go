package blockvalidation

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	catchupAdmissionTimeout          = 5 * time.Second
	catchupAdmissionRetryInterval    = time.Second
	catchupAdmissionRecoveryBudget   = 2 * time.Minute
	catchupAdmissionMaxRetryInterval = 5 * time.Second
)

// waitForCatchupAdmission retains the caller's current unit until the blockchain
// authority accepts it. Its state snapshot is serialized with durable STOP;
// an accepted unit may finish after STOP, but its successor needs new admission.
// Cached FSM state must not be consulted here: cached IDLE can be synthetic, and
// cached CATCHINGBLOCKS can predate an operator's STOP.
func (u *Server) waitForCatchupAdmission(ctx context.Context) error {
	waiting := false
	err := waitForCatchupAdmission(ctx, func(rpcCtx context.Context) error {
		err := u.blockchainClient.AdmitCatchupWork(rpcCtx)
		if status.Code(err) == codes.Unimplemented {
			u.logger.Warnf("[catchup] Blockchain authority lacks ReadFSMState admission RPC; waiting for blockchain service upgrade")
		}
		if err != nil && !waiting {
			u.logger.Infof("[catchup] Waiting for authoritative work admission: %v", err)
			waiting = true
		} else if err == nil && waiting {
			u.logger.Infof("[catchup] Authoritative work admission restored")
		}
		return err
	}, catchupAdmissionTimeout, catchupAdmissionRetryInterval)
	if err != nil && ctx.Err() == nil {
		u.logger.Warnf("[catchup] Authoritative work admission failed: %v", err)
	}
	return err
}

// Each failed or ambiguous RPC outcome leaves the unit unstarted. In particular,
// a timeout may have reached the server, but never grants permission locally.
// Retry waits use the service context, not the short-lived RPC context. A
// confirmed pause waits for explicit resume or cancellation. Transient failures
// have a bounded budget; permanent failures return immediately. Neither is proof
// of peer misbehavior. A pause does not erase earlier transient failures.
func waitForCatchupAdmission(ctx context.Context, admit func(context.Context) error, rpcTimeout, retryInterval time.Duration) error {
	return waitForCatchupAdmissionWithBudget(ctx, admit, rpcTimeout, retryInterval, catchupAdmissionRecoveryBudget)
}

func waitForCatchupAdmissionWithBudget(ctx context.Context, admit func(context.Context) error, rpcTimeout, retryInterval, recoveryBudget time.Duration) error {
	var firstFailure time.Time
	var pausedAt time.Time
	backoff := retryInterval
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		err := admit(rpcCtx)
		if err == nil {
			err = rpcCtx.Err()
		}
		cancel()
		if parentErr := ctx.Err(); parentErr != nil {
			return parentErr
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, blockchain.ErrCatchupPaused) {
			if pausedAt.IsZero() {
				pausedAt = time.Now()
			}
		} else {
			if !pausedAt.IsZero() {
				if !firstFailure.IsZero() {
					firstFailure = firstFailure.Add(time.Since(pausedAt))
				}
				pausedAt = time.Time{}
			}
			if !transientCatchupAdmissionError(err) {
				return catchupAdmissionFailure("catchup authority rejected work admission", err)
			}
			if firstFailure.IsZero() {
				firstFailure = time.Now()
			}
			if time.Since(firstFailure) >= recoveryBudget {
				return catchupAdmissionFailure(fmt.Sprintf("catchup admission failed after %s of authority recovery", recoveryBudget), err)
			}
		}
		wait := backoff
		if !errors.Is(err, blockchain.ErrCatchupPaused) {
			remaining := recoveryBudget - time.Since(firstFailure)
			if wait > remaining {
				wait = remaining
			}
			if backoff < catchupAdmissionMaxRetryInterval {
				backoff *= 2
				if backoff > catchupAdmissionMaxRetryInterval {
					backoff = catchupAdmissionMaxRetryInterval
				}
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// The same ServiceError code can also wrap a real peer request failure. Keep
// local admission provenance on a native error link so reputation decisions
// can distinguish those two sources after catchup adds more wrappers.
const catchupAdmissionFailureKey = "catchup_admission_failure"

func catchupAdmissionFailure(message string, cause error) error {
	marked := errors.NewServiceError(message, cause)
	marked.SetData(catchupAdmissionFailureKey, true)
	return marked
}

func isCatchupAdmissionFailure(err error) bool {
	var current *errors.Error
	if !errors.As(err, &current) {
		return false
	}
	for depth := 0; current != nil && depth < 32; depth++ {
		if marked, ok := current.GetData(catchupAdmissionFailureKey).(bool); ok && marked {
			return true
		}
		next := current.WrappedErr()
		if next == nil || !errors.As(next, &current) {
			return false
		}
	}
	return false
}

// RPC deadlines can expire while the service context remains usable. Canceled
// service contexts are checked by the caller before this classification.
func transientCatchupAdmissionError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.IsRetryableError(err) || errors.IsTransientLocalError(err) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unimplemented:
		return true
	default:
		return false
	}
}
