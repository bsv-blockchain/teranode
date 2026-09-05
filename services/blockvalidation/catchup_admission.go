package blockvalidation

import (
	"context"
	"time"
)

const (
	catchupAdmissionTimeout       = 5 * time.Second
	catchupAdmissionRetryInterval = time.Second
)

// waitForCatchupAdmission retains the caller's current unit until the blockchain
// authority accepts it. Its state snapshot is serialized with durable STOP;
// an accepted unit may finish after STOP, but its successor needs new admission.
// Cached FSM state must not be consulted here: cached IDLE can be synthetic, and
// cached CATCHINGBLOCKS can predate an operator's STOP.
func (u *Server) waitForCatchupAdmission(ctx context.Context) error {
	waiting := false
	return waitForCatchupAdmission(ctx, func(rpcCtx context.Context) error {
		err := u.blockchainClient.AdmitCatchupWork(rpcCtx)
		if err != nil && !waiting {
			u.logger.Infof("[catchup] Waiting for authoritative work admission: %v", err)
			waiting = true
		} else if err == nil && waiting {
			u.logger.Infof("[catchup] Authoritative work admission restored")
		}
		return err
	}, catchupAdmissionTimeout, catchupAdmissionRetryInterval)
}

// Each failed or ambiguous RPC outcome leaves the unit unstarted. In particular,
// a timeout may have reached the server, but never grants permission locally.
// Retry waits use the service context, not the short-lived RPC context. Only
// service cancellation ends a suspended admission; it is not a peer failure.
func waitForCatchupAdmission(ctx context.Context, admit func(context.Context) error, rpcTimeout, retryInterval time.Duration) error {
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
		if err == nil {
			return nil
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
