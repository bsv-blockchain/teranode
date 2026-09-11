package subtreevalidation

import (
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
)

// allowAssemblyForObservedFSM records suppression at an admission boundary.
// GetFSMCurrentState can return synthetic IDLE after notification-stream loss:
// this metric exposes that fail-closed window, not an authoritative pause state.
// Paths are fixed labels supplied only by the four internal entry paths.
func (s *Server) allowAssemblyForObservedFSM(state *blockchain.FSMStateType, path string) bool {
	if state != nil && *state == blockchain.FSMStateRUNNING {
		return true
	}
	observedState := "missing"
	if state != nil {
		switch *state {
		case blockchain.FSMStateIDLE:
			observedState = "idle"
		case blockchain.FSMStateCATCHINGBLOCKS:
			observedState = "catchingblocks"
		default:
			observedState = "unknown"
		}
	}
	prometheusAssemblyFeedingSuppressed.WithLabelValues(path, observedState).Inc()
	// Expected catchup suppresses feeding without indicating subscription loss.
	// Preserve the warning budget for unexpected suppression observations.
	if observedState == "catchingblocks" {
		return false
	}
	// One warning per service per minute; counters retain every observation.
	now := time.Now().UnixNano()
	last := s.assemblySuppressionLastWarning.Load()
	if (last == 0 || now-last >= int64(time.Minute)) && s.assemblySuppressionLastWarning.CompareAndSwap(last, now) {
		s.logger.Warnf("[SubtreeValidation] Assembly feeding suppressed: path=%s observed_state=%s; cached or synthetic IDLE may indicate subscription loss; check blockchain connectivity and FSM state; recovery may require an unmined transaction reload", path, observedState)
	}
	return false
}
