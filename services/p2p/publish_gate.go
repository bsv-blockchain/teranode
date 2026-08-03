package p2p

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
)

// topicKind identifies an outbound pubsub message class independent of the
// chain-specific prefix in the wire topic name.
type topicKind string

const (
	topicKindBlock      topicKind = "block"
	topicKindSubtree    topicKind = "subtree"
	topicKindRejectedTx topicKind = "rejected_tx"
	topicKindNodeStatus topicKind = "node_status"
)

// outboundTopicsAllowed declares, per blockchain FSM state, which outbound
// pubsub message classes the node may emit. Every publish goes through
// publishToNetwork, which drops (and counts) anything not listed here, so a
// new code path cannot silently leak announcements while the node is idle or
// catching up. node_status stays allowed in every state so peers can track
// our height during catchup.
var outboundTopicsAllowed = map[blockchain_api.FSMStateType]map[topicKind]bool{
	blockchain_api.FSMStateType_IDLE: {
		topicKindNodeStatus: true,
	},
	blockchain_api.FSMStateType_CATCHINGBLOCKS: {
		topicKindNodeStatus: true,
	},
	blockchain_api.FSMStateType_RUNNING: {
		topicKindBlock:      true,
		topicKindSubtree:    true,
		topicKindRejectedTx: true,
		topicKindNodeStatus: true,
	},
}

// fsmStateCacheTTL bounds GetFSMCurrentState calls from the publish path. A
// state change can take up to this long to be enforced, which matches the
// inherent check-then-publish race the previous per-call checks had.
const fsmStateCacheTTL = time.Second

// topicKindForName maps a wire topic name (with chain prefix) back to its
// message class. Unknown names fail closed in publishToNetwork.
func (s *Server) topicKindForName(topicName string) (topicKind, bool) {
	switch topicName {
	case s.blockTopicName:
		return topicKindBlock, true
	case s.subtreeTopicName:
		return topicKindSubtree, true
	case s.rejectedTxTopicName:
		return topicKindRejectedTx, true
	case s.nodeStatusTopicName:
		return topicKindNodeStatus, true
	default:
		return "", false
	}
}

// currentFSMState returns the blockchain FSM state, cached for fsmStateTTL
// (a zero TTL disables caching). A nil blockchain client reports RUNNING so
// setups without a blockchain stay permissive, matching the previous
// behavior of isBlockchainSyncingOrCatchingUp.
func (s *Server) currentFSMState(ctx context.Context) (blockchain_api.FSMStateType, error) {
	if s.blockchainClient == nil {
		return blockchain_api.FSMStateType_RUNNING, nil
	}

	s.fsmStateMu.Lock()
	defer s.fsmStateMu.Unlock()

	if s.fsmStateTTL > 0 && time.Now().Before(s.fsmStateExpiry) {
		return s.fsmStateCached, nil
	}

	fsmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	state, err := s.blockchainClient.GetFSMCurrentState(fsmCtx)
	if err != nil {
		return blockchain_api.FSMStateType_IDLE, err
	}

	s.fsmStateCached = *state
	s.fsmStateExpiry = time.Now().Add(s.fsmStateTTL)

	return *state, nil
}

// canSendToNetwork reports whether messages of the given class may be
// published in the current FSM state. State-fetch failures fail open so a
// briefly unreachable blockchain service does not silence the node.
func (s *Server) canSendToNetwork(ctx context.Context, kind topicKind) bool {
	state, err := s.currentFSMState(ctx)
	if err != nil {
		s.logger.Warnf("[canSendToNetwork] allowing %s publish, error getting blockchain FSM state: %v", kind, err)
		return true
	}

	return outboundTopicsAllowed[state][kind]
}

// shouldSkipNotification applies the outbound allow-list before any work is
// done for a blockchain notification that would end in a network publish.
// Control notifications (e.g. PeerFailure, which drives peer switching
// during catchup) are never skipped.
func (s *Server) shouldSkipNotification(ctx context.Context, notificationType model.NotificationType) bool {
	var kind topicKind

	switch notificationType {
	case model.NotificationType_Block:
		kind = topicKindBlock
	case model.NotificationType_Subtree:
		kind = topicKindSubtree
	default:
		return false
	}

	return !s.canSendToNetwork(ctx, kind)
}

// publishToNetwork is the single choke point in front of P2PClient.Publish:
// it drops any message whose class is not allowed in the current FSM state
// (or whose topic is unknown - fail closed), logging the drop and counting
// it in prometheus so a leaking code path is visible instead of silent.
func (s *Server) publishToNetwork(ctx context.Context, topicName string, msgBytes []byte) error {
	initPrometheusMetrics()

	kind, ok := s.topicKindForName(topicName)
	if !ok {
		s.logger.Errorf("[publishToNetwork] dropping publish to unknown topic %s: not in the outbound allow-list", topicName)
		prometheusP2PPublishBlocked.WithLabelValues("unknown", "unknown").Inc()

		return nil
	}

	state, err := s.currentFSMState(ctx)
	if err != nil {
		s.logger.Warnf("[publishToNetwork] allowing %s publish, error getting blockchain FSM state: %v", kind, err)
		return s.P2PClient.Publish(ctx, topicName, msgBytes)
	}

	if !outboundTopicsAllowed[state][kind] {
		s.logger.Errorf("[publishToNetwork] dropping %s publish: not allowed in FSM state %s", kind, state.String())
		prometheusP2PPublishBlocked.WithLabelValues(string(kind), state.String()).Inc()

		return nil
	}

	return s.P2PClient.Publish(ctx, topicName, msgBytes)
}
