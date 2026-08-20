package blockchain

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// blockAssemblyFullMetadataKey is the notification metadata key that carries the transaction
// ingress flag. The producer (block assembly) and the consumers (the clients that cache the flag)
// both go through this file, so they cannot drift apart on the key or its encoding.
const blockAssemblyFullMetadataKey = "full"

// blockAssemblyFullTTL is how long a cached "block assembly is full" survives without being
// re-announced. Past it, the client reports not-full again.
//
// The expiry exists because a cached refusal has no other way to be cleared. Block assembly only
// publishes while it has a limit configured, so nothing announces the change when an operator sets
// blockassembly_maxTransactionsInMemory back to 0, or when block assembly stops. Without an expiry
// every ingress point would keep refusing transactions until it was itself restarted.
//
// Expiring towards not-full is the safe direction, and it matches the documented default that a
// client which has heard nothing accepts transactions.
//
// This must stay comfortably above the publisher's re-announce period,
// blockassembly.txIngressHeartbeatInterval (10s), or a genuinely full block assembly would look
// stale between heartbeats and ingress would flap. It is deliberately a loose multiple: the cost of
// expiring late is a few extra seconds of refusal, while expiring early reopens ingress against a
// block assembly that is actually full.
const blockAssemblyFullTTL = 60 * time.Second

// NewBlockAssemblyFullNotification builds the notification that block assembly publishes when its
// in-memory transaction limit is crossed in either direction.
//
// The flag is encoded as a bool string in the notification metadata. Hash and Base_URL carry no
// meaning for this notification type and are set to their zero values.
func NewBlockAssemblyFullNotification(full bool) *Notification {
	return &Notification{
		Type:     model.NotificationType_BlockAssemblyFull,
		Hash:     (&chainhash.Hash{})[:],
		Base_URL: "",
		Metadata: &NotificationMetadata{
			Metadata: map[string]string{
				blockAssemblyFullMetadataKey: strconv.FormatBool(full),
			},
		},
	}
}

// blockAssemblyFullFromNotification reads the ingress flag out of a BlockAssemblyFull notification.
//
// Anything other than an explicit "true" reads as not full, so a malformed or missing value fails
// safe: the node keeps accepting transactions rather than refusing them on a value it cannot parse.
func blockAssemblyFullFromNotification(notification *Notification) bool {
	full, err := strconv.ParseBool(notification.GetMetadata().GetMetadata()[blockAssemblyFullMetadataKey])
	if err != nil {
		return false
	}

	return full
}

// blockAssemblyFullState is a client's cached copy of the transaction ingress flag, held together
// with the time it last heard from block assembly so a refusal can expire.
//
// Both ClientI implementations that cache the flag embed this, so they age it identically. The zero
// value reports not-full, which is the safe default for a client that has heard nothing.
type blockAssemblyFullState struct {
	full        atomic.Bool
	lastHeardAt atomic.Int64 // unix nanoseconds of the last notification, 0 if none
}

// set records what block assembly just announced, and reports whether the value changed.
//
// The timestamp is refreshed on every announcement, including a repeated full=true from the
// heartbeat, because it is the liveness signal that keeps the refusal from expiring.
func (s *blockAssemblyFullState) set(full bool, now time.Time) (changed bool) {
	s.lastHeardAt.Store(now.UnixNano())

	return s.full.Swap(full) != full
}

// isFull reports whether transaction ingress should be refused.
//
// A cached full=true is only honoured while it is fresh. Once blockAssemblyFullTTL has passed with
// no further announcement, block assembly is either running without a limit or is not running at
// all, and either way it is no longer asking for ingress to stop.
//
// The second return value is true on the single call that observes the expiry. Lapsing towards
// not-full is a fail-open: if the notification bus degrades — which is plausible precisely when the
// node is under memory pressure, and which the blockchain server can cause on its own by evicting a
// subscriber whose buffer filled — the limit silently stops being enforced. That is the right
// default, but it must not be invisible, so the caller gets one chance to say so. It is reported
// exactly once because the expiry also clears the cached flag: whoever wins the CompareAndSwap owns
// reporting it, and every later call takes the cheap not-full path above.
func (s *blockAssemblyFullState) isFull(now time.Time) (full bool, expired bool) {
	if !s.full.Load() {
		return false, false
	}

	lastHeardAt := s.lastHeardAt.Load()
	if lastHeardAt == 0 {
		// full was set without a timestamp, so treat it as fresh rather than silently ignoring it
		return true, false
	}

	if now.Sub(time.Unix(0, lastHeardAt)) <= blockAssemblyFullTTL {
		return true, false
	}

	return false, s.full.CompareAndSwap(true, false)
}
