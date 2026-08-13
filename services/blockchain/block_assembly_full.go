package blockchain

import (
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// blockAssemblyFullMetadataKey is the notification metadata key that carries the transaction
// ingress flag. The producer (block assembly) and the consumers (the clients that cache the flag)
// both go through this file, so they cannot drift apart on the key or its encoding.
const blockAssemblyFullMetadataKey = "full"

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
