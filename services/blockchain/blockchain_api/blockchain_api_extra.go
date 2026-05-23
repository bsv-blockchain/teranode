// Package blockchain_api provides additional utility methods for blockchain API types.
package blockchain_api

import (
	"fmt"

	"github.com/bsv-blockchain/teranode/util"
)

// Stringify returns a human-readable string representation of the notification.
// It includes the notification type, hex-encoded hash, and metadata.
func (n *Notification) Stringify() string {
	return fmt.Sprintf("%s: %s, metadata: %s", n.Type.String(), util.ReverseAndHexEncodeSlice(n.Hash), n.Metadata)
}
