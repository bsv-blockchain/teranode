package model

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

type Notification struct {
	Type     NotificationType
	Hash     *chainhash.Hash
	BaseURL  string
	Metadata NotificationMetadata
}
