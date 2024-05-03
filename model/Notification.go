package model

import "github.com/libsv/go-bt/v2/chainhash"

type Notification struct {
	Type     NotificationType
	Hash     *chainhash.Hash
	BaseURL  string
	Metadata NotificationMetadata
}
