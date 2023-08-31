package blobserver

import "context"

type PeerI interface {
	Start(ctx context.Context) error
	Stop() error
}
