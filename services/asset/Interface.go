package asset

import "context"

type PeerI interface {
	Start(ctx context.Context) error
	Stop() error
}
