package servicemanager

import "context"

// Service is an interface for services that can be started and stopped.
type Service interface {
	Init(ctx context.Context) error
	Start(ctx context.Context, ready chan<- struct{}) error
	Stop(ctx context.Context) error
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
}
