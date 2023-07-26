package db

import (
	"context"
)

type IStore interface {
	Create(ctx context.Context) error
	Update(ctx context.Context) error
	Delete(ctx context.Context) error
	Get(ctx context.Context) error
}
