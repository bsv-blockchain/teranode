package store

import "context"

// The Store is a simple key/value store.
// The key is a sha256 hash of

type Store interface {
	// SetAndGet will key/value providing the key does not already exist.
	// If the key already exists, the value will NOT be set and the existing value will be returned.
	// If the existing value is not the same as the value passed in, bool will be false.
	SetAndGet(ctx context.Context, key string, value string) (string, bool, error)

	// Delete deletes the value for the given key.
	Delete(ctx context.Context, key string) error
}
