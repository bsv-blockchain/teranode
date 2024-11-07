package redis2

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
)

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	// Basic connectivity check with PING
	if err := s.client.Ping(ctx).Err(); err != nil {
		return -1, "NO_PING", errors.NewStorageError("Redis ping failed", err)
	}

	// If liveness check requested, try a basic SET/GET operation
	if checkLiveness {
		key := "health_check"
		value := "ok"

		pipe := s.client.Pipeline()
		pipe.Set(ctx, key, value, time.Second) // 1 second is minimum TTL for Redis
		pipe.Get(ctx, key)
		pipe.Del(ctx, key)

		if _, err := pipe.Exec(ctx); err != nil {
			return -2, "NO_SET_GET_DEL", errors.NewStorageError("Redis operations check failed", err)
		}
	}

	return 0, "OK", nil
}
