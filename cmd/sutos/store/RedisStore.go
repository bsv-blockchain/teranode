package store

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore() Store {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // direct
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	// 	opt, err := redis.ParseURL("redis://<user>:<pass>@localhost:6379/<db>")
	// if err != nil {
	// 	panic(err)
	// }

	return &RedisStore{
		rdb: rdb,
	}
}

// func (s *RedisStore) Set(ctx context.Context, key string, value string) (string, error) {
// 	return s.rdb.Set(ctx, key, value, 0).Result()
// }

func (s *RedisStore) SetAndGet(ctx context.Context, key string, value string) (string, bool, error) {
	ok, err := s.rdb.SetNX(ctx, key, value, 0).Result()
	if err != nil {
		return "", false, err
	}

	if ok {
		return value, true, nil
	}

	existing := s.rdb.Get(ctx, key).Val()
	return existing, existing == value, nil
}

func (s *RedisStore) Delete(ctx context.Context, key string) error {
	_, err := s.rdb.Del(ctx, key).Result()

	return err
}
