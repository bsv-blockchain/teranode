package store

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type NutcrackerStore struct {
	rdb *redis.Client
}

func NewNutcrackerStore() Store {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:22121",
		Password: "",
		DB:       0,
	})

	rdb.Options().Dialer = redis.NewDialer(nil)

	return &NutcrackerStore{
		rdb: rdb,
	}
}

// func (s *NutcrackerStore) Set(ctx context.Context, key string, value string) (string, error) {
// 	return s.rdb.Set(ctx, key, value, 0).Result()
// }

func (s *NutcrackerStore) SetAndGet(ctx context.Context, key string, value string) (string, bool, error) {
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

func (s *NutcrackerStore) Delete(ctx context.Context, key string) error {
	_, err := s.rdb.Del(ctx, key).Result()

	return err
}
