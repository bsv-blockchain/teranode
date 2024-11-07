package redis2

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	redis_db "github.com/redis/go-redis/v9"
)

type Store struct {
	logger           ulogger.Logger
	ctx              context.Context // store the global context for things that run in the background
	version          string
	url              *url.URL
	client           *redis_db.Client
	transactionStore blob.Store
	expiration       time.Duration
	blockHeight      atomic.Uint32
	medianBlockTime  atomic.Uint32
}

func New(ctx context.Context, logger ulogger.Logger, redisURL *url.URL) (*Store, error) {
	addr := fmt.Sprintf("%s:%s", redisURL.Hostname(), redisURL.Port())

	client := redis_db.NewClient(&redis_db.Options{
		Addr: addr,
	})

	expiration := time.Duration(0)

	var err error

	transactionStoreURL, err := url.Parse(redisURL.Query().Get("transactionStore"))
	if err != nil {
		return nil, err
	}

	transactionStore, err := blob.NewStore(logger, transactionStoreURL)
	if err != nil {
		return nil, err
	}

	expirationValue := redisURL.Query().Get("expiration")
	if expirationValue != "" {
		expiration, err = time.ParseDuration(expirationValue)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("could not parse expiration %s", expirationValue, err)
		}

		if expiration == 0 {
			logger.Infof("expiration is set to 0 meaning the default Redis TTL setting will be used")
		}
	}

	// Make sure the udf lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err := registerLuaIfNecessary(ctx, logger, client); err != nil {
		return nil, errors.NewStorageError("Failed to register Redis LUA", err)
	}

	return &Store{
		ctx:              ctx,
		version:          luaScriptVersion,
		url:              redisURL,
		client:           client,
		transactionStore: transactionStore,
		expiration:       expiration,
		logger:           logger,
	}, nil
}

// This is used by testing to inject a specific version of LUA functions
func (s *Store) setVersion(version string) {
	s.version = version
}

// internal state functions
func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)

	return nil
}

func (s *Store) GetBlockHeight() uint32 {
	return s.blockHeight.Load()
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	s.logger.Debugf("setting median block time to %d", medianTime)
	s.medianBlockTime.Store(medianTime)

	return nil
}

func (s *Store) GetMedianBlockTime() uint32 {
	return s.medianBlockTime.Load()
}
