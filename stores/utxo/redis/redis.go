package redis

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	redis_db "github.com/redis/go-redis/v9"
)

type Store struct {
	logger          ulogger.Logger
	ctx             context.Context // store the global context for things that run in the background
	version         string
	url             *url.URL
	client          *redis_db.Client
	expiration      time.Duration
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
}

type TXRecord struct {
	Version     uint32 `redis:"version"`
	LockTime    uint32 `redis:"locktime"`
	InputCount  int    `redis:"nrInput"`
	OutputCount int    `redis:"nrOutput"`
	BlockIDs    string `redis:"blockIDs"`
}

func New(ctx context.Context, logger ulogger.Logger, redisURL *url.URL) (*Store, error) {
	addr := fmt.Sprintf("%s:%s", redisURL.Hostname(), redisURL.Port())

	client := redis_db.NewClient(&redis_db.Options{
		Addr: addr,
	})

	expiration := time.Duration(0)

	var err error

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
		ctx:        ctx,
		version:    luaScriptVersion,
		url:        redisURL,
		client:     client,
		expiration: expiration,
		logger:     logger,
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

func getTxFromFields(txRecord TXRecord, data map[string]string) (tx *bt.Tx, err error) {
	tx = &bt.Tx{
		Version:  txRecord.Version,
		LockTime: txRecord.LockTime,
		Inputs:   make([]*bt.Input, txRecord.InputCount),
		Outputs:  make([]*bt.Output, txRecord.OutputCount),
	}

	// Read inputs
	for i := 0; i < txRecord.InputCount; i++ {
		inputKey := fmt.Sprintf("input:%d", i)

		input, ok := data[inputKey]
		if !ok {
			return nil, errors.NewTxInvalidError("input %d not found", i)
		}

		tx.Inputs[i] = &bt.Input{}

		in, err := hex.DecodeString(input)
		if err != nil {
			return nil, errors.NewTxInvalidError("could not decode input", err)
		}

		_, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(in))
		if err != nil {
			return nil, errors.NewTxInvalidError("could not read input", err)
		}
	}

	// Read outputs
	for i := 0; i < txRecord.OutputCount; i++ {
		outputKey := fmt.Sprintf("output:%d", i)

		output, ok := data[outputKey]
		if !ok {
			return nil, errors.NewTxInvalidError("output %d not found", i)
		}

		tx.Outputs[i] = &bt.Output{}

		out, err := hex.DecodeString(output)
		if err != nil {
			return nil, errors.NewTxInvalidError("could not decode output", err)
		}

		_, err = tx.Outputs[i].ReadFrom(bytes.NewReader(out))
		if err != nil {
			return nil, errors.NewTxInvalidError("could not read output", err)
		}
	}

	return tx, nil
}
