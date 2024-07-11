// //go:build aerospike

package aerospike2

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	binNames = []string{
		"spendable",
		"fee",
		"size",
		"locktime",
		"utxos",
		"parentTxHashes",
		"blockIDs",
	}
)

type Store struct {
	url           *url.URL
	client        *uaerospike.Client
	namespace     string
	setName       string
	expiration    uint32
	blockHeight   atomic.Uint32
	logger        ulogger.Logger
	batchId       atomic.Uint64
	storeBatcher  *batcher.Batcher2[batchStoreItem]
	getBatcher    *batcher.Batcher2[batchGetItem]
	spendBatcher  *batcher.Batcher2[batchSpend]
	externalStore blob.Store
}

func New(logger ulogger.Logger, aerospikeURL *url.URL) (*Store, error) {
	initPrometheusMetrics()
	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	namespace := aerospikeURL.Path[1:]

	client, err := util.GetAerospikeClient(logger, aerospikeURL)
	if err != nil {
		return nil, err
	}

	placeholderKey, err = aerospike.NewKey(namespace, "placeholderKey", "placeHolderKey")
	if err != nil {
		log.Fatal("Failed to init placeholder key")
	}

	expiration := uint32(0)
	expirationValue := aerospikeURL.Query().Get("expiration")
	if expirationValue != "" {
		expiration64, err := strconv.ParseUint(expirationValue, 10, 64)
		if err != nil {
			logger.Fatalf("could not parse expiration %s: %v", expirationValue, err)
		}
		expiration = uint32(expiration64)
	}

	setName := aerospikeURL.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	externalStoreUrl, err := url.Parse(aerospikeURL.Query().Get("externalStore"))
	if err != nil {
		return nil, err
	}

	externalStore, err := blob.NewStore(logger, externalStoreUrl)
	if err != nil {
		return nil, err
	}

	s := &Store{
		url:           aerospikeURL,
		client:        client,
		namespace:     namespace,
		setName:       setName,
		expiration:    expiration,
		logger:        logger,
		externalStore: externalStore,
	}

	batchingEnabled := gocore.Config().GetBool("utxostore_batchingEnabled", true)

	if batchingEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_storeBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("utxostore_storeBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.storeBatcher = batcher.New[batchStoreItem](batchSize, duration, s.sendStoreBatch, true)
	}

	if batchingEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_getBatcherSize", 1024)
		batchDuration, _ := gocore.Config().GetInt("utxostore_getBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.getBatcher = batcher.New[batchGetItem](batchSize, duration, s.sendGetBatch, true)
	}

	// Make sure the spend and unSpend lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err := registerLuaIfNecessary(client, luaSpendFunction, spendLUA); err != nil {
		return nil, fmt.Errorf("Failed to register spendLUA: %w", err)
	}

	if err := registerLuaIfNecessary(client, luaUnSpendFunction, unSpendLUA); err != nil {
		return nil, fmt.Errorf("Failed to register unSpendLUA: %w", err)
	}

	if batchingEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("utxostore_spendBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.spendBatcher = batcher.New[batchSpend](batchSize, duration, s.sendSpendBatchLua, true)
	}

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)

	return s, nil
}

func registerLuaIfNecessary(client *uaerospike.Client, funcName string, funcBytes []byte) error {
	udfs, err := client.ListUDF(nil)
	if err != nil {
		return err
	}

	foundScript := false

	for _, udf := range udfs {
		if udf.Filename == funcName+".lua" {

			foundScript = true
			break
		}
	}

	if !foundScript {
		registerSpendLua, err := client.RegisterUDF(nil, funcBytes, funcName+".lua", aerospike.LUA)
		if err != nil {
			return err
		}

		err = <-registerSpendLua.OnComplete()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)
	return nil
}

func (s *Store) GetBlockHeight() (uint32, error) {
	return s.blockHeight.Load(), nil
}

func (s *Store) Health(ctx context.Context) (int, string, error) {
	/* As written by one of the Aerospike developers, Go contexts are not supported:

	The Aerospike Go Client is a high performance library that supports hundreds of thousands
	of transactions per second per instance. Context support would require us to spawn a new
	goroutine for every request, adding significant overhead to the scheduler and GC.

	I am convinced that most users would benchmark their code with the context support and
	decide against using it after noticing the incurred penalties.

	Therefore, we will extract the Deadline from the context and use it as a timeout for the
	operation.
	*/

	var timeout time.Duration

	deadline, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}

	writePolicy := aerospike.NewWritePolicy(0, 0)
	if timeout > 0 {
		writePolicy.TotalTimeout = timeout
	}

	details := fmt.Sprintf("url: %s, namespace: %s", s.url.String(), s.namespace)

	// Trying to put and get a record to test the connection
	key, err := aerospike.NewKey("test", "set", "key")
	if err != nil {
		return -1, details, err
	}

	bin := aerospike.NewBin("bin", "value")
	err = s.client.PutBins(writePolicy, key, bin)
	if err != nil {
		return -2, details, err
	}

	policy := aerospike.NewPolicy()
	if timeout > 0 {
		policy.TotalTimeout = timeout
	}

	_, err = s.client.Get(policy, key)
	if err != nil {
		return -3, details, err
	}

	return 0, details, nil
}

func calculateOffsetForOutput(vout uint32, utxoBatchSize uint32) uint32 {
	return vout % utxoBatchSize
}

func calculateKeySource(hash *chainhash.Hash, num uint32) []byte {
	// The key is normally the hash of the transaction
	keySource := hash.CloneBytes()
	if num == 0 {
		return keySource
	}

	// Convert the offset to int64 little ending
	batchOffsetLE := make([]byte, 4)
	binary.LittleEndian.PutUint32(batchOffsetLE, num)

	keySource = append(keySource, batchOffsetLE...)
	return keySource
}
