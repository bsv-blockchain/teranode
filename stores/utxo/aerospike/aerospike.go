// //go:build aerospike

package aerospike

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"golang.org/x/exp/slices"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

//go:embed spend.lua
var spendLUA []byte

var luaSpendFunction = "spend_v1"

type batchStoreItem struct {
	tx       *bt.Tx
	lockTime uint32
	done     chan error
}

type batchGetItemData struct {
	Data *meta.Data
	Err  error
}

type batchGetItem struct {
	hash   chainhash.Hash
	fields []string
	done   chan batchGetItemData
}

type batchSpend struct {
	spend *utxo.Spend
	done  chan error
}

type batchLastSpend struct {
	key  *aerospike.Key
	hash chainhash.Hash
	time int
}

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
	url              *url.URL
	client           *uaerospike.Client
	namespace        string
	setName          string
	expiration       uint32
	blockHeight      atomic.Uint32
	logger           ulogger.Logger
	batchId          atomic.Uint64
	storeBatcher     *batcher.Batcher2[batchStoreItem]
	getBatcher       *batcher.Batcher2[batchGetItem]
	spendBatcher     *batcher.Batcher2[batchSpend]
	lastSpendBatcher *batcher.Batcher2[batchLastSpend]
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

	s := &Store{
		url:        aerospikeURL,
		client:     client,
		namespace:  namespace,
		setName:    setName,
		expiration: expiration,
		logger:     logger,
	}

	storeBatcherEnabled := gocore.Config().GetBool("txmeta_store_storeBatcherEnabled", true)
	if storeBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("txmeta_store_storeBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("txmeta_store_storeBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.storeBatcher = batcher.New[batchStoreItem](batchSize, duration, s.sendStoreBatch, true)
	}

	getBatcherEnabled := gocore.Config().GetBool("txmeta_store_getBatcherEnabled", true)
	if getBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("txmeta_store_getBatcherSize", 1024)
		batchDuration, _ := gocore.Config().GetInt("txmeta_store_getBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.getBatcher = batcher.New[batchGetItem](batchSize, duration, s.sendGetBatch, true)
	}

	udfs, err := client.ListUDF(nil)
	if err != nil {
		return nil, err
	}
	// check whether the spend lua script is installed in the cluster
	foundScript := false
	for _, udf := range udfs {
		if udf.Filename == luaSpendFunction+".lua" {
			// we found the script, no need to register it again
			foundScript = true
			break
		}
	}

	if !foundScript {
		// update the version of the lua script when a new version is launched, do not re-use the old one
		registerSpendLua, err := client.RegisterUDF(nil, spendLUA, luaSpendFunction+".lua", aerospike.LUA)
		if err != nil {
			return nil, err
		}

		err = <-registerSpendLua.OnComplete()
		if err != nil {
			return nil, err
		}
	}

	spendBatcherEnabled := gocore.Config().GetBool("utxostore_spendBatcherEnabled", true)
	if spendBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("utxostore_spendBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.spendBatcher = batcher.New[batchSpend](batchSize, duration, s.sendSpendBatchLua, true)
	}

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)

	return s, nil
}

func (s *Store) sendStoreBatch(batch []*batchStoreItem) {
	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var hash *chainhash.Hash
	var key *aerospike.Key
	var binsToStore []*aerospike.Bin
	var err error

	for idx, bItem := range batch {
		hash = bItem.tx.TxIDChainHash()
		key, err = aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			bItem.done <- err
			continue
		}

		binsToStore, err = getBinsToStore(bItem.tx)
		if err != nil {
			bItem.done <- errors.New(errors.ERR_PROCESSING, "could not get bins to store", err)
			continue
		}

		putOps := make([]*aerospike.Operation, len(binsToStore))
		for i, bin := range binsToStore {
			putOps[i] = aerospike.PutOp(bin)
		}

		record := aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		batchRecords[idx] = record

	}

	batchId := s.batchId.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[STORE_BATCH][batch:%d] error in aerospike map store batch records: %v", batchId, err)
		for _, bItem := range batch {
			bItem.done <- err
		}
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_EXISTS_ERROR {
				s.logger.Warnf("[STORE_BATCH][%s:%d] txMeta already exists in batch %d, skipping", batch[idx].tx.TxIDChainHash().String(), idx, batchId)
				batch[idx].done <- utxo.NewErrTxmetaAlreadyExists(hash)
				continue
			}

			batch[idx].done <- errors.New(errors.ERR_STORAGE_ERROR, "[STORE_BATCH][%s:%d] error in aerospike store batch record for txMeta (will retry): %d - %w", batch[idx].tx.TxIDChainHash().String(), idx, batchId, err)
		} else {
			batch[idx].done <- nil
		}
	}
}

func (s *Store) sendGetBatch(batch []*batchGetItem) {
	items := make([]*utxo.UnresolvedMetaData, 0, len(batch))
	for idx, item := range batch {
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
	}

	retries := 0
	for {
		if err := s.BatchDecorate(context.Background(), items); err != nil {
			if retries < 3 {
				retries++
				s.logger.Errorf("failed to get batch of txmeta: %v", err)
				time.Sleep(time.Duration(retries) * time.Second)
				continue
			}

			// mark all items as errored
			for _, bItem := range batch {
				bItem.done <- batchGetItemData{
					Err: err,
				}
			}
			return
		}

		break
	}

	for _, item := range items {
		// send the data back to the original caller
		batch[item.Idx].done <- batchGetItemData{
			Data: item.Data,
			Err:  item.Err,
		}
	}
}

func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	batchId := s.batchId.Add(1)
	s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchId, len(batch))
	defer func() {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchId, len(batch))
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	var key *aerospike.Key
	var err error
	for idx, bItem := range batch {
		key, err = aerospike.NewKey(s.namespace, s.setName, bItem.spend.TxID.CloneBytes())
		if err != nil {
			// we just return the error on the channel, we cannot process this utxo any further
			bItem.done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend: %w", bItem.spend.Hash.String(), err)
			continue
		}

		batchUDFPolicy := aerospike.NewBatchUDFPolicy()
		batchRecords[idx] = aerospike.NewBatchUDF(batchUDFPolicy, key, luaSpendFunction, "spend",
			aerospike.NewValue(bItem.spend.Hash.String()),         // utxo hash
			aerospike.NewValue(bItem.spend.SpendingTxID.String()), // spending tx id
			aerospike.NewValue(s.blockHeight.Load()),              // current block height
			aerospike.NewValue(time.Now().Unix()),                 // current time
			aerospike.NewValue(s.expiration),                      // ttl
		)
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%d] failed to batch spend aerospike map utxos in batchId %d: %v", batchId, len(batch), err)
		for idx, bItem := range batch {
			bItem.done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.Hash.String(), batchId, idx, err)
		}
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		spend := batch[idx].spend
		err = batchRecord.BatchRec().Err
		if err != nil {
			batch[idx].done <- errors.New(errors.ERR_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, locktime %d: %d - %w", spend.Hash.String(), s.blockHeight.Load(), batchId, err)
		} else {
			response := batchRecord.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					responseMsgParts := strings.Split(responseMsg, ":")
					switch responseMsgParts[0] {
					case "OK":
						batch[idx].done <- nil
					case "SPENT":
						// spent by another transaction
						spendingTxID, hashErr := chainhash.NewHashFromStr(responseMsgParts[1])
						if hashErr != nil {
							batch[idx].done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] could not parse spending tx hash: %w", spend.Hash.String(), hashErr)
						}
						// TODO we need to be able to send the spending TX ID in the error down the line
						batch[idx].done <- utxo.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, spendingTxID)
					case "LOCKED":
						locktime, hashErr := strconv.ParseUint(responseMsgParts[1], 10, 32)
						if hashErr != nil {
							batch[idx].done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] could not parse locktime: %w", spend.Hash.String(), hashErr)
						}
						batch[idx].done <- utxo.NewErrLockTime(uint32(locktime), s.blockHeight.Load(), err)
					case "ERROR":
						batch[idx].done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, locktime %d: %d - %s", spend.Hash.String(), s.blockHeight.Load(), batchId, responseMsgParts[1])
					default:
						batch[idx].done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, locktime %d: %d - %s", spend.Hash.String(), s.blockHeight.Load(), batchId, responseMsg)
					}
				}
			} else {
				batch[idx].done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] could not parse response", spend.Hash.String())
			}
		}
	}
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

func (s *Store) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	prometheusUtxoMapGet.Inc()

	key, aErr := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()
		s.logger.Errorf("Failed to init new aerospike key: %v\n", aErr)
		return nil, aErr
	}

	policy := util.GetAerospikeReadPolicy()
	policy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	value, aErr := s.client.Get(policy, key, binNames...)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()
		if errors.Is(aErr, aerospike.ErrKeyNotFound) {
			return &utxo.SpendResponse{
				Status: int(utxostore.Status_NOT_FOUND),
			}, nil
		}
		s.logger.Errorf("Failed to get aerospike key: %v\n", aErr)
		return nil, aErr
	}

	var err error
	var spendingTxId *chainhash.Hash
	lockTime := uint32(0)
	if value != nil {
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		if ok {
			spendingTxIdStr, ok := utxoMap[spend.Hash.String()].(string)
			if ok && spendingTxIdStr != "" {
				spendingTxId, err = chainhash.NewHashFromStr(spendingTxIdStr)
				if err != nil {
					return nil, errors.New(errors.ERR_PROCESSING, "chain hash error", err)
				}
			}
		}

		iVal := value.Bins["locktime"]
		if iVal != nil {
			lockTimeInt, ok := iVal.(int)
			if ok {
				lockTime = uint32(lockTimeInt)
			}
		}
	}

	return &utxo.SpendResponse{
		Status:       int(utxostore.CalculateUtxoStatus(spendingTxId, lockTime, s.blockHeight.Load())),
		SpendingTxID: spendingTxId,
		LockTime:     lockTime,
	}, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.get(ctx, hash, utxo.MetaFields)
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields[0]
	}
	return s.get(ctx, hash, bins)
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*meta.Data, error) {

	bins = s.addAbstractedBins(bins)

	done := make(chan batchGetItemData)
	item := &batchGetItem{hash: *hash, fields: bins, done: done}

	if s.getBatcher != nil {
		s.getBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendGetBatch([]*batchGetItem{item})
		}()
	}

	data := <-done
	if data.Err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", data.Err.Error()).Inc()
	} else {
		prometheusTxMetaAerospikeMapGet.Inc()
	}
	return data.Data, data.Err
}

func (s *Store) getTxFromBins(bins aerospike.BinMap) (*bt.Tx, error) {
	tx := &bt.Tx{
		Version:  uint32(bins["version"].(int)),
		LockTime: uint32(bins["locktime"].(int)),
	}
	inputInterfaces, ok := bins["inputs"].([]interface{})
	if ok {
		tx.Inputs = make([]*bt.Input, len(inputInterfaces))
		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			tx.Inputs[i] = &bt.Input{}
			_, err := tx.Inputs[i].ReadFromExtended(bytes.NewReader(input))
			if err != nil {
				return nil, errors.New(errors.ERR_TX_INVALID, "could not read input", err)
			}
		}
	}

	outputInterfaces, ok := bins["outputs"].([]interface{})
	if ok {
		tx.Outputs = make([]*bt.Output, len(outputInterfaces))
		for i, outputInterface := range outputInterfaces {
			output := outputInterface.([]byte)
			tx.Outputs[i] = &bt.Output{}
			_, err := tx.Outputs[i].ReadFrom(bytes.NewReader(output))
			if err != nil {
				return nil, errors.New(errors.ERR_TX_INVALID, "could not read output", err)
			}
		}
	}

	return tx, nil
}

func (s *Store) addAbstractedBins(bins []string) []string {
	// add missing bins
	if slices.Contains(bins, "parentTxHashes") {
		if !slices.Contains(bins, "inputs") {
			bins = append(bins, "inputs")
		}
	}
	if slices.Contains(bins, "tx") {
		if !slices.Contains(bins, "inputs") {
			bins = append(bins, "inputs")
		}
		if !slices.Contains(bins, "outputs") {
			bins = append(bins, "outputs")
		}
		if !slices.Contains(bins, "version") {
			bins = append(bins, "version")
		}
		if !slices.Contains(bins, "locktime") {
			bins = append(bins, "locktime")
		}
	}
	return bins
}

func (s *Store) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, fields ...string) error {
	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "failed to init new aerospike key for txMeta", err)
		}

		bins := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}
		if len(item.Fields) > 0 {
			bins = item.Fields
		} else if len(fields) > 0 {
			bins = fields
		}

		item.Fields = s.addAbstractedBins(bins)

		record := aerospike.NewBatchRead(policy, key, item.Fields)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike map store batch records", err)
	}

	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			items[idx].Data = nil
			if !model.CoinbasePlaceholderHash.Equal(items[idx].Hash) {
				if errors.Is(err, aerospike.ErrKeyNotFound) {
					items[idx].Err = utxo.NewErrTxmetaNotFound(&items[idx].Hash)
				} else {
					items[idx].Err = err
				}
			}
		} else {
			bins := batchRecord.BatchRec().Record.Bins

			items[idx].Data = &meta.Data{}

			for _, key := range items[idx].Fields {
				value := bins[key]
				switch key {
				case "tx":
					tx, txErr := s.getTxFromBins(bins)
					if txErr != nil {
						return errors.New(errors.ERR_TX_INVALID, "invalid tx", txErr)
					}
					items[idx].Data.Tx = tx
				case "fee":
					fee, ok := value.(int)
					if ok {
						items[idx].Data.Fee = uint64(fee)
					}
				case "sizeInBytes":
					sizeInBytes, ok := value.(int)
					if ok {
						items[idx].Data.SizeInBytes = uint64(sizeInBytes)
					}
				case "parentTxHashes":
					inputInterfaces, ok := bins["inputs"].([]interface{})
					if ok {
						items[idx].Data.ParentTxHashes = make([]chainhash.Hash, len(inputInterfaces))
						for i, inputInterface := range inputInterfaces {
							input := inputInterface.([]byte)
							items[idx].Data.ParentTxHashes[i] = chainhash.Hash(input[:32])
						}
					}
				case "blockIDs":
					temp := value.([]interface{})
					var blockIDs []uint32
					for _, val := range temp {
						blockIDs = append(blockIDs, uint32(val.(int)))
					}
					items[idx].Data.BlockIDs = blockIDs
				case "isCoinbase":
					coinbaseBool, ok := value.(bool)
					if ok {
						items[idx].Data.IsCoinbase = coinbaseBool
					}
				}
			}
		}
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(outpoints))

	for idx, item := range outpoints {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.PreviousTxID[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "failed to init new aerospike key for txMeta", err)
		}

		bins := []string{"version", "locktime", "inputs", "outputs"}
		record := aerospike.NewBatchRead(policy, key, bins)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike map store batch records", err)
	}

	for idx, batchRecordIfc := range batchRecords {
		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			return errors.New(errors.ERR_PROCESSING, "error in aerospike map store batch record", batchRecord.Err)
		}

		bins := batchRecord.Record.Bins
		previousTx, err := s.getTxFromBins(bins)
		if err != nil {
			return errors.New(errors.ERR_TX_INVALID, "invalid tx", err)
		}

		outpoints[idx].Satoshis = previousTx.Outputs[outpoints[idx].Vout].Satoshis
		outpoints[idx].LockingScript = *previousTx.Outputs[outpoints[idx].Vout].LockingScript
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockIDs ...uint32) (*meta.Data, error) {
	startTotal, stat, _ := util.StartStatFromContext(ctx, "Create")

	defer func() {
		stat.AddTime(startTotal)
	}()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get tx meta data", err)
	}

	done := make(chan error)
	item := &batchStoreItem{tx: tx, lockTime: tx.LockTime, done: done}

	if s.storeBatcher != nil {
		s.storeBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendStoreBatch([]*batchStoreItem{item})
		}()
	}

	err = <-done
	if err != nil {
		// return raw err, should already be wrapped
		return nil, err
	}

	prometheusTxMetaAerospikeMapStore.Inc()
	return txMeta, nil
}

func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend) (err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	spentSpends := make([]*utxo.Spend, 0, len(spends))

	if s.spendBatcher != nil {
		g := errgroup.Group{}
		for _, spend := range spends {
			if spend == nil {
				continue
			}

			spend := spend
			g.Go(func() error {
				done := make(chan error)
				s.spendBatcher.Put(&batchSpend{
					spend: spend,
					done:  done,
				})

				// this waits for the batch to be sent and the response to be received from the batch operation
				batchErr := <-done
				if batchErr != nil {
					// just return the raw error, should already be wrapped
					return batchErr
				}

				spentSpends = append(spentSpends, spend)

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			// revert the successfully spent utxos
			unspendErr := s.UnSpend(ctx, spentSpends)
			if unspendErr != nil {
				err = errors.Join(err, unspendErr)
			}
			return errors.New(errors.ERR_ERROR, "error in aerospike spend record", err)
		}

		prometheusUtxoMapSpend.Add(float64(len(spends)))
	}

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New(errors.ERR_PROCESSING, "timeout spending %d of %d utxos", i, len(spends))
			}
			return errors.New(errors.ERR_PROCESSING, "context cancelled spending %d of %d utxos", i, len(spends))

		default:
			if spend == nil {
				continue
			}

			err = s.spendUtxo(policy, spend)
			if err != nil {
				if errors.Is(err, utxo.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, spend.SpendingTxID)) {
					return err
				}

				// another error encountered, reverse all spends and return error
				if resetErr := s.UnSpend(context.Background(), spends); resetErr != nil {
					s.logger.Errorf("ERROR in aerospike reset: %v\n", resetErr)
				}

				return errors.New(errors.ERR_ERROR, "error in aerospike spend record", err)
			}
		}
	}

	return nil
}

func (s *Store) spendUtxo(policy *aerospike.WritePolicy, spend *utxo.Spend) error {
	key, err := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Spend", err.Error()).Inc()
		return errors.New(errors.ERR_PROCESSING, "error failed creating key in aerospike Spend", err)
	}

	policy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpOr(
			// anything below the block height is spendable, including 0
			aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(int64(s.blockHeight.Load()))),

			aerospike.ExpAnd(
				aerospike.ExpGreaterEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(500000000)),
				// TODO Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median
				// time past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined),
				// and not the block time itself.
				aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(time.Now().Unix())),
			),
		),

		// spent check - value of utxo hash in map should be nil
		aerospike.ExpEq(aerospike.ExpMapGetByKey(
			aerospike.MapReturnType.VALUE,
			aerospike.ExpTypeSTRING,
			aerospike.ExpStringVal(spend.Hash.String()),
			aerospike.ExpMapBin("utxos"),
		), aerospike.ExpStringVal("")),
	)

	response, err := s.client.Operate(policy, key, []*aerospike.Operation{
		aerospike.MapPutOp(
			aerospike.DefaultMapPolicy(),
			"utxos",
			spend.Hash.String(),
			spend.SpendingTxID.String(),
		),
		aerospike.GetBinOp("utxos"),
	}...)
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return errors.New(errors.ERR_NOT_FOUND, "utxo not found", err)
		}

		if errors.Is(err, aerospike.ErrFilteredOut) {
			prometheusUtxoMapGet.Inc()
			errPolicy := util.GetAerospikeReadPolicy()
			errPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

			value, getErr := s.client.Get(errPolicy, key, "utxos", "locktime")
			if getErr != nil {
				return errors.New(errors.ERR_PROCESSING, "could not see if the value was the same as before", getErr)
			}

			locktime, ok := value.Bins["locktime"].(int)
			if ok {
				status := utxostore.CalculateUtxoStatus(nil, uint32(locktime), s.blockHeight.Load())
				if status == utxostore.Status_LOCKED {
					return utxo.NewErrLockTime(uint32(locktime), s.blockHeight.Load(), err)
				}
			}

			// check whether we had the same value set as before
			utxosValue, ok := value.Bins["utxos"].(map[interface{}]interface{})
			if ok {
				// get utxo from map
				valueStr, ok := utxosValue[spend.Hash.String()].(string)
				if ok {
					valueHash, err := chainhash.NewHashFromStr(valueStr)
					if err != nil {
						return errors.New(errors.ERR_PROCESSING, "could not parse value hash", err)
					}
					if spend.SpendingTxID.Equal(*valueHash) {
						prometheusUtxoMapReSpend.Inc()
						return nil
					} else {
						prometheusUtxoMapSpendSpent.Inc()
						spendingTxHash, err := chainhash.NewHashFromStr(valueStr)
						if err != nil {
							return errors.New(errors.ERR_PROCESSING, "could not parse value hash", err)
						}

						s.logger.Debugf("utxo %s was spent by %s", spend.TxID.String(), spendingTxHash)
						// TODO replace this error with the new one
						return utxo.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, spendingTxHash)
					}
				}
			}
		}

		prometheusUtxoMapErrors.WithLabelValues("Spend", err.Error()).Inc()
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike spend PutBins", err)
	}

	prometheusUtxoMapSpend.Inc()

	// check whether all utxos are spent
	utxosValue, ok := response.Bins["utxos"].([]interface{})
	if ok {
		if len(utxosValue) == 2 {
			// utxos are in index 1 of the response
			utxos, ok := utxosValue[1].(map[interface{}]interface{})
			if ok {
				spentUtxos := 0
				for _, v := range utxos {
					if v != "" {
						spentUtxos++
					}
				}
				if spentUtxos == len(utxos) {
					// mark document as spent and add expiration for TTL
					if s.lastSpendBatcher != nil {
						s.lastSpendBatcher.Put(&batchLastSpend{
							key:  key,
							hash: *spend.Hash,
							time: int(time.Now().Unix()),
						})
					} else {
						ttlPolicy := util.GetAerospikeWritePolicy(0, s.expiration)
						ttlPolicy.RecordExistsAction = aerospike.UPDATE_ONLY
						_, err = s.client.Operate(ttlPolicy, key, aerospike.PutOp(
							aerospike.NewBin(
								"lastSpend",
								aerospike.NewIntegerValue(int(time.Now().Unix())),
							),
						))
						if err != nil {
							return errors.New(errors.ERR_STORAGE_ERROR, "could not set lastSpend", err)
						}
					}
				}
			}
		}
	}

	return nil
}

func (s *Store) UnSpend(ctx context.Context, spends []*utxo.Spend) (err error) {
	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New(errors.ERR_STORAGE_ERROR, "timeout un-spending %d of %d utxos", i, len(spends))
			}
			return errors.New(errors.ERR_STORAGE_ERROR, "context cancelled un-spending %d of %d utxos", i, len(spends))
		default:
			s.logger.Warnf("unspending utxo %s of tx %s:%d, spending tx: %s", spend.Hash.String(), spend.TxID.String(), spend.Vout, spend.SpendingTxID.String())
			if err = s.unSpend(ctx, spend); err != nil {
				// just return the raw error, should already be wrapped
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpend(_ context.Context, spend *utxo.Spend) error {
	policy := util.GetAerospikeWritePolicy(3, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.New(errors.ERR_PROCESSING, "error in aerospike NewKey", err)
	}

	_, err = s.client.Operate(policy, key, aerospike.MapPutOp(
		aerospike.DefaultMapPolicy(),
		"utxos",
		spend.Hash.String(),
		aerospike.NewStringValue(""),
	))
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike unspend record", err)
	}

	prometheusUtxoMapReset.Inc()

	return nil
}

func (s *Store) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	// math.MaxUint32 - 1 does not update expiration of the record
	policy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32-1)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "aerospike NewKey error", err)
		}
		op := aerospike.ListAppendOp("blockIDs", blockID)
		record := aerospike.NewBatchWrite(policy, key, op)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "aerospike BatchOperate error", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	okUpdates := 0
	var errs error
	nrErrors := 0
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
				// the tx Meta does not exist anymore, so we do not have to set the mined status
				continue
			}
			errs = errors.Join(errs, errors.New(errors.ERR_STORAGE_ERROR, "aerospike batchRecord error: %s", hashes[idx].String(), err))
			nrErrors++
		} else {
			okUpdates++
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if errs != nil || nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return errors.New(errors.ERR_ERROR, "aerospike batchRecord errors", errs)
	}

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "aerospike NewKey error", err)
	}

	_, err = s.client.Operate(policy, key, aerospike.ListAppendOp("blockIDs", blockID))
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "aerospike Operate error", err)
	}

	prometheusTxMetaAerospikeMapSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(0, 0)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error in aerospike NewKey", err)
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}

func getBinsToStore(tx *bt.Tx, blockIDs ...uint32) ([]*aerospike.Bin, error) {
	fee, utxoHashes, err := utxo.GetFeesAndUtxoHashes(context.Background(), tx)
	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get fees and utxo hashes", err)
	}

	utxos := make(map[interface{}]interface{})
	for _, utxoHash := range utxoHashes {
		utxos[utxoHash.String()] = aerospike.NewStringValue("")
	}

	// create a tx interface[] map
	inputs := make([]interface{}, len(tx.Inputs))
	for i, input := range tx.Inputs {
		h := input.Bytes(false)

		// this is needed for extended txs, go-bt does not do this itself
		h = append(h, []byte{
			byte(input.PreviousTxSatoshis),
			byte(input.PreviousTxSatoshis >> 8),
			byte(input.PreviousTxSatoshis >> 16),
			byte(input.PreviousTxSatoshis >> 24),
			byte(input.PreviousTxSatoshis >> 32),
			byte(input.PreviousTxSatoshis >> 40),
			byte(input.PreviousTxSatoshis >> 48),
			byte(input.PreviousTxSatoshis >> 56),
		}...)

		if input.PreviousTxScript == nil {
			h = append(h, bt.VarInt(0).Bytes()...)
		} else {
			l := uint64(len(*input.PreviousTxScript))
			h = append(h, bt.VarInt(l).Bytes()...)
			h = append(h, *input.PreviousTxScript...)
		}

		inputs[i] = h
	}

	outputs := make([]interface{}, len(tx.Outputs))
	for i, output := range tx.Outputs {
		outputs[i] = output.Bytes()
	}

	bins := []*aerospike.Bin{
		aerospike.NewBin("inputs", inputs),
		aerospike.NewBin("outputs", outputs),
		aerospike.NewBin("version", aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(tx.LockTime))),
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(fee))),
		aerospike.NewBin("sizeInBytes", aerospike.NewIntegerValue(tx.Size())),
		aerospike.NewBin("utxos", aerospike.NewMapValue(utxos)),
		aerospike.NewBin("nrUtxos", aerospike.NewIntegerValue(len(utxos))),
		aerospike.NewBin("spentUtxos", aerospike.NewIntegerValue(0)),
		aerospike.NewBin("blockIDs", blockIDs),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	return bins, nil
}
