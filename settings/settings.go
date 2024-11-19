package settings

import (
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/util"
)

func NewSettings() *Settings {
	params, err := chaincfg.GetChainParams(getString("network", "mainnet"))
	if err != nil {
		panic(err)
	}

	blockMaxSize, err := util.ParseMemoryUnit(getString("blockmaxsize", "0")) // default to 0 - unlimited
	if err != nil {
		panic(err)
	}

	subtreeTTLMinutes := getInt("blockassembly_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	return &Settings{
		ClientName:     getString("clientName", "defaultClientName"),
		DataFolder:     getString("dataFolder", "data"),
		ChainCfgParams: params,
		Policy: &PolicySettings{
			ExcessiveBlockSize: getInt("excessiveblocksize", 4294967296), // 4GB
			// TODO: change BlockMaxSize to uint64
			//nolint:gosec // G115: integer overflow conversion uint64 -> int (gosec)
			BlockMaxSize:    int(blockMaxSize),
			MaxTxSizePolicy: getInt("maxtxsizepolicy", 10485760), // 10MB
			MinMiningTxFee:  getFloat64("minminingtxfee", 0.00000500),
			// MaxOrphanTxSize:                 getInt("maxorphantxsize", 1000000),
			// DataCarrierSize:                 int64(getInt("datacarriersize", 1000000)),
			MaxScriptSizePolicy: getInt("maxscriptsizepolicy", 500000), // 500KB
			// TODO: what should this be?
			//MaxOpsPerScriptPolicy:           int64(getInt("maxopsperscriptpolicy", 1000000)),
			MaxScriptNumLengthPolicy:     getInt("maxscriptnumlengthpolicy", 10000),       // 10K
			MaxPubKeysPerMultisigPolicy:  int64(getInt("maxpubkeyspermultisigpolicy", 0)), // 0 is unlimited
			MaxTxSigopsCountsPolicy:      int64(getInt("maxtxsigopscountspolicy", 0)),     // 0 is unlimited
			MaxStackMemoryUsagePolicy:    getInt("maxstackmemoryusagepolicy", 104857600),  // 100MB
			MaxStackMemoryUsageConsensus: getInt("maxstackmemoryusageconsensus", 0),       // 0 is unlimited
			// LimitAncestorCount:              getInt("limitancestorcount", 1000000),
			// LimitCPFPGroupMembersCount:      getInt("limitcpfpgroupmemberscount", 1000000),
			AcceptNonStdOutputs: getBool("acceptnonstdoutputs", true),
			// DataCarrier:                     getBool("datacarrier", false),
			// MaxStdTxValidationDuration:    getInt("maxstdtxvalidationduration", 3),       // 3ms
			// MaxNonStdTxValidationDuration: getInt("maxnonstdtxvalidationduration", 1000), // 1000ms
			// MaxTxChainValidationBudget:    getInt("maxtxchainvalidationbudget", 50),      // 50ms
			// ValidationClockCPU:              getBool("validationclockcpu", false),
			// MinConsolidationFactor:          getInt("minconsolidationfactor", 20),
			// MaxConsolidationInputScriptSize: getInt("maxconsolidationinputscriptsize", 1000000),
			// MinConfConsolidationInput:       getInt("minconfconsolidationinput", 1000000),
			// MinConsolidationInputMaturity:   getInt("minconsolidationinputmaturity", 1000000),
			// AcceptNonStdConsolidationInput:  getBool("acceptnonstdconsolidationinput", false),
		},
		Kafka: KafkaSettings{
			Blocks:            getString("KAFKA_BLOCKS", "blocks"),
			BlocksFinal:       getString("KAFKA_BLOCKS_FINAL", "blocks-final"),
			BlocksValidate:    getString("KAFKA_BLOCKS_VALIDATE", "blocks-validate"),
			Hosts:             getString("KAFKA_HOSTS", "localhost:9092"),
			LegacyInv:         getString("KAFKA_LEGACY_INV", "legacy-inv"),
			Partitions:        getInt("KAFKA_PARTITIONS", 1),
			Port:              getInt("KAFKA_PORT", 9092),
			RejectedTx:        getString("KAFKA_REJECTEDTX", "rejectedtx"),
			ReplicationFactor: getInt("KAFKA_REPLICATION_FACTOR", 1),
			Subtrees:          getString("KAFKA_SUBTREES", "subtrees"),
			TxMeta:            getString("KAFKA_TXMETA", "txmeta"),
			UnitTest:          getString("KAFKA_UNITTEST", "unittest"),
			ValidatorTxs:      getString("KAFKA_VALIDATORTXS", "validatortxs"),
		},
		Aerospike: AerospikeSettings{
			Debug:                  getBool("aerospike_debug", false),
			Host:                   getString("aerospike_host", "localhost"),
			BatchPolicy:            getString("aerospike_batchPolicy", "defaultBatchPolicy"),
			ReadPolicy:             getString("aerospike_readPolicy", "defaultReadPolicy"),
			WritePolicy:            getString("aerospike_writePolicy", "defaultWritePolicy"),
			Port:                   getInt("aerospike_port", 3000),
			UseDefaultBasePolicies: getBool("aerospike_useDefaultBasePolicies", false),
			UseDefaultPolicies:     getBool("aerospike_useDefaultPolicies", false),
			WarmUp:                 getBool("aerospike_warmUp", true),
		},
		Alert: AlertSettings{
			GenesisKeys:   getMultiString("alert_genesis_keys", ""),
			P2PPrivateKey: getString("alert_p2p_private_key", ""),
			ProtocolID:    getString("alert_protocol_id", "/bitcoin/alert-system/1.0.0"),
			Store:         getString("alert_store", "sqlite:///alert"),
			StoreURL:      getURL("alert_store", "sqlite:///alert"),
			TopicName:     getString("alert_topic_name", "bitcoin_alert_system"),
			P2PPort:       getInt("ALERT_P2P_PORT", 9908),
		},
		Asset: AssetSettings{
			APIPrefix:               getString("asset_apiPrefix", "/api/v1"),
			CentrifugeListenAddress: getString("asset_centrifugeListenAddress", ":8892"),
			CentrifugeDisable:       getBool("asset_centrifuge_disable", false),
			HTTPAddress:             getString("asset_httpAddress", "http://localhost:8090/api/v1"),
			HTTPListenAddress:       getString("asset_httpListenAddress", ":8090"),
			HTTPPort:                getInt("ASSET_HTTP_PORT", 8090),
			HTTPSPort:               getInt("asset_https_port", 443),
		},
		Block: BlockSettings{
			MinedCacheMaxMB:                       getInt("blockMinedCacheMaxMB", 256),
			PersisterStore:                        getString("blockPersisterStore", "file://./data/blockstore"),
			PersisterHTTPListenAddress:            getString("blockPersister_httpListenAddress", ":8083"),
			PersisterWorkingDir:                   getString("blockPersister_workingDir", "./data/blockstore"),
			CheckDuplicateTransactionsConcurrency: getInt("block_checkDuplicateTransactionsConcurrency", 32),
			GetAndValidateSubtreesConcurrency:     getInt("block_getAndValidateSubtreesConcurrency", 32),
			KafkaWorkers:                          getInt("block_kafkaWorkers", 0),
			QuorumPath:                            getString("block_quorum_path", "./data/blockstore_quorum"),
			ValidOrderAndBlessedConcurrency:       getInt("block_validOrderAndBlessedConcurrency", 32),
			StoreCacheEnabled:                     getBool("blockchain_store_cache_enabled", true),
			MaxSize:                               getInt("blockmaxsize", 4294967296),
			StorePath:                             getString("blockstore", "file://./data/blockstore"),
			FailFastValidation:                    getBool("blockvalidation_fail_fast_validation", true),
			FinalizeBlockValidationConcurrency:    getInt("blockvalidation_finalizeBlockValidationConcurrency", 8),
			GetMissingTransactions:                getInt("blockvalidation_getMissingTransactions", 32),
		},
		BlockAssembly: BlockAssemblySettings{
			Disabled:                            getBool("blockassembly_disabled", false),
			GRPCAddress:                         getString("blockassembly_grpcAddress", "localhost:8085"),
			GRPCListenAddress:                   getString("blockassembly_grpcListenAddress", ":8085"),
			GRPCMaxRetries:                      getInt("blockassembly_grpcMaxRetries", 3),
			GRPCRetryBackoff:                    getString("blockassembly_grpcRetryBackoff", "2s"),
			LocalTTLCache:                       getString("blockassembly_localTTLCache", ""),
			MaxBlockReorgCatchup:                getInt("blockassembly_maxBlockReorgCatchup", 100),
			MaxBlockReorgRollback:               getInt("blockassembly_maxBlockReorgRollback", 100),
			MoveDownBlockConcurrency:            getInt("blockassembly_moveDownBlockConcurrency", 375),
			ProcessRemainderTxHashesConcurrency: getInt("blockassembly_processRemainderTxHashesConcurrency", 375),
			SendBatchSize:                       getInt("blockassembly_sendBatchSize", 100),
			SendBatchTimeout:                    getInt("blockassembly_sendBatchTimeout", 2),
			SubtreeProcessorBatcherSize:         getInt("blockassembly_subtreeProcessorBatcherSize", 1000),
			SubtreeProcessorConcurrentReads:     getInt("blockassembly_subtreeProcessorConcurrentReads", 375),
			SubtreeTTL:                          subtreeTTL,
			NewSubtreeChanBuffer:                getInt("blockassembly_newSubtreeChanBuffer", 1_000),
			SubtreeRetryChanBuffer:              getInt("blockassembly_subtreeRetryChanBuffer", 1_000),
			SubmitMiningSolutionWaitForResponse: getBool("blockassembly_SubmitMiningSolution_waitForResponse", true),
		},
		BlockChain: BlockChainSettings{
			GRPCAddress:       getString("blockchain_grpcAddress", "localhost:8087"),
			GRPCListenAddress: getString("blockchain_grpcListenAddress", ":8087"),
			HTTPListenAddress: getString("blockchain_httpListenAddress", ":8082"),
			MaxRetries:        getInt("blockchain_maxRetries", 3),
			StoreURL:          getURL("blockchain_store", "sqlite:///blockchain"),
			FSMStateRestore:   getBool("fsm_state_restore", false),
		},
		BlockValidation: BlockValidationSettings{
			MaxRetries:                     getInt("blockValidationMaxRetries", 3),
			RetrySleep:                     getString("blockValidationRetrySleep", "1s"),
			GRPCAddress:                    getString("blockvalidation_grpcAddress", "localhost:8088"),
			GRPCListenAddress:              getString("blockvalidation_grpcListenAddress", ":8088"),
			HTTPAddress:                    getString("blockvalidation_httpAddress", "http://localhost:8188"),
			HTTPListenAddress:              getString("blockvalidation_httpListenAddress", ":8188"),
			KafkaWorkers:                   getInt("blockvalidation_kafkaWorkers", 0),
			LocalSetTxMinedConcurrency:     getInt("blockvalidation_localSetTxMinedConcurrency", 8),
			MaxPreviousBlockHeadersToCheck: getInt("blockvalidation_maxPreviousBlockHeadersToCheck", 100),
		},
		Validator: ValidatorSettings{
			GrpcAddress:               getString("validator_grpcAddress", "localhost:8081"),
			GrpcListenAddress:         getString("validator_grpcListenAddress", ":8081"),
			KafkaWorkers:              getInt("validator_kafkaWorkers", 0),
			ScriptVerificationLibrary: getString("validator_scriptVerificationLibrary", "libscript.so"),
			SendBatchSize:             getInt("validator_sendBatchSize", 100),
			SendBatchTimeout:          getInt("validator_sendBatchTimeout", 2),
			SendBatchWorkers:          getInt("validator_sendBatchWorkers", 10),
			GrpcPort:                  getInt("VALIDATOR_GRPC_PORT", 8081),
			BlockValidationDelay:      getInt("validator_blockvalidation_delay", 0),
			BlockValidationMaxRetries: getInt("validator_blockvalidation_maxRetries", 5),
			BlockValidationRetrySleep: getString("validator_blockvalidation_retrySleep", "2s"),
		},
		Redis: RedisSettings{
			Hosts: getString("REDIS_HOSTS", "localhost:6379"),
			Port:  getInt("REDIS_PORT", 6379),
		},
		Region: RegionSettings{
			Name: getString("regionName", "defaultRegionName"),
		},
		Advertising: AdvertisingSettings{
			Interval: getString("advertisingInterval", "10s"),
			URL:      getString("advertisingURL", "defaultAdvertisingURL"),
		},
		UtxoStore: UtxoStoreSettings{
			OutpointBatcherSize:        getInt("utxostore_outpointBatcherSize", 100),
			SpendBatcherConcurrency:    getInt("utxostore_spendBatcherConcurrency", 32),
			SpendBatcherDurationMillis: getInt("utxostore_spendBatcherDurationMillis", 100),
			SpendBatcherSize:           getInt("utxostore_spendBatcherSize", 100),
			StoreBatcherConcurrency:    getInt("utxostore_storeBatcherConcurrency", 32),
			StoreBatcherDurationMillis: getInt("utxostore_storeBatcherDurationMillis", 100),
			StoreBatcherSize:           getInt("utxostore_storeBatcherSize", 100),
			UtxoBatchSize:              getInt("utxostore_utxoBatchSize", 0),
		},
	}
}
