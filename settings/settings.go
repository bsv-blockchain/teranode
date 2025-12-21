package settings

import (
	"runtime"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/ordishs/gocore"
)

func NewSettings(alternativeContext ...string) *Settings {
	settingsContext := gocore.Config().GetContext()
	if len(alternativeContext) > 0 {
		settingsContext = alternativeContext[0]
	}

	params, err := chaincfg.GetChainParams(getString("network", "mainnet", alternativeContext...))
	if err != nil {
		panic(err)
	}

	blockMaxSize, err := ParseMemoryUnit(getString("blockmaxsize", "0", alternativeContext...)) // default to 0 - unlimited
	if err != nil {
		panic(err)
	}

	const blocksInADayOnAverage = 144

	globalBlockHeightRetention := getUint32("global_blockHeightRetention", blocksInADayOnAverage*2, alternativeContext...)

	doubleSpendWindowMillis := getInt("double_spend_window_millis", 0, alternativeContext...)
	doubleSpendWindow := time.Duration(doubleSpendWindowMillis) * time.Millisecond

	blacklistMap := getMultiStringMap("subtreevalidation_blacklisted_baseurls", "|", []string{}, alternativeContext...)

	return &Settings{
		Commit:                       gocore.GetCommit(),
		Version:                      gocore.GetVersion(),
		Context:                      settingsContext,
		ServiceName:                  getString("SERVICE_NAME", "teranode", alternativeContext...),
		TracingEnabled:               getBool("tracing_enabled", false, alternativeContext...),
		TracingSampleRate:            getFloat64("tracing_SampleRate", 0.01, alternativeContext...),
		TracingCollectorURL:          getURL("tracing_collector_url", "http://localhost:4318", alternativeContext...),
		ClientName:                   getString("clientName", "defaultClientName", alternativeContext...),
		DataFolder:                   getString("dataFolder", "data", alternativeContext...),
		SecurityLevelHTTP:            getInt("securityLevelHTTP", 0, alternativeContext...),
		ServerCertFile:               getString("server_certFile", "", alternativeContext...),
		ServerKeyFile:                getString("server_keyFile", "", alternativeContext...),
		Logger:                       getString("logger", "", alternativeContext...),
		LogLevel:                     getString("logLevel", "INFO", alternativeContext...),
		PrettyLogs:                   getBool("prettyLogs", true, alternativeContext...),
		JSONLogging:                  getBool("jsonLogging", false, alternativeContext...),
		ProfilerAddr:                 getString("profilerAddr", "", alternativeContext...),
		StatsPrefix:                  getString("stats_prefix", "gocore", alternativeContext...),
		PrometheusEndpoint:           getString("prometheusEndpoint", "", alternativeContext...),
		HealthCheckHTTPListenAddress: getString("health_check_httpListenAddress", ":8000", alternativeContext...),
		UseDatadogProfiler:           getBool("use_datadog_profiler", false, alternativeContext...),
		LocalTestStartFromState:      getString("local_test_start_from_state", "", alternativeContext...),
		PostgresCheckAddress:         getString("postgres_check_address", "localhost:5432", alternativeContext...),
		UseCgoVerifier:               getBool("use_cgo_verifier", true, alternativeContext...),
		GRPCResolver:                 getString("grpc_resolver", "", alternativeContext...),
		GRPCMaxRetries:               getInt("grpc_max_retries", 40, alternativeContext...),
		GRPCRetryBackoff:             getDuration("grpc_retry_backoff", 250*time.Millisecond, alternativeContext...),
		SecurityLevelGRPC:            getInt("security_level_grpc", 0, alternativeContext...),
		UsePrometheusGRPCMetrics:     getBool("use_prometheus_grpc_metrics", true, alternativeContext...),
		GRPCAdminAPIKey:              getString("grpc_admin_api_key", "", alternativeContext...),
		GlobalBlockHeightRetention:   globalBlockHeightRetention,

		ChainCfgParams: params,
		Policy: &PolicySettings{
			ExcessiveBlockSize: getInt("excessiveblocksize", 4294967296, alternativeContext...), // 4GB
			// TODO: change BlockMaxSize to uint64
			//nolint:gosec // G115: integer overflow conversion uint64 -> int (gosec)
			BlockMaxSize:    int(blockMaxSize),
			MaxTxSizePolicy: getInt("maxtxsizepolicy", 10485760, alternativeContext...), // 10MB
			MinMiningTxFee:  getFloat64("minminingtxfee", 0.00000500, alternativeContext...),
			// MaxOrphanTxSize:                 getInt("maxorphantxsize", 1000000, alternativeContext...),
			// DataCarrierSize:                 int64(getInt("datacarriersize", 1000000, alternativeContext...)),
			MaxScriptSizePolicy: getInt("maxscriptsizepolicy", 500000, alternativeContext...), // 500KB
			// TODO: what should this be?
			// MaxOpsPerScriptPolicy:           int64(getInt("maxopsperscriptpolicy", 1000000, alternativeContext...)),
			MaxScriptNumLengthPolicy:     getInt("maxscriptnumlengthpolicy", 10000, alternativeContext...),       // 10K
			MaxPubKeysPerMultisigPolicy:  int64(getInt("maxpubkeyspermultisigpolicy", 0, alternativeContext...)), // 0 is unlimited
			MaxTxSigopsCountsPolicy:      int64(getInt("maxtxsigopscountspolicy", 0, alternativeContext...)),     // 0 is unlimited
			MaxStackMemoryUsagePolicy:    getInt("maxstackmemoryusagepolicy", 104857600, alternativeContext...),  // 100MB
			MaxStackMemoryUsageConsensus: getInt("maxstackmemoryusageconsensus", 0, alternativeContext...),       // 0 is unlimited
			// LimitAncestorCount:              getInt("limitancestorcount", 1000000, alternativeContext...),
			// LimitCPFPGroupMembersCount:      getInt("limitcpfpgroupmemberscount", 1000000, alternativeContext...),
			AcceptNonStdOutputs: getBool("acceptnonstdoutputs", true, alternativeContext...),
			// DataCarrier:                     getBool("datacarrier", false, alternativeContext...),
			// MaxStdTxValidationDuration:    getInt("maxstdtxvalidationduration", 3, alternativeContext...),       // 3ms
			// MaxNonStdTxValidationDuration: getInt("maxnonstdtxvalidationduration", 1000, alternativeContext...), // 1000ms
			// MaxTxChainValidationBudget:    getInt("maxtxchainvalidationbudget", 50, alternativeContext...),      // 50ms
			// ValidationClockCPU:              getBool("validationclockcpu", false, alternativeContext...),
			MinConsolidationFactor:          getInt("minconsolidationfactor", 20, alternativeContext...),
			MaxConsolidationInputScriptSize: getInt("maxconsolidationinputscriptsize", 150, alternativeContext...),
			MinConfConsolidationInput:       getInt("minconfconsolidationinput", 6, alternativeContext...),
			MinConsolidationInputMaturity:   getInt("minconsolidationinputmaturity", 6, alternativeContext...),
			AcceptNonStdConsolidationInput:  getBool("acceptnonstdconsolidationinput", false, alternativeContext...),
		},
		Kafka: KafkaSettings{
			Blocks:                getString("KAFKA_BLOCKS", "blocks", alternativeContext...),
			BlocksFinal:           getString("KAFKA_BLOCKS_FINAL", "blocks-final", alternativeContext...),
			Hosts:                 getString("KAFKA_HOSTS", "localhost:9092", alternativeContext...),
			InvalidBlocks:         getString("KAFKA_INVALID_BLOCKS", "invalid-blocks", alternativeContext...),
			InvalidSubtrees:       getString("KAFKA_INVALID_SUBTREES", "invalid-subtrees", alternativeContext...),
			LegacyInv:             getString("KAFKA_LEGACY_INV", "legacy-inv", alternativeContext...),
			Partitions:            getInt("KAFKA_PARTITIONS", 1, alternativeContext...),
			Port:                  getInt("KAFKA_PORT", 9092, alternativeContext...),
			RejectedTx:            getString("KAFKA_REJECTEDTX", "rejectedtx", alternativeContext...),
			ReplicationFactor:     getInt("KAFKA_REPLICATION_FACTOR", 1, alternativeContext...),
			Subtrees:              getString("KAFKA_SUBTREES", "subtrees", alternativeContext...),
			TxMeta:                getString("KAFKA_TXMETA", "txmeta", alternativeContext...),
			UnitTest:              getString("KAFKA_UNITTEST", "unittest", alternativeContext...),
			ValidatorTxsConfig:    getURL("kafka_validatortxsConfig", "", alternativeContext...),
			TxMetaConfig:          getURL("kafka_txmetaConfig", "", alternativeContext...),
			LegacyInvConfig:       getURL("kafka_legacyInvConfig", "", alternativeContext...),
			BlocksFinalConfig:     getURL("kafka_blocksFinalConfig", "", alternativeContext...),
			RejectedTxConfig:      getURL("kafka_rejectedTxConfig", "", alternativeContext...),
			InvalidBlocksConfig:   getURL("kafka_invalidBlocksConfig", "", alternativeContext...),
			InvalidSubtreesConfig: getURL("kafka_invalidSubtreesConfig", "", alternativeContext...),
			SubtreesConfig:        getURL("kafka_subtreesConfig", "", alternativeContext...),
			BlocksConfig:          getURL("kafka_blocksConfig", "", alternativeContext...),
			// TLS settings
			EnableTLS:     getBool("KAFKA_ENABLE_TLS", false, alternativeContext...),
			TLSSkipVerify: getBool("KAFKA_TLS_SKIP_VERIFY", false, alternativeContext...),
			TLSCAFile:     getString("KAFKA_TLS_CA_FILE", "", alternativeContext...),
			TLSCertFile:   getString("KAFKA_TLS_CERT_FILE", "", alternativeContext...),
			TLSKeyFile:    getString("KAFKA_TLS_KEY_FILE", "", alternativeContext...),
			// Debug logging
			EnableDebugLogging: getBool("kafka_enable_debug_logging", false, alternativeContext...),
		},
		Aerospike: AerospikeSettings{
			Debug:                  getBool("aerospike_debug", false, alternativeContext...),
			Host:                   getString("aerospike_host", "localhost", alternativeContext...),
			BatchPolicyURL:         getURL("aerospike_batchPolicy", "defaultBatchPolicy", alternativeContext...),
			ReadPolicyURL:          getURL("aerospike_readPolicy", "defaultReadPolicy", alternativeContext...),
			WritePolicyURL:         getURL("aerospike_writePolicy", "defaultWritePolicy", alternativeContext...),
			QueryPolicyURL:         getURL("aerospike_queryPolicy", "defaultQueryPolicy", alternativeContext...),
			Port:                   getInt("aerospike_port", 3000, alternativeContext...),
			UseDefaultBasePolicies: getBool("aerospike_useDefaultBasePolicies", false, alternativeContext...),
			UseDefaultPolicies:     getBool("aerospike_useDefaultPolicies", false, alternativeContext...),
			WarmUp:                 getBool("aerospike_warmUp", true, alternativeContext...),
			StoreBatcherDuration:   getDuration("aerospike_storeBatcherDuration", 10*time.Millisecond, alternativeContext...),
			StatsRefreshDuration:   getDuration("aerospike_statsRefresh", 5*time.Second, alternativeContext...),
		},
		Alert: AlertSettings{
			GenesisKeys:   getMultiString("alert_genesis_keys", "|", []string{}, alternativeContext...),
			P2PPrivateKey: getString("alert_p2p_private_key", "", alternativeContext...),
			ProtocolID:    getString("alert_protocol_id", "/bitcoin/alert-system/1.0.0", alternativeContext...),
			Store:         getString("alert_store", "sqlite:///alert", alternativeContext...),
			StoreURL:      getURL("alert_store", "sqlite:///alert", alternativeContext...),
			TopicName:     getString("alert_topic_name", "bitcoin_alert_system", alternativeContext...),
			P2PPort:       getPort("ALERT_P2P_PORT", 9908, alternativeContext...),
		},
		Asset: AssetSettings{
			APIPrefix:               getString("asset_apiPrefix", "/api/v1", alternativeContext...),
			CentrifugeListenAddress: getString("asset_centrifugeListenAddress", ":8892", alternativeContext...),
			CentrifugeDisable:       getBool("asset_centrifuge_disable", false, alternativeContext...),
			HTTPAddress:             getString("asset_httpAddress", "http://localhost:8090/api/v1", alternativeContext...),
			HTTPPublicAddress:       getString("asset_httpPublicAddress", "", alternativeContext...),
			HTTPListenAddress:       getString("asset_httpListenAddress", ":8090", alternativeContext...),
			HTTPPort:                getPort("ASSET_HTTP_PORT", 8090, alternativeContext...),
			SignHTTPResponses:       getBool("asset_sign_http_responses", false, alternativeContext...),
			EchoDebug:               getBool("ECHO_DEBUG", false, alternativeContext...),

			// Concurrency limits for repository methods (0 = unlimited, -1 = NumCPU(), anything else is the specific limit)
			ConcurrencyGetTransaction:         getInt("asset_concurrency_get_transaction", 0, alternativeContext...),
			ConcurrencyGetTransactionMeta:     getInt("asset_concurrency_get_transaction_meta", 0, alternativeContext...),
			ConcurrencyGetSubtreeData:         getInt("asset_concurrency_get_subtree_data", 0, alternativeContext...),
			ConcurrencyGetSubtreeDataReader:   getInt("asset_concurrency_get_subtree_data_reader", 0, alternativeContext...),
			ConcurrencyGetSubtreeTransactions: getInt("asset_concurrency_get_subtree_transactions", 0, alternativeContext...),
			ConcurrencyGetSubtreeExists:       getInt("asset_concurrency_get_subtree_exists", 0, alternativeContext...),
			ConcurrencyGetSubtreeHead:         getInt("asset_concurrency_get_subtree_head", 0, alternativeContext...),
			ConcurrencyGetUtxo:                getInt("asset_concurrency_get_utxo", 0, alternativeContext...),
			ConcurrencyGetLegacyBlockReader:   getInt("asset_concurrency_get_legacy_block_reader", -1, alternativeContext...), // -1 = NumCPU()
		},
		Block: BlockSettings{
			MinedCacheMaxMB:                         getInt("blockMinedCacheMaxMB", 256, alternativeContext...),
			PersisterStore:                          getURL("blockPersisterStore", "file://./data/blockstore", alternativeContext...),
			StateFile:                               getString("blockPersister_stateFile", "", alternativeContext...),
			PersisterHTTPListenAddress:              getString("blockPersister_httpListenAddress", ":8083", alternativeContext...),
			CheckDuplicateTransactionsConcurrency:   getInt("block_checkDuplicateTransactionsConcurrency", -1, alternativeContext...),
			GetAndValidateSubtreesConcurrency:       getInt("block_getAndValidateSubtreesConcurrency", -1, alternativeContext...),
			KafkaWorkers:                            getInt("block_kafkaWorkers", 0, alternativeContext...),
			ValidOrderAndBlessedConcurrency:         getInt("block_validOrderAndBlessedConcurrency", -1, alternativeContext...),
			MaxSize:                                 getInt("blockmaxsize", 4294967296, alternativeContext...),
			BlockStore:                              getURL("blockstore", "file://./data/blockstore", alternativeContext...),
			FailFastValidation:                      getBool("blockvalidation_fail_fast_validation", true, alternativeContext...),
			FinalizeBlockValidationConcurrency:      getInt("blockvalidation_finalizeBlockValidationConcurrency", 8, alternativeContext...),
			GetMissingTransactions:                  getInt("blockvalidation_getMissingTransactions", 32, alternativeContext...),
			QuorumTimeout:                           getDuration("block_quorum_timeout", 10*time.Second, alternativeContext...),
			BlockPersisterConcurrency:               getInt("blockpersister_concurrency", 8, alternativeContext...),
			BatchMissingTransactions:                getBool("blockpersister_batchMissingTransactions", true, alternativeContext...),
			ProcessTxMetaUsingStoreBatchSize:        getInt("blockvalidation_processTxMetaUsingStore_BatchSize", 1024, alternativeContext...),
			SkipUTXODelete:                          getBool("blockpersister_skipUTXODelete", false, alternativeContext...),
			UTXOPersisterBufferSize:                 getString("utxoPersister_buffer_size", "4KB", alternativeContext...),
			UTXOPersisterDirect:                     getBool("direct", true, alternativeContext...),
			TxStore:                                 getURL("txstore", "", alternativeContext...),
			BlockPersisterPersistAge:                uint32(getInt("blockpersister_persistAge", 2, alternativeContext...)), //nolint:gosec // G115: integer overflow conversion int -> uint32 (gosec)
			BlockPersisterPersistSleep:              getDuration("blockPersister_persistSleep", time.Minute, alternativeContext...),
			BlockPersisterEnableDefensiveReorgCheck: getBool("blockpersister_enableDefensiveReorgCheck", true, alternativeContext...),
			UtxoStore:                               getURL("txmeta_store", "", alternativeContext...),
			FileStoreReadConcurrency:                getInt("filestore_read_concurrency", 768, alternativeContext...),
			FileStoreWriteConcurrency:               getInt("filestore_write_concurrency", 256, alternativeContext...),
			FileStoreUseSystemLimits:                getBool("filestore_use_system_limits", true, alternativeContext...),
		},
		BlockAssembly: BlockAssemblySettings{
			Disabled:                             getBool("blockassembly_disabled", false, alternativeContext...),
			GRPCAddress:                          getString("blockassembly_grpcAddress", "localhost:8085", alternativeContext...),
			GRPCListenAddress:                    getString("blockassembly_grpcListenAddress", ":8085", alternativeContext...),
			GRPCMaxRetries:                       getInt("blockassembly_grpcMaxRetries", 3, alternativeContext...),
			GRPCRetryBackoff:                     getDuration("blockassembly_grpcRetryBackoff", 2*time.Second, alternativeContext...),
			LocalDAHCache:                        getString("blockassembly_localDAHCache", "", alternativeContext...),
			MaxBlockReorgCatchup:                 getInt("blockassembly_maxBlockReorgCatchup", 100, alternativeContext...),
			MaxBlockReorgRollback:                getInt("blockassembly_maxBlockReorgRollback", 100, alternativeContext...),
			MoveBackBlockConcurrency:             getInt("blockassembly_moveBackBlockConcurrency", 375, alternativeContext...),
			ProcessRemainderTxHashesConcurrency:  getInt("blockassembly_processRemainderTxHashesConcurrency", 375, alternativeContext...),
			SendBatchSize:                        getInt("blockassembly_sendBatchSize", 100, alternativeContext...),
			SendBatchTimeout:                     getInt("blockassembly_sendBatchTimeout", 2, alternativeContext...),
			SubtreeProcessorBatcherSize:          getInt("blockassembly_subtreeProcessorBatcherSize", 1000, alternativeContext...),
			SubtreeProcessorConcurrentReads:      getInt("blockassembly_subtreeProcessorConcurrentReads", 375, alternativeContext...),
			NewSubtreeChanBuffer:                 getInt("blockassembly_newSubtreeChanBuffer", 1_000, alternativeContext...),
			SubtreeRetryChanBuffer:               getInt("blockassembly_subtreeRetryChanBuffer", 1_000, alternativeContext...),
			SubmitMiningSolutionWaitForResponse:  getBool("blockassembly_SubmitMiningSolution_waitForResponse", true, alternativeContext...),
			InitialMerkleItemsPerSubtree:         getInt("initial_merkle_items_per_subtree", 1_048_576, alternativeContext...),
			MinimumMerkleItemsPerSubtree:         getInt("minimum_merkle_items_per_subtree", 1024, alternativeContext...),
			MaximumMerkleItemsPerSubtree:         getInt("maximum_merkle_items_per_subtree", 1024*1024, alternativeContext...),
			DoubleSpendWindow:                    doubleSpendWindow,
			MaxGetReorgHashes:                    getInt("blockassembly_maxGetReorgHashes", 10_000, alternativeContext...),
			MinerWalletPrivateKeys:               getMultiString("miner_wallet_private_keys", "|", []string{}, alternativeContext...),
			DifficultyCache:                      getBool("blockassembly_difficultyCache", true, alternativeContext...),
			UseDynamicSubtreeSize:                getBool("blockassembly_useDynamicSubtreeSize", false, alternativeContext...),
			MiningCandidateCacheTimeout:          getDuration("blockassembly_miningCandidateCacheTimeout", 5*time.Second),
			MiningCandidateSmartCacheMaxAge:      getDuration("blockassembly_miningCandidateSmartCacheMaxAge", 10*time.Second, alternativeContext...),
			BlockchainSubscriptionTimeout:        getDuration("blockassembly_blockchainSubscriptionTimeout", 5*time.Minute, alternativeContext...),
			OnRestartValidateParentChain:         getBool("blockassembly_onRestartValidateParentChain", true, alternativeContext...),
			ParentValidationBatchSize:            getInt("blockassembly_parentValidationBatchSize", 1000, alternativeContext...),
			OnRestartRemoveInvalidParentChainTxs: getBool("blockassembly_onRestartRemoveInvalidParentChainTxs", false, alternativeContext...),
			// getMiningCandidate timeout settings
			GetMiningCandidateSendTimeout:     getDuration("blockassembly_getMiningCandidate_send_timeout", 1*time.Second, alternativeContext...),
			GetMiningCandidateResponseTimeout: getDuration("blockassembly_getMiningCandidate_response_timeout", 10*time.Second, alternativeContext...),
			SubtreeAnnouncementInterval:       getDuration("blockassembly_subtreeAnnouncementInterval", 10*time.Second, alternativeContext...),
		},

		BlockChain: BlockChainSettings{
			GRPCAddress:           getString("blockchain_grpcAddress", "localhost:8087", alternativeContext...),
			GRPCListenAddress:     getString("blockchain_grpcListenAddress", ":8087", alternativeContext...),
			HTTPListenAddress:     getString("blockchain_httpListenAddress", ":8082", alternativeContext...),
			MaxRetries:            getInt("blockchain_maxRetries", 3, alternativeContext...),
			RetrySleep:            getInt("blockchain_retrySleep", 1000, alternativeContext...),
			StoreURL:              getURL("blockchain_store", "sqlite:///blockchain", alternativeContext...),
			FSMStateRestore:       getBool("fsm_state_restore", false, alternativeContext...),
			FSMStateChangeDelay:   getDuration("fsm_state_change_delay", 0, alternativeContext...),
			StoreDBTimeoutMillis:  getInt("blockchain_store_dbTimeoutMillis", 5000, alternativeContext...),
			InitializeNodeInState: getString("blockchain_initializeNodeInState", "", alternativeContext...),
		},
		BlockValidation: BlockValidationSettings{
			MaxRetries:                                getInt("blockV	alidationMaxRetries", 3, alternativeContext...),
			RetrySleep:                                getDuration("blockValidationRetrySleep", 1*time.Second, alternativeContext...),
			GRPCAddress:                               getString("blockvalidation_grpcAddress", "localhost:8088", alternativeContext...),
			GRPCListenAddress:                         getString("blockvalidation_grpcListenAddress", ":8088", alternativeContext...),
			KafkaWorkers:                              getInt("blockvalidation_kafkaWorkers", 0, alternativeContext...),
			LocalSetTxMinedConcurrency:                getInt("blockvalidation_localSetTxMinedConcurrency", 8, alternativeContext...),
			MaxPreviousBlockHeadersToCheck:            getUint64("blockvalidation_maxPreviousBlockHeadersToCheck", 100, alternativeContext...),
			MissingTransactionsBatchSize:              getInt("blockvalidation_missingTransactionsBatchSize", 5000, alternativeContext...),
			ProcessTxMetaUsingCacheBatchSize:          getInt("blockvalidation_processTxMetaUsingCache_BatchSize", 1024, alternativeContext...),
			ProcessTxMetaUsingCacheConcurrency:        getInt("blockvalidation_processTxMetaUsingCache_Concurrency", 32, alternativeContext...),
			ProcessTxMetaUsingCacheMissingTxThreshold: getInt("blockvalidation_processTxMetaUsingCache_MissingTxThreshold", 1, alternativeContext...),
			ProcessTxMetaUsingStoreBatchSize:          getInt("blockvalidation_processTxMetaUsingStore_BatchSize", max(4, runtime.NumCPU()/2), alternativeContext...),
			ProcessTxMetaUsingStoreConcurrency:        getInt("blockvalidation_processTxMetaUsingStore_Concurrency", 32, alternativeContext...),
			ProcessTxMetaUsingStoreMissingTxThreshold: getInt("blockvalidation_processTxMetaUsingStore_MissingTxThreshold", 1, alternativeContext...),
			SkipCheckParentMined:                      getBool("blockvalidation_skipCheckParentMined", false, alternativeContext...),
			SubtreeFoundChConcurrency:                 getInt("blockvalidation_subtreeFoundChConcurrency", 1, alternativeContext...),
			SubtreeValidationAbandonThreshold:         getInt("blockvalidation_subtree_validation_abandon_threshold", 1, alternativeContext...),
			ValidateBlockSubtreesConcurrency:          getInt("blockvalidation_validateBlockSubtreesConcurrency", max(4, runtime.NumCPU()/2), alternativeContext...),
			ValidationMaxRetries:                      getInt("blockvalidation_validation_max_retries", 3, alternativeContext...),
			ValidationRetrySleep:                      getDuration("blockvalidation_validation_retry_sleep", 5*time.Second, alternativeContext...),
			OptimisticMining:                          getBool("blockvalidation_optimistic_mining", true, alternativeContext...),
			IsParentMinedRetryMaxRetry:                getInt("blockvalidation_isParentMined_retry_max_retry", 45, alternativeContext...),
			IsParentMinedRetryBackoffMultiplier:       getInt("blockvalidation_isParentMined_retry_backoff_multiplier", 4, alternativeContext...),
			IsParentMinedRetryBackoffDuration:         getDuration("blockvalidation_isParentMined_retry_backoff_duration", 20*time.Millisecond, alternativeContext...),
			SubtreeGroupConcurrency:                   getInt("blockvalidation_subtreeGroupConcurrency", 1, alternativeContext...),
			BlockFoundChBufferSize:                    getInt("blockvalidation_blockFoundCh_buffer_size", 1000, alternativeContext...),
			CatchupChBufferSize:                       getInt("blockvalidation_catchupCh_buffer_size", 100, alternativeContext...),
			UseCatchupWhenBehind:                      getBool("blockvalidation_useCatchupWhenBehind", false, alternativeContext...),
			CatchupConcurrency:                        getInt("blockvalidation_catchupConcurrency", max(4, runtime.NumCPU()/2), alternativeContext...),
			ValidationWarmupCount:                     getInt("blockvalidation_validation_warmup_count", 128, alternativeContext...),
			BatchMissingTransactions:                  getBool("blockvalidation_batch_missing_transactions", false, alternativeContext...),
			CheckSubtreeFromBlockTimeout:              getDuration("blockvalidation_check_subtree_from_block_timeout", 5*time.Minute),
			CheckSubtreeFromBlockRetries:              getInt("blockvalidation_check_subtree_from_block_retries", 5, alternativeContext...),
			CheckSubtreeFromBlockRetryBackoffDuration: getDuration("blockvalidation_check_subtree_from_block_retry_backoff_duration", 30*time.Second),
			SecretMiningThreshold:                     getUint32("blockvalidation_secret_mining_threshold", uint32(params.CoinbaseMaturity-1), alternativeContext...), // golint:nolint
			PreviousBlockHeaderCount:                  getUint64("blockvalidation_previous_block_header_count", 100, alternativeContext...),
			MaxBlocksBehindBlockAssembly:              getInt("blockvalidation_maxBlocksBehindBlockAssembly", 20, alternativeContext...),
			// Catchup configuration
			CatchupMaxRetries:            getInt("blockvalidation_catchup_max_retries", 3, alternativeContext...),
			CatchupIterationTimeout:      getInt("blockvalidation_catchup_iteration_timeout", 30, alternativeContext...),
			CatchupOperationTimeout:      getInt("blockvalidation_catchup_operation_timeout", 300, alternativeContext...),
			CatchupMaxAccumulatedHeaders: getInt("blockvalidation_max_accumulated_headers", 100000, alternativeContext...),
			// Catchup circuit breaker configuration
			CircuitBreakerFailureThreshold: getInt("blockvalidation_circuit_breaker_failure_threshold", 5, alternativeContext...),
			CircuitBreakerSuccessThreshold: getInt("blockvalidation_circuit_breaker_success_threshold", 2, alternativeContext...),
			CircuitBreakerTimeoutSeconds:   getInt("blockvalidation_circuit_breaker_timeout_seconds", 30, alternativeContext...),
			// Block fetching configuration
			FetchLargeBatchSize:             getInt("blockvalidation_fetch_large_batch_size", 100, alternativeContext...),
			FetchNumWorkers:                 getInt("blockvalidation_fetch_num_workers", 16, alternativeContext...),
			FetchBufferSize:                 getInt("blockvalidation_fetch_buffer_size", 50, alternativeContext...),
			SubtreeFetchConcurrency:         getInt("blockvalidation_subtree_fetch_concurrency", 8, alternativeContext...),
			ExtendTransactionTimeout:        getDuration("blockvalidation_extend_transaction_timeout", 120*time.Second, alternativeContext...),
			GetBlockTransactionsConcurrency: getInt("blockvalidation_get_block_transactions_concurrency", 64, alternativeContext...),
			// Priority queue and fork processing settings
			NearForkThreshold: getInt("blockvalidation_near_fork_threshold", 0, alternativeContext...), // 0 means use default (coinbase maturity / 2)
			MaxParallelForks:  getInt("blockvalidation_max_parallel_forks", 4, alternativeContext...),
			MaxTrackedForks:   getInt("blockvalidation_max_tracked_forks", 1000, alternativeContext...),
		},
		Validator: ValidatorSettings{
			GRPCAddress:               getString("validator_grpcAddress", "localhost:8081", alternativeContext...),
			GRPCListenAddress:         getString("validator_grpcListenAddress", ":8081", alternativeContext...),
			KafkaWorkers:              getInt("validator_kafkaWorkers", 0, alternativeContext...),
			SendBatchSize:             getInt("validator_sendBatchSize", 100, alternativeContext...),
			SendBatchTimeout:          getInt("validator_sendBatchTimeout", 2, alternativeContext...),
			SendBatchWorkers:          getInt("validator_sendBatchWorkers", 10, alternativeContext...),
			BlockValidationDelay:      getInt("validator_blockvalidation_delay", 0, alternativeContext...),
			BlockValidationMaxRetries: getInt("validator_blockvalidation_maxRetries", 5, alternativeContext...),
			BlockValidationRetrySleep: getString("validator_blockvalidation_retrySleep", "2s", alternativeContext...),
			VerboseDebug:              getBool("validator_verbose_debug", false, alternativeContext...),
			HTTPListenAddress:         getString("validator_httpListenAddress", "", alternativeContext...),
			HTTPAddress:               getURL("validator_httpAddress", "", alternativeContext...),
			HTTPRateLimit:             getInt("validator_httpRateLimit", 1024, alternativeContext...),
			KafkaMaxMessageBytes:      getInt("validator_kafka_maxMessageBytes", 1024*1024, alternativeContext...), // Default 1MB
			UseLocalValidator:         getBool("useLocalValidator", false, alternativeContext...),
		},
		Region: RegionSettings{
			Name: getString("regionName", "defaultRegionName", alternativeContext...),
		},
		Advertising: AdvertisingSettings{
			Interval: getString("advertisingInterval", "10s", alternativeContext...),
			URL:      getString("advertisingURL", "defaultAdvertisingURL", alternativeContext...),
		},
		UtxoStore: UtxoStoreSettings{
			UtxoStore:                         getURL("utxostore", "", alternativeContext...),
			BlockHeightRetention:              getUint32("utxostore_blockHeightRetention", globalBlockHeightRetention, alternativeContext...),
			UnminedTxRetention:                getUint32("utxostore_unminedTxRetention", globalBlockHeightRetention/2, alternativeContext...),
			ParentPreservationBlocks:          getUint32("utxostore_parentPreservationBlocks", blocksInADayOnAverage*10, alternativeContext...),
			OutpointBatcherSize:               getInt("utxostore_outpointBatcherSize", 100, alternativeContext...),
			OutpointBatcherDurationMillis:     getInt("utxostore_outpointBatcherDurationMillis", 10, alternativeContext...),
			SpendBatcherDurationMillis:        getInt("utxostore_spendBatcherDurationMillis", 100, alternativeContext...),
			SpendBatcherSize:                  getInt("utxostore_spendBatcherSize", 100, alternativeContext...),
			SpendBatcherConcurrency:           getInt("utxostore_spendBatcherConcurrency", 32, alternativeContext...),
			SpendWaitTimeout:                  getDuration("utxostore_spendWaitTimeout", 30*time.Second, alternativeContext...),
			SpendCircuitBreakerFailureCount:   getInt("utxostore_spendCircuitBreakerFailureCount", 10, alternativeContext...),
			SpendCircuitBreakerCooldown:       getDuration("utxostore_spendCircuitBreakerCooldown", 30*time.Second, alternativeContext...),
			SpendCircuitBreakerHalfOpenMax:    getInt("utxostore_spendCircuitBreakerHalfOpenMax", 4, alternativeContext...),
			StoreBatcherDurationMillis:        getInt("utxostore_storeBatcherDurationMillis", 100, alternativeContext...),
			StoreBatcherSize:                  getInt("utxostore_storeBatcherSize", 100, alternativeContext...),
			UtxoBatchSize:                     getInt("utxostore_utxoBatchSize", 128, alternativeContext...),
			IncrementBatcherSize:              getInt("utxostore_incrementBatcherSize", 256, alternativeContext...),
			IncrementBatcherDurationMillis:    getInt("utxostore_incrementBatcherDurationMillis", 10, alternativeContext...),
			SetDAHBatcherSize:                 getInt("utxostore_setDAHBatcherSize", 256, alternativeContext...),
			SetDAHBatcherDurationMillis:       getInt("utxostore_setDAHBatcherDurationMillis", 10, alternativeContext...),
			LockedBatcherSize:                 getInt("utxostore_lockedBatcherSize", 1024, alternativeContext...),
			LockedBatcherDurationMillis:       getInt("utxostore_lockedBatcherDurationMillis", 5, alternativeContext...),
			LongestChainBatcherSize:           getInt("utxostore_longestChainBatcherSize", 1024, alternativeContext...),
			LongestChainBatcherDurationMillis: getInt("utxostore_longestChainBatcherDurationMillis", 5, alternativeContext...),
			GetBatcherSize:                    getInt("utxostore_getBatcherSize", 1, alternativeContext...),
			GetBatcherDurationMillis:          getInt("utxostore_getBatcherDurationMillis", 10, alternativeContext...),
			DBTimeout:                         getDuration("utxostore_dbTimeoutDuration", 5*time.Second, alternativeContext...),
			UseExternalTxCache:                getBool("utxostore_useExternalTxCache", true, alternativeContext...),
			ExternalizeAllTransactions:        getBool("utxostore_externalizeAllTransactions", false, alternativeContext...),
			ExternalStoreConcurrency:          getInt("utxostore_externalStoreConcurrency", 16, alternativeContext...),
			PostgresMaxIdleConns:              getInt("utxostore_utxo_postgresMaxIdleConns", 10, alternativeContext...),
			PostgresMaxOpenConns:              getInt("utxostore_utxo_postgresMaxOpenConns", 80, alternativeContext...),
			VerboseDebug:                      getBool("utxostore_verbose_debug", false, alternativeContext...),
			UpdateTxMinedStatus:               getBool("utxostore_updateTxMinedStatus", true, alternativeContext...),
			MaxMinedRoutines:                  getInt("utxostore_maxMinedRoutines", 128, alternativeContext...),
			MaxMinedBatchSize:                 getInt("utxostore_maxMinedBatchSize", 1024, alternativeContext...),
			BlockHeightRetentionAdjustment:    getInt32("utxostore_blockHeightRetentionAdjustment", 0, alternativeContext...),
			DisableDAHCleaner:                 getBool("utxostore_disableDAHCleaner", false, alternativeContext...),
		},
		P2P: P2PSettings{
			BlockTopic:         getString("p2p_block_topic", "", alternativeContext...),
			SubtreeTopic:       getString("p2p_subtree_topic", "", alternativeContext...),
			GRPCAddress:        getString("p2p_grpcAddress", "", alternativeContext...),
			GRPCListenAddress:  getString("p2p_grpcListenAddress", ":9906", alternativeContext...),
			HTTPAddress:        getString("p2p_httpAddress", "localhost:9906", alternativeContext...),
			HTTPListenAddress:  getString("p2p_httpListenAddress", "", alternativeContext...),
			ListenAddresses:    getMultiString("p2p_listen_addresses", "|", []string{}, alternativeContext...),
			AdvertiseAddresses: getMultiString("p2p_advertise_addresses", "|", []string{}, alternativeContext...), // This is used to announce the node to the network on a different address than the listen address
			Port:               getInt("p2p_port", 9906, alternativeContext...),                                   // This is the port that go-p2p-message-bus will listen on but only used when the AdvertiseAddresses are specified
			ListenMode:         getString("listen_mode", ListenModeFull, alternativeContext...),
			PeerID:             getString("p2p_peer_id", "", alternativeContext...),
			PrivateKey:         getString("p2p_private_key", "", alternativeContext...),
			RejectedTxTopic:    getString("p2p_rejected_tx_topic", "", alternativeContext...),
			StaticPeers:        getMultiString("p2p_static_peers", "|", []string{}, alternativeContext...),
			BootstrapPeers:     getMultiString("p2p_bootstrap_peers", "|", []string{}, alternativeContext...),
			// Peer persistence
			PeerCacheDir: getString("p2p_peer_cache_dir", "", alternativeContext...), // Empty = binary directory
			BanThreshold: getInt("p2p_ban_threshold", 100, alternativeContext...),
			BanDuration:  getDuration("p2p_ban_duration", 24*time.Hour),
			// Sync manager configuration
			ForceSyncPeer:         getString("p2p_force_sync_peer", "", alternativeContext...),
			NodeStatusTopic:       getString("p2p_node_status_topic", "", alternativeContext...),
			SharePrivateAddresses: getBool("p2p_share_private_addresses", true, alternativeContext...),
			// DHT configuration
			DHTMode:            getString("p2p_dht_mode", "server", alternativeContext...),
			DHTCleanupInterval: getDuration("p2p_dht_cleanup_interval", 24*time.Hour, alternativeContext...),
			// Network scanning prevention (important for shared hosting/cloud)
			// Safe defaults: NAT disabled, mDNS disabled, private IPs filtered, DHT discovery disabled
			EnableNAT:       getBool("p2p_enable_nat", false, alternativeContext...),        // Default false - UPnP scans gateway
			EnableMDNS:      getBool("p2p_enable_mdns", false, alternativeContext...),       // Default false to prevent LAN scanning
			AllowPrivateIPs: getBool("p2p_allow_private_ips", false, alternativeContext...), // Default false for production safety
			// Full/pruned node selection configuration
			AllowPrunedNodeFallback:                   getBool("p2p_allow_pruned_node_fallback", true, alternativeContext...),
			SyncCoordinatorPeriodicEvaluationInterval: getDuration("p2p_sync_coordinator_periodic_evaluation_interval", 30*time.Second, alternativeContext...),
		},
		Coinbase: CoinbaseSettings{
			DB:                          getString("coinbaseDB", "", alternativeContext...),
			UserPwd:                     getString("coinbaseDBUserPwd", "", alternativeContext...),
			ArbitraryText:               getString("coinbase_arbitrary_text", "", alternativeContext...),
			GRPCAddress:                 getString("coinbase_grpcAddress", "", alternativeContext...),
			GRPCListenAddress:           getString("coinbase_grpcListenAddress", "", alternativeContext...),
			NotificationThreshold:       getInt("coinbase_notification_threshold", 0, alternativeContext...),
			P2PPeerID:                   getString("coinbase_p2p_peer_id", "", alternativeContext...),
			P2PPrivateKey:               getString("coinbase_p2p_private_key", "", alternativeContext...),
			P2PStaticPeers:              getMultiString("coinbase_p2p_static_peers", "|", []string{}, alternativeContext...),
			ShouldWait:                  getBool("coinbase_should_wait", false, alternativeContext...),
			Store:                       getURL("coinbase_store", "", alternativeContext...),
			StoreDBTimeoutMillis:        getInt("coinbase_store_dbTimeoutMillis", 0, alternativeContext...),
			WaitForPeers:                getBool("coinbase_wait_for_peers", false, alternativeContext...),
			WalletPrivateKey:            getString("coinbase_wallet_private_key", "", alternativeContext...),
			PeerStatusTimeout:           getDuration("peerStatus_timeout", 30*time.Second, alternativeContext...),
			SlackChannel:                getString("slack_channel", "", alternativeContext...),
			SlackToken:                  getString("slack_token", "", alternativeContext...),
			TestMode:                    getBool("coinbase_test_mode", false, alternativeContext...),
			P2PPort:                     getInt("p2p_port_coinbase", 9906, alternativeContext...),
			DistributorFailureTolerance: getInt("distributor_failure_tolerance", 0, alternativeContext...),
			DistributorTimeout:          getDuration("distributor_timeout", 30*time.Second, alternativeContext...),
		},
		Pruner: PrunerSettings{
			GRPCAddress:                getString("pruner_grpcAddress", "localhost:8096", alternativeContext...),
			GRPCListenAddress:          getString("pruner_grpcListenAddress", ":8096", alternativeContext...),
			UTXODefensiveEnabled:       getBool("pruner_utxoDefensiveEnabled", false, alternativeContext...),                 // Defensive mode off by default (production)
			UTXODefensiveBatchReadSize: getInt("pruner_utxoDefensiveBatchReadSize", 10000, alternativeContext...),            // Batch size for child verification
			UTXOChunkSize:              getInt("pruner_utxoChunkSize", 1000, alternativeContext...),                          // Chunk size for batch operations
			UTXOChunkGroupLimit:        getInt("pruner_utxoChunkGroupLimit", 10, alternativeContext...),                      // Process 10 chunks in parallel
			UTXOProgressLogInterval:    getDuration("pruner_utxoProgressLogInterval", 30*time.Second, alternativeContext...), // Progress every 30s
		},
		SubtreeValidation: SubtreeValidationSettings{
			QuorumAbsoluteTimeout:                     getDuration("subtree_quorum_absolute_timeout", 30*time.Second, alternativeContext...),
			QuorumPath:                                getString("subtree_quorum_path", "", alternativeContext...),
			SubtreeStore:                              getURL("subtreestore", "", alternativeContext...),
			GetMissingTransactions:                    getInt("subtreevalidation_getMissingTransactions", max(4, runtime.NumCPU()/2), alternativeContext...),
			GRPCAddress:                               getString("subtreevalidation_grpcAddress", "localhost:8089", alternativeContext...),
			GRPCListenAddress:                         getString("subtreevalidation_grpcListenAddress", ":8089", alternativeContext...),
			ProcessTxMetaUsingCacheBatchSize:          getInt("subtreevalidation_processTxMetaUsingCache_BatchSize", 1024, alternativeContext...),
			ProcessTxMetaUsingCacheConcurrency:        getInt("subtreevalidation_processTxMetaUsingCache_Concurrency", 32, alternativeContext...),
			ProcessTxMetaUsingCacheMissingTxThreshold: getInt("subtreevalidation_processTxMetaUsingCache_MissingTxThreshold", 1, alternativeContext...),
			SubtreeBlockHeightRetention:               getUint32("subtreevalidation_subtreeBlockHeightRetention", globalBlockHeightRetention),
			SubtreeDAHConcurrency:                     getInt("subtreevalidation_subtreeDAHConcurrency", 8, alternativeContext...),
			TxMetaCacheEnabled:                        getBool("subtreevalidation_txMetaCacheEnabled", true, alternativeContext...),
			TxMetaCacheMaxMB:                          getInt("txMetaCacheMaxMB", 256, alternativeContext...),
			TxChanBufferSize:                          getInt("subtreevalidation_txChanBufferSize", 0, alternativeContext...),
			BatchMissingTransactions:                  getBool("subtreevalidation_batch_missing_transactions", true, alternativeContext...),
			SpendBatcherSize:                          getInt("subtreevalidation_spendBatcherSize", 1024, alternativeContext...),
			MissingTransactionsBatchSize:              getInt("subtreevalidation_missingTransactionsBatchSize", 16_384, alternativeContext...),
			PercentageMissingGetFullData:              getFloat64("subtreevalidation_percentageMissingGetFullData", 20, alternativeContext...),
			BlacklistedBaseURLs:                       blacklistMap,
			BlockHeightRetentionAdjustment:            getInt32("subtreevalidation_blockHeightRetentionAdjustment", 0, alternativeContext...),
			OrphanageTimeout:                          getDuration("subtreevalidation_orphanageTimeout", 15*time.Minute, alternativeContext...),
			OrphanageMaxSize:                          getInt("subtreevalidation_orphanageMaxSize", 100_000, alternativeContext...),
			CheckBlockSubtreesConcurrency:             getInt("subtreevalidation_check_block_subtrees_concurrency", 32, alternativeContext...),
			PauseTimeout:                              getDuration("subtreevalidation_pauseTimeout", 5*time.Minute, alternativeContext...),
			TxBatchSize:                               getInt("subtreevalidation_check_block_subtrees_tx_batch_size", 1048576, alternativeContext...),
			UseOrderedLevelAlgorithm:                  getBool("subtreevalidation_useOrderedLevelAlgorithm", true, alternativeContext...),
		},
		Legacy: LegacySettings{
			WorkingDir:                       getString("legacy_workingDir", "../../data", alternativeContext...),
			ListenAddresses:                  getMultiString("legacy_listen_addresses", "|", []string{}, alternativeContext...),
			ConnectPeers:                     getMultiString("legacy_connect_peers", "|", []string{}, alternativeContext...),
			OrphanEvictionDuration:           getDuration("legacy_orphanEvictionDuration", 10*time.Minute, alternativeContext...),
			StoreBatcherSize:                 getInt("legacy_storeBatcherSize", 1024, alternativeContext...),
			StoreBatcherConcurrency:          getInt("legacy_storeBatcherConcurrency", 32, alternativeContext...),
			SpendBatcherSize:                 getInt("legacy_spendBatcherSize", 1024, alternativeContext...),
			SpendBatcherConcurrency:          getInt("legacy_spendBatcherConcurrency", 32, alternativeContext...),
			OutpointBatcherSize:              getInt("legacy_outpointBatcherSize", 1024, alternativeContext...),
			OutpointBatcherConcurrency:       getInt("legacy_outpointBatcherConcurrency", 32, alternativeContext...),
			PrintInvMessages:                 getBool("legacy_printInvMessages", false, alternativeContext...),
			GRPCAddress:                      getString("legacy_grpcAddress", "", alternativeContext...),
			AllowBlockPriority:               getBool("legacy_allowBlockPriority", false, alternativeContext...),
			GRPCListenAddress:                getString("legacy_grpcListenAddress", "", alternativeContext...),
			SavePeers:                        getBool("legacy_savePeers", false, alternativeContext...), // by default we do not save the peers
			AllowSyncCandidateFromLocalPeers: getBool("legacy_allowSyncCandidateFromLocalPeers", false, alternativeContext...),
			TempStore:                        getURL("temp_store", "file://./data/tempstore", alternativeContext...),
			PeerIdleTimeout:                  getDuration("legacy_peerIdleTimeout", 125*time.Second, alternativeContext...),     // ping/pong interval is 2 mins, so we set this to 125s to be sure
			PeerProcessingTimeout:            getDuration("legacy_peerProcessingTimeout", 3*time.Minute, alternativeContext...), // processing a block will be the largest message to process
		},
		Propagation: PropagationSettings{
			IPv6Addresses:        getString("ipv6_addresses", "", alternativeContext...),
			IPv6Interface:        getString("ipv6_interface", "", alternativeContext...),
			GRPCMaxConnectionAge: getDuration("propagation_grpcMaxConnectionAge", 90*time.Second, alternativeContext...),
			HTTPListenAddress:    getString("propagation_httpListenAddress", "", alternativeContext...),
			HTTPAddresses:        getMultiString("propagation_httpAddresses", "|", []string{}, alternativeContext...),
			HTTPRateLimit:        getInt("propagation_httpRateLimit", 1024, alternativeContext...),
			AlwaysUseHTTP:        getBool("propagation_alwaysUseHTTP", false, alternativeContext...),
			SendBatchSize:        getInt("propagation_sendBatchSize", 100, alternativeContext...),
			SendBatchTimeout:     getInt("propagation_sendBatchTimeout", 5, alternativeContext...),
			GRPCAddresses:        getMultiString("propagation_grpcAddresses", "|", []string{}, alternativeContext...),
			GRPCListenAddress:    getString("propagation_grpcListenAddress", "", alternativeContext...),
		},
		RPC: RPCSettings{
			RPCUser:           getString("rpc_user", "", alternativeContext...),
			RPCPass:           getString("rpc_pass", "", alternativeContext...),
			RPCLimitUser:      getString("rpc_limit_user", "", alternativeContext...),
			RPCLimitPass:      getString("rpc_limit_pass", "", alternativeContext...),
			RPCMaxClients:     getInt("rpc_max_clients", 1, alternativeContext...),
			RPCQuirks:         getBool("rpc_quirks", true, alternativeContext...),
			RPCListenerURL:    getURL("rpc_listener_url", "", alternativeContext...),
			CacheEnabled:      getBool("rpc_cache_enabled", true, alternativeContext...),
			RPCTimeout:        getDuration("rpc_timeout", 30*time.Second, alternativeContext...),
			ClientCallTimeout: getDuration("rpc_client_call_timeout", 5*time.Second, alternativeContext...),
		},
		Faucet: FaucetSettings{
			HTTPListenAddress: getString("faucet_httpListenAddress", "", alternativeContext...),
		},
		Dashboard: DashboardSettings{
			Enabled:        getBool("dashboard_enabled", false, alternativeContext...),
			DevServerPorts: getIntSlice("dashboard_devServerPorts", []int{5173, 4173}, alternativeContext...),
			WebSocketPort:  getString("dashboard_websocketPort", "8090", alternativeContext...),
			WebSocketPath:  getString("dashboard_websocketPath", "/connection/websocket", alternativeContext...),
		},
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
