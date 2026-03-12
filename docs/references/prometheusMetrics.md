# Teranode Prometheus Metrics Reference

## Metric Types

| Type       | Description                                                                                                                                                                                                                                               |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Counter    | A cumulative metric that represents an increasing counter whose value can only increase OR be reset to zero. <br/>Used for counting events or operations (e.g., number of requests, errors).                                                              |
| CounterVec | A Counter that includes additional labels/dimensions. <br/>Allows for breaking down the counter by various labels (e.g., counting errors by type, requests by status code).                                                                               |
| Gauge        | A metric that represents a single numerical value that can arbitrarily go up and down. <br/>Used for measured values like items in a channel or queue.                                                                                                    |
| GaugeVec     | A Gauge that includes additional labels/dimensions. <br/>Allows for breaking down the gauge by various labels (e.g., index readiness by name).                                                                                                           |
| Histogram    | Samples observations (such as duration or size) and counts them in configurable buckets. <br/>Also provides a sum of all observed values and count of observations. <br/>Used for measuring distributions of values (e.g., request durations, response sizes). |
| HistogramVec | A Histogram that includes additional labels/dimensions. <br/>Allows for breaking down the histogram by various labels (e.g., duration by state, operation by scan type).                                                                                  |

## Block Model Metrics

| Metric Name                                    | Type      | Description                                  |
|------------------------------------------------|-----------|----------------------------------------------|
| `teranode_block_from_bytes`                    | Histogram | Histogram of Block.FromBytes                 |
| `teranode_block_valid`                         | Histogram | Histogram of Block.Valid                     |
| `teranode_block_check_merkle_root`             | Histogram | Histogram of Block.CheckMerkleRoot           |
| `teranode_block_get_subtrees`                  | Histogram | Histogram of Block.GetSubtrees               |
| `teranode_block_get_and_validate_subtrees`     | Histogram | Histogram of Block.GetAndValidateSubtrees    |

## Alert Service Metrics

| Metric Name             | Type    | Description                            |
|-------------------------|---------|----------------------------------------|
| `teranode_alert_health` | Counter | Number of calls to the Health endpoint |

## Asset Service HTTP Metrics

All metrics are CounterVec type with labels: `function` (handler function name), `operation` (operation result).

| Metric Name                                 | Type       | Description                         |
|---------------------------------------------|------------|-------------------------------------|
| `teranode_asset_http_get_transaction`       | CounterVec | Number of Get transactions ops      |
| `teranode_asset_http_get_transactions`      | CounterVec | Number of Get transactions ops      |
| `teranode_asset_http_get_subtree`           | CounterVec | Number of Get subtree ops           |
| `teranode_asset_http_get_block_header`      | CounterVec | Number of Get block header ops      |
| `teranode_asset_http_get_best_block_header` | CounterVec | Number of Get best block header ops |
| `teranode_asset_http_get_block`             | CounterVec | Number of Get block ops             |
| `teranode_asset_http_get_block_legacy`      | CounterVec | Number of Get legacy block ops      |
| `teranode_asset_http_get_subtree_data`      | CounterVec | Number of Get subtree data ops      |
| `teranode_asset_http_get_last_n_blocks`     | CounterVec | Number of Get last N blocks ops     |
| `teranode_asset_http_get_utxo`              | CounterVec | Number of Get UTXO ops              |
| `teranode_asset_http_get_merkle_proof`      | CounterVec | Number of Get merkle proof ops      |

## Block Assembly Service Metrics

| Metric Name                                                   | Type      | Description                                                                      |
|---------------------------------------------------------------|-----------|----------------------------------------------------------------------------------|
| `teranode_blockassembly_health`                               | Counter   | Number of calls to the health endpoint of the blockassembly service              |
| `teranode_blockassembly_add_tx`                               | Histogram | Histogram of AddTx in the blockassembly service                                  |
| `teranode_blockassembly_remove_tx`                            | Histogram | Histogram of RemoveTx in the blockassembly service                               |
| `teranode_blockassembly_get_mining_candidate_duration`        | Histogram | Histogram of GetMiningCandidate in the blockassembly service                     |
| `teranode_blockassembly_submit_mining_solution_ch`            | Gauge     | Number of items in the SubmitMiningSolution channel in the blockassembly service |
| `teranode_blockassembly_submit_mining_solution`               | Histogram | Histogram of SubmitMiningSolution in the blockassembly service                   |
| `teranode_blockassembly_update_subtrees_dah`                  | Histogram | Histogram of updating subtrees DAH in the blockassembly service                  |
| `teranode_blockassembly_block_assembler_get_mining_candidate` | Counter   | Number of calls to GetMiningCandidate in the block assembler                     |
| `teranode_blockassembly_subtree_stored`                       | Histogram | Histogram of subtree stored duration in block assembler                          |
| `teranode_blockassembly_transactions`                         | Gauge     | Number of transactions currently in the block assembler subtree processor        |
| `teranode_blockassembly_queued_transactions`                  | Gauge     | Number of transactions currently queued in the block assembler subtree processor |
| `teranode_blockassembly_subtrees`                             | Gauge     | Number of subtrees currently in the block assembler subtree processor            |
| `teranode_blockassembly_tx_meta_get`                          | Histogram | Histogram of reading tx meta data from txmeta store in block assembler           |
| `teranode_blockassembly_reorg`                                | Counter   | Number of reorgs in block assembler                                              |
| `teranode_blockassembly_reorg_duration`                       | Histogram | Histogram of reorg in block assembler                                            |
| `teranode_blockassembly_get_reorg_blocks_duration`            | Histogram | Histogram of GetReorgBlocks in block assembler                                   |
| `teranode_blockassembly_update_best_block`                    | Histogram | Histogram of updating best block in block assembler                              |
| `teranode_blockassembly_best_block_height`                    | Gauge     | Best block height in block assembly                                              |
| `teranode_blockassembly_current_block_height`                 | Gauge     | Current block height in block assembly                                           |
| `teranode_blockassembly_generate_blocks`                      | Histogram | Histogram of generating blocks in block assembler                                |
| `teranode_blockassembly_current_state`                                    | Gauge        | Current state of the block assembly process                                     |
| `teranode_blockassembly_add_tx_counter`                                   | Counter      | Number of transactions added in the blockassembly service                       |
| `teranode_blockassembly_state_transitions_total`                          | CounterVec   | Total number of state transitions (labels: `from`, `to`)                        |
| `teranode_blockassembly_state_duration_seconds`                           | HistogramVec | Time spent in each state (label: `state`)                                       |
| `teranode_blockassembly_utxo_index_ready`                                 | GaugeVec     | Status of UTXO index readiness (label: `index_name`)                            |
| `teranode_blockassembly_utxo_index_wait_seconds`                          | HistogramVec | Time taken waiting for UTXO index to become ready (labels: `index_name`, `status`) |
| `teranode_blockassembly_get_unmined_tx_iterator_seconds`                  | HistogramVec | Time taken to get unmined transaction iterator (labels: `full_scan`, `status`)  |
| `teranode_blockassembly_iterator_processing_seconds`                      | HistogramVec | Time taken to process all transactions from the unmined iterator (label: `full_scan`) |
| `teranode_blockassembly_iterator_transactions_total`                      | CounterVec   | Total number of transactions processed from the unmined iterator (label: `full_scan`) |
| `teranode_blockassembly_iterator_transactions_stats`                      | CounterVec   | Statistics about transactions processed from the iterator (labels: `full_scan`, `category`) |
| `teranode_blockassembly_mark_transactions_on_longest_chain_seconds`       | Histogram    | Time taken to mark transactions as mined on longest chain                       |
| `teranode_blockassembly_mark_transactions_on_longest_chain_total`         | Counter      | Total number of transactions marked as mined on longest chain                   |
| `teranode_blockassembly_sort_transactions_seconds`                        | HistogramVec | Time taken to sort unmined transactions by createdAt (label: `transaction_count_bucket`) |
| `teranode_blockassembly_validate_parent_chain_seconds`                    | Histogram    | Time taken to validate parent chain for unmined transactions                    |
| `teranode_blockassembly_validate_parent_chain_filtered_total`             | Counter      | Total number of transactions filtered out during parent chain validation        |
| `teranode_blockassembly_add_directly_seconds`                             | Histogram    | Time taken for individual AddDirectly calls to subtree processor                |
| `teranode_blockassembly_add_directly_total`                               | Counter      | Total number of transactions added directly to subtree processor                |
| `teranode_blockassembly_add_directly_batch_seconds`                       | Histogram    | Time taken to add all unmined transactions to subtree processor                 |

## Blockchain Service Metrics

| Metric Name                                             | Type      | Description                                                             |
|---------------------------------------------------------|-----------|-------------------------------------------------------------------------|
| `teranode_blockchain_health`                            | Counter   | Number of calls to the health endpoint of the blockchain service        |
| `teranode_blockchain_add_block`                         | Histogram | Histogram of block added to the blockchain service                      |
| `teranode_blockchain_get_block`                         | Histogram | Histogram of Get block calls to the blockchain service                  |
| `teranode_blockchain_get_block_stats`                   | Histogram | Histogram of Get block stats calls to the blockchain service            |
| `teranode_blockchain_get_block_graph_data`              | Histogram | Histogram of Get block graph data calls to the blockchain service       |
| `teranode_blockchain_get_last_n_block`                  | Histogram | Histogram of GetLastNBlocks calls to the blockchain service             |
| `teranode_blockchain_get_suitable_block`                | Histogram | Histogram of GetSuitableBlock calls to the blockchain service           |
| `teranode_blockchain_get_hash_of_ancestor_block`        | Histogram | Histogram of GetHashOfAncestorBlock calls to the blockchain service     |
| `teranode_blockchain_get_next_work_required`            | Histogram | Histogram of GetNextWorkRequired calls to the blockchain service        |
| `teranode_blockchain_get_block_exists`                  | Histogram | Histogram of GetBlockExists calls to the blockchain service             |
| `teranode_blockchain_get_get_best_block_header`         | Histogram | Histogram of GetBestBlockHeader calls to the blockchain service         |
| `teranode_blockchain_check_block_is_in_current_chain`   | Histogram | Histogram of CheckBlockIsInCurrentChain calls to the blockchain service |
| `teranode_blockchain_get_chain_tips`                    | Histogram | Histogram of GetChainTips calls to the blockchain service               |
| `teranode_blockchain_get_get_block_header`              | Histogram | Histogram of GetBlockHeader calls to the blockchain service             |
| `teranode_blockchain_get_get_block_headers`             | Histogram | Histogram of GetBlockHeaders calls to the blockchain service            |
| `teranode_blockchain_get_get_block_headers_from_height` | Histogram | Histogram of GetBlockHeadersFromHeight calls to the blockchain service  |
| `teranode_blockchain_get_get_block_headers_by_height`   | Histogram | Histogram of GetBlockHeadersByHeight calls to the blockchain service    |
| `teranode_blockchain_get_block_is_mined`                | Histogram | Histogram of GetBlockIsMined calls to the blockchain service            |
| `teranode_blockchain_subscribe`                         | Histogram | Histogram of Subscribe calls to the blockchain service                  |
| `teranode_blockchain_get_state`                         | Histogram | Histogram of GetState calls to the blockchain service                   |
| `teranode_blockchain_set_state`                         | Histogram | Histogram of SetState calls to the blockchain service                   |
| `teranode_blockchain_get_block_header_ids`              | Histogram | Histogram of GetBlockHeaderIDs calls to the blockchain service          |
| `teranode_blockchain_invalidate_block`                  | Histogram | Histogram of InvalidateBlock calls to the blockchain service            |
| `teranode_blockchain_revalidate_block`                  | Histogram | Histogram of RevalidateBlock calls to the blockchain service            |
| `teranode_blockchain_send_notification`                 | Histogram | Histogram of SendNotification calls to the blockchain service           |
| `teranode_blockchain_set_block_mined_set`               | Histogram | Histogram of SetBlockMinedSet calls to the blockchain service           |
| `teranode_blockchain_get_blocks_mined_not_set`          | Histogram | Histogram of GetBlocksMinedNotSet calls to the blockchain service       |
| `teranode_blockchain_set_block_subtrees_set`            | Histogram | Histogram of SetBlockSubtreesSet calls to the blockchain service        |
| `teranode_blockchain_get_blocks_subtrees_not_set`       | Histogram | Histogram of GetBlocksSubtreesNotSet calls to the blockchain service    |
| `teranode_blockchain_fsm_current_state`                 | Gauge     | Current state of the blockchain FSM                                     |
| `teranode_blockchain_get_fsm_current_state`             | Histogram | Histogram of GetFSMCurrentState calls to the blockchain service         |
| `teranode_blockchain_get_block_locator`                              | Histogram | Histogram of GetBlockLocator calls to the blockchain service                           |
| `teranode_blockchain_locate_block_headers`                           | Histogram | Histogram of LocateBlockHeaders calls to the blockchain service                        |
| `teranode_blockchain_get_latest_block_header_from_block_locator`     | Histogram | Histogram of GetLatestBlockHeaderFromBlockLocator calls to the blockchain service      |
| `teranode_blockchain_get_block_headers_from_oldest`                  | Histogram | Histogram of GetBlockHeadersFromOldest calls to the blockchain service                 |
| `teranode_blockchain_check_block_is_ancestor_of_block`               | Histogram | Histogram of CheckBlockIsAncestorOfBlock calls to the blockchain service               |
| `teranode_blockchain_get_blocks_by_height`                           | Histogram | Histogram of GetBlocksByHeight calls to the blockchain service                         |
| `teranode_blockchain_set_block_persisted_at`                         | Histogram | Histogram of SetBlockPersistedAt calls to the blockchain service                       |
| `teranode_blockchain_get_blocks_not_persisted`                       | Histogram | Histogram of GetBlocksNotPersisted calls to the blockchain service                     |

## Block Persister Service Metrics

| Metric Name                                              | Type      | Description                                                           |
|----------------------------------------------------------|-----------|-----------------------------------------------------------------------|
| `teranode_blockpersister_validate_subtree`               | Histogram | Histogram of subtree validation                                       |
| `teranode_blockpersister_validate_subtree_retry`         | Counter   | Number of retries when subtrees validated                             |
| `teranode_blockpersister_validate_subtree_handler`       | Histogram | Histogram of subtree handler                                          |
| `teranode_blockpersister_persist_block`                  | Histogram | Histogram of PersistBlock in the blockpersister service               |
| `teranode_blockpersister_bless_missing_transaction`      | Histogram | Histogram of bless missing transaction                                |
| `teranode_blockpersister_set_tx_meta_cache_kafka`        | Histogram | Histogram of setting tx meta cache from kafka                         |
| `teranode_blockpersister_del_tx_meta_cache_kafka`        | Histogram | Duration of deleting tx meta cache from kafka                         |
| `teranode_blockpersister_set_tx_meta_cache_kafka_errors` | Counter   | Number of errors setting tx meta cache from kafka                     |
| `teranode_blockpersister_blocks_duration`                | Histogram | Duration of block processing by the block persister service           |
| `teranode_blockpersister_subtrees_duration`              | Histogram | Duration of subtree processing by the block persister service         |
| `teranode_blockpersister_subtree_batch_duration`         | Histogram | Duration of a subtree batch processing by the block persister service |

## Block Validation Service Metrics

| Metric Name                                            | Type      | Description                                                       |
|--------------------------------------------------------|-----------|-------------------------------------------------------------------|
| `teranode_blockvalidation_health`                      | Counter   | Number of health checks                                           |
| `teranode_blockvalidation_block_found_ch`              | Gauge     | Number of blocks found buffered in the block found channel        |
| `teranode_blockvalidation_block_found`                 | Histogram | Histogram of calls to BlockFound method                           |
| `teranode_blockvalidation_catchup_ch`                  | Gauge     | Number of catchups buffered in the catchup channel                |
| `teranode_blockvalidation_catchup`                     | Histogram | Histogram of catchup events                                       |
| `teranode_blockvalidation_process_block_found`         | Histogram | Histogram of process block found                                  |
| `teranode_blockvalidation_validate_block`              | Histogram | Histogram of calls to ValidateBlock method                        |
| `teranode_blockvalidation_revalidate_block`            | Histogram | Histogram of re-validate block                                    |
| `teranode_blockvalidation_revalidate_block_err`        | Histogram | Number of blocks revalidated with error                           |
| `teranode_blockvalidation_last_validated_blocks_cache` | Gauge     | Number of blocks in the last validated blocks cache               |
| `teranode_blockvalidation_block_exists_cache`          | Gauge     | Number of blocks in the block exists cache                        |
| `teranode_blockvalidation_subtree_exists_cache`        | Gauge     | Number of subtrees in the subtree exists cache                    |

### Catchup Operation Metrics

CounterVec and HistogramVec metrics use labels: `peer_id`, `success`, `error_type`, `priority`, `result`, `fork_id`, `reason`.

| Metric Name                                                  | Type         | Description                                                      |
|--------------------------------------------------------------|--------------|------------------------------------------------------------------|
| `teranode_blockvalidation_catchup_duration_seconds`          | HistogramVec | Duration of catchup operations in seconds                        |
| `teranode_blockvalidation_catchup_blocks_fetched_total`      | CounterVec   | Total number of blocks fetched during catchup                    |
| `teranode_blockvalidation_catchup_headers_fetched_total`     | CounterVec   | Total number of headers fetched during catchup                   |
| `teranode_blockvalidation_catchup_errors_total`              | CounterVec   | Total number of errors during catchup operations                 |
| `teranode_blockvalidation_catchup_active`                    | Gauge        | Number of active catchup operations (0 or 1)                     |
| `teranode_blockvalidation_priority_queue_size`               | GaugeVec     | Current size of the block priority queue by priority level       |
| `teranode_blockvalidation_priority_queue_added_total`        | CounterVec   | Total number of blocks added to priority queue by priority level |
| `teranode_blockvalidation_priority_queue_processed_total`    | CounterVec   | Total number of blocks processed from priority queue             |
| `teranode_blockvalidation_fork_count`                        | Gauge        | Current number of active forks being tracked                     |
| `teranode_blockvalidation_fork_processing_workers`           | Gauge        | Current number of active fork processing workers                 |
| `teranode_blockvalidation_fork_blocks_processed_total`       | CounterVec   | Total number of blocks processed on forks by fork ID             |
| `teranode_blockvalidation_fork_created_total`                | CounterVec   | Total number of forks created                                    |
| `teranode_blockvalidation_fork_lifetime_seconds`             | Histogram    | Lifetime of forks in seconds                                     |
| `teranode_blockvalidation_fork_depth_blocks`                 | Histogram    | Depth of forks in blocks                                         |
| `teranode_blockvalidation_fork_resolved_total`               | CounterVec   | Total number of forks resolved                                   |
| `teranode_blockvalidation_fork_resolution_depth_blocks`      | Histogram    | Depth at which forks were resolved in blocks                     |
| `teranode_blockvalidation_fork_orphaned_total`               | Counter      | Total number of forks marked as orphaned                         |
| `teranode_blockvalidation_processing_blocks_stuck`           | Gauge        | Number of blocks stuck in processing state                       |
| `teranode_blockvalidation_fork_average_depth`                | Gauge        | Average depth of active forks in blocks                          |
| `teranode_blockvalidation_fork_longest_depth`                | Gauge        | Longest active fork depth in blocks                              |
| `teranode_blockvalidation_fork_average_lifetime_seconds`     | Gauge        | Average lifetime of active forks in seconds                      |
| `teranode_blockvalidation_queue_skip_count`                  | Histogram    | Number of times blocks were skipped before processing            |
| `teranode_blockvalidation_queue_wait_seconds`                | Histogram    | Time blocks spend in queue before processing                     |

## Legacy Peer Server Metrics

Each metric measures "The time taken to handle a specific legacy action handler".

| Metric Name                                | Type      | Description                           |
|--------------------------------------------|-----------|---------------------------------------|
| `teranode_legacy_peer_server_OnVersion`    | Histogram | The time taken to handle OnVersion    |
| `teranode_legacy_peer_server_OnProtoconf`  | Histogram | The time taken to handle OnProtoconf  |
| `teranode_legacy_peer_server_OnMemPool`    | Histogram | The time taken to handle OnMemPool    |
| `teranode_legacy_peer_server_OnTx`         | Histogram | The time taken to handle OnTx         |
| `teranode_legacy_peer_server_OnBlock`      | Histogram | The time taken to handle OnBlock      |
| `teranode_legacy_peer_server_OnInv`        | Histogram | The time taken to handle OnInv        |
| `teranode_legacy_peer_server_OnHeaders`    | Histogram | The time taken to handle OnHeaders    |
| `teranode_legacy_peer_server_OnGetData`    | Histogram | The time taken to handle OnGetData    |
| `teranode_legacy_peer_server_OnGetBlocks`  | Histogram | The time taken to handle OnGetBlocks  |
| `teranode_legacy_peer_server_OnGetHeaders` | Histogram | The time taken to handle OnGetHeaders |
| `teranode_legacy_peer_server_OnFeeFilter`  | Histogram | The time taken to handle OnFeeFilter  |
| `teranode_legacy_peer_server_OnGetAddr`    | Histogram | The time taken to handle OnGetAddr    |
| `teranode_legacy_peer_server_OnAddr`       | Histogram | The time taken to handle OnAddr       |
| `teranode_legacy_peer_server_OnReject`     | Histogram | The time taken to handle OnReject     |
| `teranode_legacy_peer_server_OnNotFound`   | Histogram | The time taken to handle OnNotFound   |
| `teranode_legacy_peer_server_OnRead`       | Histogram | The time taken to handle OnRead       |
| `teranode_legacy_peer_server_OnWrite`          | Histogram | The time taken to handle OnWrite          |
| `teranode_legacy_peer_server_OnCreateStream`   | Histogram | The time taken to handle OnCreateStream   |
| `teranode_legacy_peer_server_OnStreamAck`      | Histogram | The time taken to handle OnStreamAck      |

## Legacy NetSync Service Metrics

| Metric Name                                                 | Type      | Description                                               |
|-------------------------------------------------------------|-----------|-----------------------------------------------------------|
| `teranode_legacy_netsync_block_height`                      | Gauge     | The height of the block being processed                   |
| `teranode_legacy_netsync_handle_tx_msg`                     | Histogram | The time taken to handle a tx message                     |
| `teranode_legacy_netsync_handle_tx_msg_validate`            | Histogram | The time taken to validate a tx message                   |
| `teranode_legacy_netsync_process_orphan_transactions`       | Histogram | The time taken to process orphan transactions             |
| `teranode_legacy_netsync_handle_block_direct`               | Histogram | The time taken to handle a block directly                 |
| `teranode_legacy_netsync_process_block`                     | Histogram | The time taken to process a block                         |
| `teranode_legacy_netsync_prepare_subtrees`                  | Histogram | The time taken to prepare the subtrees                    |
| `teranode_legacy_netsync_validate_transactions_legacy_mode` | Histogram | The time taken to validate transactions in legacy mode    |
| `teranode_legacy_netsync_pre_validate_transactions`         | Histogram | The time taken to pre-validate transactions               |
| `teranode_legacy_netsync_validate_transactions`             | Histogram | The time taken to validate transactions                   |
| `teranode_legacy_netsync_extend_transactions`               | Histogram | The time taken to extend transactions                     |
| `teranode_legacy_netsync_create_utxos`                      | Histogram | The time taken to create UTXOs                            |
| `teranode_legacy_netsync_block_tx_size`                     | Histogram | The size of the transactions in the block being processed |
| `teranode_legacy_netsync_block_tx_nr_inputs`                | Histogram | The number of inputs in the block being processed         |
| `teranode_legacy_netsync_block_tx_nr_outputs`               | Histogram | The number of outputs in the block being processed        |
| `teranode_legacy_netsync_block_tx_extend`                   | Histogram | The time taken to extend a transaction                    |
| `teranode_legacy_netsync_block_tx_validate`                 | Histogram | The time taken to validate a transaction                  |
| `teranode_legacy_netsync_orphans`                           | Gauge     | The number of orphan transactions                         |
| `teranode_legacy_netsync_orphan_time`                       | Histogram | The time taken to process an orphan transaction           |

## Propagation Service Metrics

| Metric Name                                      | Type      | Description                                                           |
|--------------------------------------------------|-----------|-----------------------------------------------------------------------|
| `teranode_propagation_health`                    | Histogram | Histogram of calls to the health endpoint of the propagation service                    |
| `teranode_propagation_transactions`              | Histogram | Histogram of transaction processing by the propagation service                           |
| `teranode_propagation_transactions_batch`        | Histogram | Histogram of transaction processing by the propagation service                           |
| `teranode_propagation_handle_single_tx`          | Histogram | Histogram of transaction processing by the propagation service using HTTP                |
| `teranode_propagation_handle_multiple_tx`        | Histogram | Histogram of multiple transaction processing by the propagation service using HTTP       |
| `teranode_propagation_transactions_size`         | Histogram | Size of transactions processed by the propagation service                                |
| `teranode_propagation_invalid_transactions`      | Counter   | Number of transactions found invalid by the propagation service                          |

## RPC Service Metrics

| Metric Name                           | Type      | Description                                                         |
|---------------------------------------|-----------|---------------------------------------------------------------------|
| `teranode_rpc_get_block`              | Histogram | Histogram of calls to handleGetBlock in the rpc service             |
| `teranode_rpc_get_block_by_height`    | Histogram | Histogram of calls to handleGetBlockByHeight in the rpc service     |
| `teranode_rpc_get_block_hash`         | Histogram | Histogram of calls to handleGetBlockHash in the rpc service         |
| `teranode_rpc_get_block_header`       | Histogram | Histogram of calls to handleGetBlockHeader in the rpc service       |
| `teranode_rpc_get_best_block_hash`    | Histogram | Histogram of calls to handleGetBestBlockHash in the rpc service     |
| `teranode_rpc_get_raw_transaction`    | Histogram | Histogram of calls to handleGetRawTransaction in the rpc service    |
| `teranode_rpc_create_raw_transaction` | Histogram | Histogram of calls to handleCreateRawTransaction in the rpc service |
| `teranode_rpc_send_raw_transaction`   | Histogram | Histogram of calls to handleSendRawTransaction in the rpc service   |
| `teranode_rpc_generate`               | Histogram | Histogram of calls to handleGenerate in the rpc service             |
| `teranode_rpc_generate_to_address`    | Histogram | Histogram of calls to handleGenerateToAddress in the rpc service    |
| `teranode_rpc_get_mining_candidate`   | Histogram | Histogram of calls to handleGetMiningCandidate in the rpc service   |
| `teranode_rpc_submit_mining_solution` | Histogram | Histogram of calls to handleSubmitMiningSolution in the rpc service |
| `teranode_rpc_get_peer_info`          | Histogram | Histogram of calls to handleGetpeerinfo in the rpc service          |
| `teranode_rpc_get_rawmempool`         | Histogram | Histogram of calls to handleGetRawmempool in the rpc service        |
| `teranode_rpc_get_blockchain_info`    | Histogram | Histogram of calls to handleGetblockchaininfo in the rpc service    |
| `teranode_rpc_get_info`               | Histogram | Histogram of calls to handleGetinfo in the rpc service              |
| `teranode_rpc_get_difficulty`         | Histogram | Histogram of calls to handleGetDifficulty in the rpc service        |
| `teranode_rpc_invalidate_block`       | Histogram | Histogram of calls to handleInvalidateBlock in the rpc service      |
| `teranode_rpc_reconsider_block`       | Histogram | Histogram of calls to handleReconsiderBlock in the rpc service      |
| `teranode_rpc_help`                   | Histogram | Histogram of calls to handleHelp in the rpc service                 |
| `teranode_rpc_set_ban`                | Histogram | Histogram of calls to handleSetBan in the rpc service               |
| `teranode_rpc_is_banned`              | Histogram | Histogram of calls to handleIsBanned in the rpc service             |
| `teranode_rpc_get_mining_info`        | Histogram | Histogram of calls to handleGetMiningInfo in the rpc service        |
| `teranode_rpc_list_banned`            | Histogram | Histogram of calls to handleListBanned in the rpc service           |
| `teranode_rpc_clear_banned`           | Histogram | Histogram of calls to handleClearBanned in the rpc service          |
| `teranode_rpc_freeze`                 | Histogram | Histogram of calls to handleFreeze in the rpc service               |
| `teranode_rpc_unfreeze`               | Histogram | Histogram of calls to handleUnfreeze in the rpc service             |
| `teranode_rpc_reassign`               | Histogram | Histogram of calls to handleReassign in the rpc service             |
| `teranode_rpc_get_chaintips`          | Histogram | Histogram of calls to handleGetChainTips in the rpc service         |

## Subtree Validation Service Metrics

| Metric Name                                                 | Type      | Description                                       |
|-------------------------------------------------------------|-----------|---------------------------------------------------|
| `teranode_subtreevalidation_health`                         | Histogram | Histogram of calls to health endpoint             |
| `teranode_subtreevalidation_check_subtree`                  | Histogram | Duration of calls to checkSubtree endpoint        |
| `teranode_subtreevalidation_validate_subtree`               | Histogram | Histogram of subtrees validated                   |
| `teranode_subtreevalidation_validate_subtree_retry`         | Counter   | Number of retries when subtrees validated         |
| `teranode_subtreevalidation_validate_subtree_handler`       | Histogram | Duration of subtree handler                       |
| `teranode_subtreevalidation_validate_subtree_duration`      | Histogram | Duration of validate subtree                      |
| `teranode_subtreevalidation_bless_missing_transaction`      | Histogram | Duration of bless missing transaction             |
| `teranode_subtreevalidation_set_tx_meta_cache_kafka`        | Histogram | Duration of setting tx meta cache from kafka      |
| `teranode_subtreevalidation_del_tx_meta_cache_kafka`        | Histogram | Duration of deleting tx meta cache from kafka     |
| `teranode_subtreevalidation_set_tx_meta_cache_kafka_errors` | Counter   | Number of errors setting tx meta cache from kafka |

## Validator Service Metrics

| Metric Name                                | Type      | Description                                                             |
|--------------------------------------------|-----------|-------------------------------------------------------------------------|
| `teranode_validator_health`                | Counter   | Number of calls to the health endpoint                                  |
| `teranode_validator_invalid_transactions`  | Counter   | Number of transactions found invalid by the validator service           |
| `teranode_validator_transactions_validate_total` | Histogram | Histogram of total transaction validation                               |
| `teranode_validator_transactions_validate` | Histogram | Histogram of transaction validation                                     |
| `teranode_validator_transactions_extend`   | Histogram | Histogram of transaction extension operations                           |
| `teranode_validator_transactions_validate_scripts` | Histogram | Histogram of transaction script validation                              |
| `teranode_validator_transactions_validate_batch` | Histogram | Histogram of transaction batch validation                               |
| `teranode_validator_transactions_spend_utxos` | Histogram | Histogram of transaction spending utxos                                 |
| `teranode_validator_transactions_input_block_heights` | Histogram | Histogram of transaction input block heights                            |
| `teranode_validator_transactions_2phase_commit` | Histogram | Histogram of 2-phase commit operations                                  |
| `teranode_validator_transactions`          | Histogram | Histogram of transaction processing by the validator service            |
| `teranode_validator_transactions_size`     | Histogram | Size of transactions processed by the validator service                 |
| `teranode_validator_send_to_block_assembly`        | Histogram | Histogram of sending transactions to block assembly           |
| `teranode_validator_send_to_blockvalidation_kafka` | Histogram | Histogram of sending transactions to block validation kafka   |
| `teranode_validator_send_to_p2p_kafka`             | Histogram | Histogram of sending rejected transactions to p2p kafka       |
| `teranode_validator_set_tx_meta`                   | Histogram | Histogram of validator set tx meta                            |

## TxMetaCache Service Metrics

| Metric Name                                   | Type  | Description                                                  |
|-----------------------------------------------|-------|--------------------------------------------------------------|
| `teranode_tx_meta_cache_size`                 | Gauge | Number of items in the tx meta cache (commented out in code, not emitted) |
| `teranode_tx_meta_cache_insertions`           | Gauge | Number of insertions into the tx meta cache                  |
| `teranode_tx_meta_cache_hits`                 | Gauge | Number of hits in the tx meta cache                          |
| `teranode_tx_meta_cache_misses`               | Gauge | Number of misses in the tx meta cache                        |
| `teranode_tx_meta_cache_get_origin`           | Gauge | Number of get origins in the tx meta cache                   |
| `teranode_tx_meta_cache_evictions`            | Gauge | Number of evictions in the tx meta cache                     |
| `teranode_tx_meta_cache_trims`                | Gauge | Number of trim operations in the tx meta cache               |
| `teranode_tx_meta_cache_map_size`             | Gauge | Number of total elements in the improved cache's bucket maps |
| `teranode_tx_meta_cache_total_elements_added` | Gauge | Number of total number of elements added to the txmetacache  |
| `teranode_tx_meta_cache_hit_old_tx`           | Gauge | Number of hits on old txs in the tx meta cache              |
| `teranode_tx_meta_cache_valid_entries_count`  | Gauge | Number of valid (readable) entries in the tx meta cache     |
| `teranode_tx_meta_cache_current_gen_entries`  | Gauge | Number of entries in the current generation of the tx meta cache |
| `teranode_tx_meta_cache_previous_gen_entries` | Gauge | Number of valid entries from the previous generation in the tx meta cache |

## Aerospike Service Metrics

CounterVec metrics use labels: `function` (function name), `error` (error type).

| Metric Name                                       | Type       | Description                                                     |
|---------------------------------------------------|------------|-----------------------------------------------------------------|
| `teranode_aerospike_txmeta_get`                   | Counter    | Number of txmeta get calls done to aerospike                    |
| `teranode_aerospike_utxo_store`                   | Counter    | Number of Create calls done to aerospike                        |
| `teranode_aerospike_txmeta_errors`                | CounterVec | Number of txmeta map errors                                     |
| `teranode_aerospike_txmeta_get_multi`             | Counter    | Number of txmeta get_multi calls done to aerospike map          |
| `teranode_aerospike_txmeta_get_multi_n`           | Counter    | Number of txmeta get_multi txs done to aerospike map            |
| `teranode_aerospike_txmeta_set_mined_batch`       | Counter    | Number of txmeta set_mined_batch calls done to aerospike map    |
| `teranode_aerospike_txmeta_set_mined_batch_n`     | Counter    | Number of txmeta set_mined_batch txs done to aerospike map      |
| `teranode_aerospike_txmeta_set_mined_batch_err_n` | Counter    | Number of txmeta set_mined_batch txs errors to aerospike map    |
| `teranode_aerospike_utxo_get`                     | Counter    | Number of utxo get calls done to aerospike                      |
| `teranode_aerospike_utxo_spend`                   | Counter    | Number of utxo spend calls done to aerospike                    |
| `teranode_aerospike_utxo_reset`                   | Counter    | Number of utxo reset calls done to aerospike                    |
| `teranode_aerospike_utxo_delete`                  | Counter    | Number of utxo delete calls done to aerospike                   |
| `teranode_aerospike_utxo_errors`                  | CounterVec | Number of utxo errors                                           |
| `teranode_aerospike_utxo_create_batch`            | Histogram  | Duration of utxo create batch                                   |
| `teranode_aerospike_utxo_create_batch_size`       | Histogram  | Size of utxo create batch                                       |
| `teranode_aerospike_utxo_spend_batch`             | Histogram  | Duration of utxo spend batch                                    |
| `teranode_aerospike_utxo_spend_batch_size`        | Histogram  | Size of utxo spend batch                                        |
| `teranode_aerospike_get_external`                 | Histogram  | Duration of getting an external transaction from the blob store |
| `teranode_aerospike_set_external`                 | Histogram  | Duration of setting an external transaction to the blob store   |
| `teranode_aerospike_txmeta_get_counter_conflicting` | Histogram  | Duration of getting counter conflicting transactions from the blob store |
| `teranode_aerospike_txmeta_get_conflicting`        | Histogram  | Duration of getting conflicting transactions from the blob store |

## SQL Service Metrics

CounterVec metrics use labels: `function` (function name), `error` (error type).

| Metric Name                | Type       | Description                             |
|----------------------------|------------|-----------------------------------------|
| `teranode_sql_utxo_get`    | Counter    | Number of utxo get calls done to sql    |
| `teranode_sql_utxo_spend`  | Counter    | Number of utxo spend calls done to sql  |
| `teranode_sql_utxo_reset`  | Counter    | Number of utxo reset calls done to sql  |
| `teranode_sql_utxo_delete` | Counter    | Number of utxo delete calls done to sql |
| `teranode_sql_utxo_errors` | CounterVec | Number of utxo errors                   |
| `teranode_sql_utxo_get_counter_conflicting` | Histogram  | Histogram of utxo get counter conflicting calls done to sql |
| `teranode_sql_utxo_get_conflicting`         | Histogram  | Histogram of utxo get conflicting calls done to sql         |

## Subtree Processor Service Metrics

| Metric Name                                              | Type      | Description                                                       |
|----------------------------------------------------------|-----------|-------------------------------------------------------------------|
| `teranode_subtreeprocessor_add_tx`                       | Counter   | Number of times a tx is added in subtree processor                |
| `teranode_subtreeprocessor_move_forward`                 | Counter   | Number of times a block is moved up in subtree processor          |
| `teranode_subtreeprocessor_move_forward_duration`        | Histogram | Histogram of moving up block in subtree processor                 |
| `teranode_subtreeprocessor_move_back`                    | Counter   | Number of times a block is moved down in subtree processor        |
| `teranode_subtreeprocessor_move_back_duration`           | Histogram | Histogram of moving down block in subtree processor               |
| `teranode_subtreeprocessor_process_coinbase_tx`          | Counter   | Number of times a coinbase tx is processed in subtree processor   |
| `teranode_subtreeprocessor_process_coinbase_tx_duration` | Histogram | Duration of processing coinbase tx in subtree processor           |
| `teranode_subtreeprocessor_transaction_map`              | Counter   | Number of times a transaction map is created in subtree processor |
| `teranode_subtreeprocessor_transaction_map_duration`     | Histogram | Duration of creating transaction map in subtree processor         |
| `teranode_subtreeprocessor_remove_tx`                    | Histogram | Duration of removing tx in subtree processor                      |
| `teranode_subtreeprocessor_reset`                        | Histogram | Duration of resetting subtree processor                           |
| `teranode_subtreeprocessor_dynamic_subtree_size`         | Gauge     | Size of the dynamic subtree in the subtree processor              |
| `teranode_subtreeprocessor_current_state`                | Gauge     | Current state of the subtree processor                           |
