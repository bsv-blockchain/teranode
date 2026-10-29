package collect

import (
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
)

const (
	metricPropagationTx      = "teranode_propagation_transactions"
	metricPropagationInvalid = "teranode_propagation_invalid_transactions"
	metricPeers              = "teranode_p2p_connected_peers"
	metricSubtreeIntake      = "teranode_subtreevalidation_validate_subtree_handler"
	metricSubtreeWaiting     = "teranode_subtreevalidation_peer_subtree_admission_waiting"

	metricValidatorTx      = "teranode_validator_transactions_validate"
	metricValidatorInvalid = "teranode_validator_invalid_transactions"
	metricSpendBatchSize   = "teranode_aerospike_utxo_spend_batch_size"

	metricCacheHits          = "teranode_tx_meta_cache_hits"
	metricCacheMisses        = "teranode_tx_meta_cache_misses"
	metricCacheEvictions     = "teranode_tx_meta_cache_evictions"
	metricCacheInsertions    = "teranode_tx_meta_cache_insertions"
	metricCacheValidEntries  = "teranode_tx_meta_cache_valid_entries_count"
	metricCacheCurrentGen    = "teranode_tx_meta_cache_current_gen_entries"
	metricCachePreviousGen   = "teranode_tx_meta_cache_previous_gen_entries"
	metricCacheBytes         = "teranode_tx_meta_cache_bytes"
	metricCacheCapacityBytes = "teranode_tx_meta_cache_capacity_bytes"

	metricAssemblyState   = "teranode_blockassembly_current_state"
	metricAssemblyTipLag  = "teranode_blockassembly_tip_lag_blocks"
	metricAssemblyHeadAge = "teranode_blockassembly_queue_head_age_seconds"
	metricAssemblyQueued  = "teranode_blockassembly_queued_transactions"
	metricAssemblyTxCount = "teranode_blockassembly_transactions"

	metricForkLongestDepth      = "teranode_blockvalidation_fork_longest_depth"
	metricSubscriberPendingFull = "teranode_blockchain_subscriber_pending_full_total"
	metricSubscriberSendErrors  = "teranode_blockchain_subscriber_send_errors_total"
	metricBlockchainAddBlock    = "teranode_blockchain_add_block"

	metricSubtreeSkipped   = "teranode_subtreevalidation_subtree_already_exists_skipped_total"
	metricSubtreeValidated = "teranode_subtreevalidation_validate_subtree"
	metricBlessMissing     = "teranode_subtreevalidation_bless_missing_transaction"
	metricBlockFound       = "teranode_blockvalidation_block_found"
	metricBlockValidated   = "teranode_blockvalidation_validate_block"
	metricMiningCandidates = "teranode_blockassembly_block_assembler_get_mining_candidate"

	metricMiningCandidateTime     = "teranode_blockassembly_last_mining_candidate_timestamp_seconds"
	metricValidatorExisting       = "teranode_validator_existing_tx_total"
	metricBlockMissingSubtrees    = "teranode_subtreevalidation_block_missing_subtrees_total"
	metricBlockMissingOutstanding = "teranode_subtreevalidation_block_missing_subtrees_outstanding"

	metricBlobReads         = "teranode_blob_reads_total"
	metricBlobWrites        = "teranode_blob_writes_total"
	metricBlobErrors        = "teranode_blob_errors_total"
	metricBlobReadDuration  = "teranode_blob_read_duration_seconds"
	metricBlobWriteDuration = "teranode_blob_write_duration_seconds"
	metricMiningSolutions   = "teranode_blockassembly_submit_mining_solution"

	metricAerospikeCreate = "teranode_aerospike_utxo_store"
	metricAerospikeSpend  = "teranode_aerospike_utxo_spend"
)

const (
	noteNoData          = "no data"
	noteValidatedCounts = "includes block-validation checks and rejected transactions"
	noteUnbounded       = "unbounded"

	assemblyMaxBatchesPerIteration = 64

	cacheRateWindow = 10 * time.Second
)

var aerospikeReadCounters = []string{
	"teranode_aerospike_txmeta_get",
	"teranode_aerospike_txmeta_get_multi_n",
	"teranode_aerospike_utxo_get",
}

var aerospikeWriteCounters = []string{
	metricAerospikeCreate,
	metricAerospikeSpend,
	"teranode_aerospike_txmeta_set_mined_batch_n",
	"teranode_aerospike_utxo_reset",
	"teranode_aerospike_utxo_delete",
}

var MetricNames = append([]string{
	metricPropagationTx,
	metricPropagationInvalid,
	metricPeers,
	metricSubtreeIntake,
	metricSubtreeWaiting,
	metricValidatorTx,
	metricValidatorInvalid,
	metricSpendBatchSize,
	metricCacheHits,
	metricCacheMisses,
	metricCacheEvictions,
	metricCacheValidEntries,
	metricCacheCurrentGen,
	metricCachePreviousGen,
	metricCacheBytes,
	metricCacheCapacityBytes,
	metricAssemblyState,
	metricAssemblyTipLag,
	metricAssemblyHeadAge,
	metricAssemblyQueued,
	metricAssemblyTxCount,
	metricSubtreeSkipped,
	metricSubtreeValidated,
	metricBlessMissing,
	metricBlockFound,
	metricBlockValidated,
	metricMiningCandidates,
	metricMiningCandidateTime,
	metricMiningSolutions,
	metricValidatorExisting,
	metricBlockMissingSubtrees,
	metricBlockMissingOutstanding,
	metricBlobReads,
	metricBlobWrites,
	metricBlobErrors,
	metricBlobReadDuration,
	metricBlobWriteDuration,
	metricForkLongestDepth,
	metricSubscriberPendingFull,
	metricSubscriberSendErrors,
	metricBlockchainAddBlock,
}, append(append([]string{}, aerospikeReadCounters...), aerospikeWriteCounters...)...)

func Apply(s *snapshot.Snapshot, m MetricSet, r *Rates, cfg *settings.Settings) {
	applyIngress(s, m, r)
	applyValidator(s, r, cfg)
	applyCache(s, m, r, cfg)
	applyAssembly(s, m, cfg)
	applyChain(s, m)
	applyStores(s, r)
	applyLanes(s, r)
	applyBlobs(s, r)
}

func applyIngress(s *snapshot.Snapshot, m MetricSet, r *Rates) {
	set(&s.Ingress.PropagationTxPerSec, rate(r, metricPropagationTx))
	set(&s.Ingress.SubtreeIntakePerSec, rate(r, metricSubtreeIntake))
	set(&s.Ingress.Peers, gauge(m, metricPeers))
	set(&s.Ingress.RejectedPerSec, rate(r, metricPropagationInvalid))
	set(&s.Ingress.SubtreeQueue, gauge(m, metricSubtreeWaiting))
	set(&s.Ingress.SubtreeQueueOldestSec, noData(noteNoData))
	setFlow(s, snapshot.FlowTxInput, rate(r, metricPropagationTx))
}

func applyValidator(s *snapshot.Snapshot, r *Rates, cfg *settings.Settings) {
	validated := rate(r, metricValidatorTx)
	if validated.Status != snapshot.StatusNoData {
		validated.Note = noteValidatedCounts
	}

	set(&s.Validator.ValidatedPerSec, validated)
	set(&s.Validator.ErrorsPerSec, rate(r, metricValidatorInvalid))
	set(&s.Validator.QueueFill, noData(noteNoData))
	setFlow(s, snapshot.FlowValidated, validated)

	if p99, ok := r.Quantile(metricValidatorTx, 0.99); ok {
		set(&s.Validator.LatencyP99Sec, okReading(p99))
	} else {
		set(&s.Validator.LatencyP99Sec, noData(noteNoData))
	}

	mean, ok := r.Mean(metricSpendBatchSize)
	if !ok || cfg.UtxoStore.SpendBatcherSize <= 0 {
		set(&s.Validator.SpendBatchFill, noData(noteNoData))
		return
	}

	set(&s.Validator.SpendBatchFill, okReading(mean/float64(cfg.UtxoStore.SpendBatcherSize)))
}

func applyCache(s *snapshot.Snapshot, m MetricSet, r *Rates, cfg *settings.Settings) {
	hits, hitsOK := r.PerSecOver(metricCacheHits, cacheRateWindow)
	misses, missesOK := r.PerSecOver(metricCacheMisses, cacheRateWindow)

	switch {
	case !hitsOK || !missesOK:
		set(&s.Cache.HitRate, noData(noteNoData))
		set(&s.Cache.MissesPerSec, windowRate(r, metricCacheMisses))
		setFlow(s, snapshot.FlowLookups, noData(noteNoData))
	case hits+misses == 0:
		set(&s.Cache.HitRate, noData(noteNoData))
		set(&s.Cache.MissesPerSec, okReading(misses))
		setFlow(s, snapshot.FlowLookups, okReading(0))
	default:
		set(&s.Cache.HitRate, okReading(hits/(hits+misses)))
		set(&s.Cache.MissesPerSec, okReading(misses))
		setFlow(s, snapshot.FlowLookups, okReading(hits+misses))
	}

	set(&s.Cache.EvictionsPerSec, windowRate(r, metricCacheEvictions))
	set(&s.Cache.InsertionsPerSec, windowRate(r, metricCacheInsertions))
	bytes, bytesOK := m.Sum(metricCacheBytes)
	if bytesOK {
		set(&s.Cache.SizeGB, okReading(bytes/1e9))
	} else {
		set(&s.Cache.SizeGB, noData(noteNoData))
	}

	capacity, capacityOK := m.Sum(metricCacheCapacityBytes)

	switch {
	case capacityOK && capacity > 0:
		set(&s.Cache.CapacityGB, okReading(capacity/1e9))
	case cfg.SubtreeValidation.TxMetaCacheMaxMB > 0:
		set(&s.Cache.CapacityGB, okReading(float64(cfg.SubtreeValidation.TxMetaCacheMaxMB)/1024))
	default:
		set(&s.Cache.CapacityGB, noData(noteNoData))
	}

	if bytesOK && capacityOK && capacity > 0 {
		set(&s.Cache.Fill, okReading(bytes/capacity))
	} else {
		set(&s.Cache.Fill, noData(noteNoData))
	}

	valid, validOK := m.Sum(metricCacheValidEntries)
	if !validOK {
		set(&s.Cache.EntriesMillions, noData(noteNoData))
	} else {
		set(&s.Cache.EntriesMillions, okReading(valid/1e6))
	}

	set(&s.Cache.CurrentGenFrac, fraction(m, metricCacheCurrentGen, valid, validOK))
	set(&s.Cache.PreviousGenFrac, fraction(m, metricCachePreviousGen, valid, validOK))
}

func fraction(m MetricSet, name string, total float64, totalOK bool) snapshot.Reading {
	part, ok := m.Sum(name)
	if !ok || !totalOK || total <= 0 {
		return noData(noteNoData)
	}

	return okReading(part / total)
}

func applyAssembly(s *snapshot.Snapshot, m MetricSet, cfg *settings.Settings) {
	if state, ok := m.Sum(metricAssemblyState); ok {
		paused := okReading(1)
		if blockassembly.State(state) == blockassembly.StateRunning {
			paused = okReading(0)
		}

		paused.Note = blockassembly.StateStrings[blockassembly.State(state)]
		set(&s.Assembly.Paused, paused)
	} else {
		set(&s.Assembly.Paused, noData(noteNoData))
	}

	set(&s.Assembly.TipLagBlocks, gauge(m, metricAssemblyTipLag))
	set(&s.Assembly.QueueHeadAgeSec, gauge(m, metricAssemblyHeadAge))
	set(&s.Assembly.Transactions, gauge(m, metricAssemblyTxCount))
	set(&s.Assembly.SubtreeFill, noData(noteNoData))

	queued, ok := m.Sum(metricAssemblyQueued)

	switch capacity := assemblyQueueCapacity(cfg); {
	case !ok:
		set(&s.Assembly.IngestQueueFill, noData(noteNoData))
	case capacity <= 0:
		set(&s.Assembly.IngestQueueFill, noData(noteUnbounded))
	default:
		set(&s.Assembly.IngestQueueFill, okReading(queued/float64(capacity)))
	}
}

func assemblyQueueCapacity(cfg *settings.Settings) int64 {
	maxItems := cfg.BlockAssembly.MaxQueueItems
	if maxItems <= 0 {
		return 0
	}

	return max(maxItems, int64(assemblyMaxBatchesPerIteration)*int64(cfg.BlockAssembly.SendBatchSize))
}

func applyLanes(s *snapshot.Snapshot, r *Rates) {
	set(&s.Lanes.Tx.AlreadySeenPerSec, rate(r, metricValidatorExisting))
	set(&s.Lanes.Subtrees.AlreadyKnownPerSec, rate(r, metricSubtreeSkipped))
	set(&s.Lanes.Subtrees.CompletedPerSec, rate(r, metricSubtreeValidated))
	set(&s.Lanes.Subtrees.MissingTxsPerSec, rate(r, metricBlessMissing))
	set(&s.Lanes.Subtrees.TxsFetchedAvg, totalRatio(r, metricBlessMissing, metricSubtreeValidated))
	set(&s.Lanes.Blocks.AnnouncedTotal, total(r, metricBlockFound))
	set(&s.Lanes.Blocks.ValidatedTotal, total(r, metricBlockValidated))
	set(&s.Lanes.Blocks.MissingSubtrees, total(r, metricBlockMissingOutstanding))
	set(&s.Lanes.Blocks.MissingSubtreesPerSec, rate(r, metricBlockMissingSubtrees))
	set(&s.Lanes.Mining.CandidatesPerSec, rate(r, metricMiningCandidates))
	set(&s.Lanes.Mining.CandidateAgeSec, ageSec(r, metricMiningCandidateTime))
	set(&s.Lanes.Mining.SolutionsTotal, total(r, metricMiningSolutions))
}

func total(r *Rates, name string) snapshot.Reading {
	return reading(r.Total(name))
}

func applyBlobs(s *snapshot.Snapshot, r *Rates) {
	applyBlobStore(&s.Blobs.Subtree, r, "subtree")
	applyBlobStore(&s.Blobs.Block, r, "block")
}

func applyBlobStore(v *snapshot.BlobStoreView, r *Rates, store string) {
	set(&v.ReadsPerSec, rate(r, StoreSeries(metricBlobReads, store)))
	set(&v.WritesPerSec, rate(r, StoreSeries(metricBlobWrites, store)))
	set(&v.ErrorsPerSec, rate(r, StoreSeries(metricBlobErrors, store)))
	set(&v.ReadP99Sec, quantile(r, StoreSeries(metricBlobReadDuration, store), 0.99))
	set(&v.WriteP99Sec, quantile(r, StoreSeries(metricBlobWriteDuration, store), 0.99))
}

func quantile(r *Rates, name string, q float64) snapshot.Reading {
	return reading(r.Quantile(name, q))
}

func ageSec(r *Rates, name string) snapshot.Reading {
	return reading(r.AgeSec(name))
}

func totalRatio(r *Rates, numerator, denominator string) snapshot.Reading {
	top, topOK := r.Total(numerator)
	bottom, bottomOK := r.Total(denominator)

	if !topOK || !bottomOK || bottom <= 0 {
		return noData(noteNoData)
	}

	return okReading(top / bottom)
}

func applyChain(s *snapshot.Snapshot, m MetricSet) {
	set(&s.Chain.ForkDepth, gauge(m, metricForkLongestDepth))

	pendingFull, pendingOK := m.Sum(metricSubscriberPendingFull)
	sendErrors, sendOK := m.Sum(metricSubscriberSendErrors)

	if !pendingOK && !sendOK {
		set(&s.Chain.SubscribersDropped, noData(noteNoData))
		return
	}

	set(&s.Chain.SubscribersDropped, okReading(pendingFull+sendErrors))
}

func applyStores(s *snapshot.Snapshot, r *Rates) {
	set(&s.Stores.Aerospike.ReadP99Sec, noData(noteNoData))
	set(&s.Stores.Aerospike.ReadsPerSec, sumRates(r, aerospikeReadCounters...))
	set(&s.Stores.Aerospike.WritesPerSec, sumRates(r, aerospikeWriteCounters...))
	setFlow(s, snapshot.FlowUTXOOps, sumRates(r, metricAerospikeSpend, metricAerospikeCreate))
	setFlow(s, snapshot.FlowBlockWrites, rate(r, metricBlockchainAddBlock))
}

func sumRates(r *Rates, names ...string) snapshot.Reading {
	var (
		total float64
		found bool
	)

	for _, name := range names {
		if v, ok := r.PerSec(name); ok {
			total += v
			found = true
		}
	}

	if !found {
		return noData(noteNoData)
	}

	return okReading(total)
}

func rate(r *Rates, name string) snapshot.Reading {
	return reading(r.PerSec(name))
}

func windowRate(r *Rates, name string) snapshot.Reading {
	return reading(r.PerSecOver(name, cacheRateWindow))
}

func gauge(m MetricSet, name string) snapshot.Reading {
	return reading(m.Sum(name))
}

func reading(v float64, ok bool) snapshot.Reading {
	if !ok {
		return noData(noteNoData)
	}

	return okReading(v)
}

func okReading(v float64) snapshot.Reading {
	return snapshot.Reading{Value: v, Status: snapshot.StatusOK}
}

func noData(note string) snapshot.Reading {
	return snapshot.Reading{Status: snapshot.StatusNoData, Note: note}
}

func set(dst *snapshot.Reading, v snapshot.Reading) {
	v.Normal = dst.Normal
	*dst = v
}

func setFlow(s *snapshot.Snapshot, id string, v snapshot.Reading) {
	if f := s.Flow(id); f != nil {
		set(&f.Rate, v)
	}
}
