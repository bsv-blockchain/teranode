package netsync

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	prometheusLegacyNetsyncBlockHeight               prometheus.Gauge
	prometheusLegacyNetsyncHandleTxMsg               prometheus.Histogram
	prometheusLegacyNetsyncHandleTxMsgValidate       prometheus.Histogram
	prometheusLegacyNetsyncProcessOrphanTransactions prometheus.Histogram
	prometheusLegacyNetsyncHandleBlockDirect         prometheus.Histogram
	prometheusLegacyNetsyncProcessBlock              prometheus.Histogram
	prometheusLegacyNetsyncOrphans                   prometheus.Gauge
	prometheusLegacyNetsyncParkedBlocks              prometheus.Gauge
	prometheusLegacyNetsyncParkedBytes               prometheus.Gauge
	prometheusLegacyNetsyncOrphanTime                prometheus.Histogram

	// prometheusLegacyNetsyncFrontierRaces counts blocks asked of a second peer because the
	// chain was about to wait on them from a slow one. See frontier_race.go.
	prometheusLegacyNetsyncFrontierRaces prometheus.Counter

	// The download side, published from the 30-second queue report
	// (publishDownloadMetrics in frontier_race.go). Until these existed the only
	// view of the download was that report's log line.
	prometheusLegacyNetsyncBlocksOwed         prometheus.Gauge
	prometheusLegacyNetsyncHeaderCacheHeights prometheus.Gauge
	prometheusLegacyNetsyncBytesAhead         prometheus.Gauge
	prometheusLegacyNetsyncParkBackstopBytes  prometheus.Gauge

	// The downloadWaste counters, fed from its atomics by downloadWaste.publish.
	prometheusLegacyNetsyncDownloadReceivedBytes     prometheus.Counter
	prometheusLegacyNetsyncDownloadDupDrained        prometheus.Counter
	prometheusLegacyNetsyncDownloadDupConverted      prometheus.Counter
	prometheusLegacyNetsyncDownloadLocalFaultDrained prometheus.Counter
	prometheusLegacyNetsyncDownloadStreamsCut        prometheus.Counter
	prometheusLegacyNetsyncDownloadBytesWasted       prometheus.Counter
	prometheusLegacyNetsyncDownloadPeersDroppedOwing prometheus.Counter
	prometheusLegacyNetsyncDownloadBlocksOwedAtDrop  prometheus.Counter
	prometheusLegacyNetsyncDownloadBlocksReasked     prometheus.Counter

	// prometheusLegacyNetsyncParkDispositions counts applyParkDisposition calls by
	// the row's name. See block_park_policy.go.
	prometheusLegacyNetsyncParkDispositions *prometheus.CounterVec

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusLegacyNetsyncFrontierRaces = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "frontier_races_total",
		Help:      "Blocks asked of a second peer because the chain was about to wait on them from a slow one",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncFrontierRaces)

	prometheusLegacyNetsyncBlockHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_height",
		Help:      "The height of the block being processed",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockHeight)

	prometheusLegacyNetsyncHandleTxMsg = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg",
		Help:      "The time taken to handle a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsg)

	prometheusLegacyNetsyncHandleTxMsgValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg_validate",
		Help:      "The time taken to validate a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsgValidate)

	prometheusLegacyNetsyncProcessOrphanTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_orphan_transactions",
		Help:      "The time taken to process orphan transactions",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessOrphanTransactions)

	prometheusLegacyNetsyncHandleBlockDirect = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_block_direct",
		Help:      "The time taken to commit a converted block (HandleConvertedBlock); the name predates the converted-record route",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleBlockDirect)

	prometheusLegacyNetsyncProcessBlock = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_block",
		Help:      "The time taken to process a block",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessBlock)

	prometheusLegacyNetsyncOrphans = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphans",
		Help:      "The number of orphan transactions",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphans)

	prometheusLegacyNetsyncParkedBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "parked_blocks",
		Help:      "The number of downloaded blocks held on disk waiting for their parent",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncParkedBlocks)

	prometheusLegacyNetsyncParkedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "parked_bytes",
		Help:      "Bytes the park bills for the blocks it holds: a block parked off the wire at its wire size, a block recovered from disk after a restart at its converted record's size; bytes_ahead is what the backstop reads",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncParkedBytes)

	prometheusLegacyNetsyncOrphanTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphan_time",
		Help:      "The time taken to process an orphan transaction",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphanTime)

	prometheusLegacyNetsyncBlocksOwed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "blocks_owed",
		Help:      "Blocks asked of peers and not yet delivered, every peer together, including blocks arriving now",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlocksOwed)

	prometheusLegacyNetsyncHeaderCacheHeights = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "header_cache_heights",
		Help:      "Heights the headers-first header cache names, the header run the download pass picks blocks from below the last checkpoint",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHeaderCacheHeights)

	prometheusLegacyNetsyncBytesAhead = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "bytes_ahead",
		Help:      "Block bytes held ahead of the chain, parked plus arriving, which the download measures against park_backstop_bytes",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBytesAhead)

	prometheusLegacyNetsyncParkBackstopBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "park_backstop_bytes",
		Help:      "The bytes_ahead figure at which no block further ahead is asked for until the chain catches up; a constant",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncParkBackstopBytes)

	prometheusLegacyNetsyncDownloadReceivedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_received_bytes_total",
		Help:      "Block body bytes read off the wire",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadReceivedBytes)

	prometheusLegacyNetsyncDownloadDupDrained = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_duplicate_copies_drained_total",
		Help:      "Block copies drained unwritten because another copy of the same block was converting",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadDupDrained)

	prometheusLegacyNetsyncDownloadDupConverted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_duplicate_copies_converted_total",
		Help:      "Block copies converted in full for a block already parked",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadDupConverted)

	prometheusLegacyNetsyncDownloadLocalFaultDrained = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_local_fault_copies_drained_total",
		Help:      "Block copies drained because this node failed to store the block; the peer was kept and the block asked for again",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadLocalFaultDrained)

	prometheusLegacyNetsyncDownloadStreamsCut = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_streams_cut_total",
		Help:      "Block bodies cut part way through",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadStreamsCut)

	prometheusLegacyNetsyncDownloadBytesWasted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_bytes_wasted_total",
		Help:      "Bytes of block bodies cut part way through and of every copy drained unwritten, duplicate or local fault",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadBytesWasted)

	prometheusLegacyNetsyncDownloadPeersDroppedOwing = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_peers_dropped_owing_total",
		Help:      "Peers that left while still owing blocks",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadPeersDroppedOwing)

	prometheusLegacyNetsyncDownloadBlocksOwedAtDrop = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_blocks_owed_at_drop_total",
		Help:      "Blocks the peers counted by download_peers_dropped_owing_total still owed when they left",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadBlocksOwedAtDrop)

	prometheusLegacyNetsyncDownloadBlocksReasked = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "download_blocks_reasked_total",
		Help:      "Blocks made askable of another peer because the peers owing them sent no block bytes for the retry window",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncDownloadBlocksReasked)

	// The backstop is a constant, published once so a dashboard can divide by it.
	prometheusLegacyNetsyncParkBackstopBytes.Set(float64(parkBackstopBytes))

	prometheusLegacyNetsyncParkDispositions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "park_dispositions_total",
		Help:      "Every disposition applied to a parked block, by the park policy row's name: a commit, a read or commit failure, a record whose files are gone, and a sweep or recovery eviction",
	}, []string{"disposition"})
	prometheus.MustRegister(prometheusLegacyNetsyncParkDispositions)

	// Every row starts at zero, so rate() has a baseline before the first block
	// of each kind and a row that never fires still shows as zero, not absent.
	for _, d := range parkDispositionRows {
		prometheusLegacyNetsyncParkDispositions.WithLabelValues(d.name)
	}
}
