package blockchain

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusBlockchainHealth                 prometheus.Counter
	prometheusBlockchainAddBlock               prometheus.Counter
	prometheusBlockchainGetBlock               prometheus.Counter
	prometheusBlockchainGetLastNBlocks         prometheus.Counter
	prometheusBlockchainGetSuitableBlock       prometheus.Counter
	prometheusBlockchainGetHashOfAncestorBlock prometheus.Counter
	prometheusBlockchainGetNextWorkRequired    prometheus.Counter
	prometheusBlockchainGetBlockExists         prometheus.Counter
	prometheusBlockchainGetBestBlockHeader     prometheus.Counter
	prometheusBlockchainGetBlockHeader         prometheus.Counter
	prometheusBlockchainGetBlockHeaders        prometheus.Counter
	prometheusBlockchainSubscribe              prometheus.Counter
	prometheusBlockchainGetState               prometheus.Counter
	prometheusBlockchainSetState               prometheus.Counter
	prometheusBlockchainSendNotification       prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockchainHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "health",
			Help:      "Number of calls to the health endpoint of the blockchain service",
		},
	)

	prometheusBlockchainAddBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "add_block",
			Help:      "Number of blocks added to the blockchain service",
		},
	)

	prometheusBlockchainGetBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_block",
			Help:      "Number of Get block calls to the blockchain service",
		},
	)

	prometheusBlockchainGetLastNBlocks = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_last_n_block",
			Help:      "Number of GetLastNBlocks calls to the blockchain service",
		},
	)

	prometheusBlockchainGetSuitableBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_suitable_block",
			Help:      "Number of GetSuitableBlock calls to the blockchain service",
		},
	)
	prometheusBlockchainGetHashOfAncestorBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_hash_of_ancestor_block",
			Help:      "Number of GetHashOfAncestorBlock calls to the blockchain service",
		},
	)
	prometheusBlockchainGetNextWorkRequired = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_next_work_required",
			Help:      "Number of GetNextWorkRequired calls to the blockchain service",
		},
	)

	prometheusBlockchainGetBlockExists = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_block_exists",
			Help:      "Number of GetBlockExists calls to the blockchain service",
		},
	)

	prometheusBlockchainGetBestBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_get_best_block_header",
			Help:      "Number of GetBestBlockHeader calls to the blockchain service",
		},
	)

	prometheusBlockchainGetBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_get_block_header",
			Help:      "Number of GetBlockHeader calls to the blockchain service",
		},
	)

	prometheusBlockchainGetBlockHeaders = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_get_block_headers",
			Help:      "Number of GetBlockHeaders calls to the blockchain service",
		},
	)

	prometheusBlockchainSubscribe = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "subscribe",
			Help:      "Number of Subscribe calls to the blockchain service",
		},
	)

	prometheusBlockchainGetState = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "get_state",
			Help:      "Number of GetState calls to the blockchain service",
		},
	)

	prometheusBlockchainSetState = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "set_state",
			Help:      "Number of SetState calls to the blockchain service",
		},
	)

	prometheusBlockchainSendNotification = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "send_notification",
			Help:      "Number of SendNotification calls to the blockchain service",
		},
	)
}
