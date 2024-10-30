package tracing

import (
	"fmt"
	"sync/atomic"

	"github.com/ordishs/go-utils/safemap"
	"github.com/prometheus/client_golang/prometheus"
)

type PeerHandlerStats struct {
	TransactionSent         atomic.Uint64
	TransactionAnnouncement atomic.Uint64
	TransactionRejection    atomic.Uint64
	TransactionGet          atomic.Uint64
	Transaction             atomic.Uint64
	BlockAnnouncement       atomic.Uint64
	Block                   atomic.Uint64
	BlockProcessingMs       atomic.Uint64
}

func NewPeerHandlerStats() *PeerHandlerStats {
	return &PeerHandlerStats{
		TransactionSent:         atomic.Uint64{},
		TransactionAnnouncement: atomic.Uint64{},
		TransactionRejection:    atomic.Uint64{},
		TransactionGet:          atomic.Uint64{},
		Transaction:             atomic.Uint64{},
		BlockAnnouncement:       atomic.Uint64{},
		Block:                   atomic.Uint64{},
		BlockProcessingMs:       atomic.Uint64{},
	}
}

type PeerHandlerCollector struct {
	service                 string
	stats                   *safemap.Safemap[string, *PeerHandlerStats]
	transactionSent         *prometheus.Desc
	transactionAnnouncement *prometheus.Desc
	transactionRejection    *prometheus.Desc
	transactionGet          *prometheus.Desc
	transaction             *prometheus.Desc
	blockAnnouncement       *prometheus.Desc
	block                   *prometheus.Desc
	blockProcessingMs       *prometheus.Desc
}

// NewPeerHandlerCollector initializes every descriptor and returns a pointer to the prometheusCollector
func NewPeerHandlerCollector(service string, stats *safemap.Safemap[string, *PeerHandlerStats]) *PeerHandlerCollector {
	c := &PeerHandlerCollector{
		service: service,
		stats:   stats,
		transactionSent: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_transaction_sent_count", service),
			"Shows the number of transactions marked as sent by the peer handler",
			[]string{"peer"}, nil,
		),
		transactionAnnouncement: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_transaction_announcement_count", service),
			"Shows the number of transactions announced by the peer handler",
			[]string{"peer"}, nil,
		),
		transactionRejection: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_transaction_rejection_count", service),
			"Shows the number of transactions rejected by the peer handler",
			[]string{"peer"}, nil,
		),
		transactionGet: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_transaction_get_count", service),
			"Shows the number of transactions get by the peer handler",
			[]string{"peer"}, nil,
		),
		transaction: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_transaction_count", service),
			"Shows the number of transactions received by the peer handler",
			[]string{"peer"}, nil,
		),
		blockAnnouncement: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_block_announcement_count", service),
			"Shows the number of blocks announced by the peer handler",
			[]string{"peer"}, nil,
		),
		block: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_block_count", service),
			"Shows the number of blocks received by the peer handler",
			[]string{"peer"}, nil,
		),
		blockProcessingMs: prometheus.NewDesc(fmt.Sprintf("teranode_%s_peer_block_processing_ms", service),
			"Shows the total time spent processing blocks by the peer handler",
			[]string{"peer"}, nil,
		),
	}

	prometheus.MustRegister(c)

	return c
}

// Describe writes all descriptors to the prometheus desc channel.
func (c *PeerHandlerCollector) Describe(ch chan<- *prometheus.Desc) {

	//Update this section with each metric you create for a given prometheusCollector
	ch <- c.transactionSent
	ch <- c.transactionAnnouncement
	ch <- c.transactionRejection
	ch <- c.transactionGet
	ch <- c.transaction
	ch <- c.blockAnnouncement
	ch <- c.block
	ch <- c.blockProcessingMs
}

// Collect implements required collect function for all prometheus collectors
func (c *PeerHandlerCollector) Collect(ch chan<- prometheus.Metric) {
	//Write the latest value for each metric in the prometheus metric channel.
	//Note that you can pass CounterValue, GaugeValue, or UntypedValue types here.
	c.stats.Each(func(peer string, peerStats *PeerHandlerStats) {
		ch <- prometheus.MustNewConstMetric(c.transactionSent, prometheus.CounterValue,
			float64(peerStats.TransactionSent.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.transactionAnnouncement, prometheus.CounterValue,
			float64(peerStats.TransactionAnnouncement.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.transactionRejection, prometheus.CounterValue,
			float64(peerStats.TransactionRejection.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.transactionGet, prometheus.CounterValue,
			float64(peerStats.TransactionGet.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.transaction, prometheus.CounterValue,
			float64(peerStats.Transaction.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.blockAnnouncement, prometheus.CounterValue,
			float64(peerStats.BlockAnnouncement.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.block, prometheus.CounterValue,
			float64(peerStats.Block.Load()), peer)
		ch <- prometheus.MustNewConstMetric(c.blockProcessingMs, prometheus.CounterValue,
			float64(peerStats.BlockProcessingMs.Load()), peer)
	})
}
