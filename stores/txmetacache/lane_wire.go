package txmetacache

import (
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lane positions on the txmeta topic, for the Phase 1 soak at utxostore_laneMigration shadow. The
// validator sends an entry in the lane metadata encoding (WireActionADDLane) when it knows the
// transaction's own lane position and every parent's, and a plain ADD entry otherwise, counting why.
// A receiver takes both. Nothing reads the positions yet, so a missing one is counted, never an error.

// The reasons a txmeta entry goes without lane positions.
const (
	// LaneMissOwn: the record has no valid lane position of its own.
	LaneMissOwn = "own_missing"
	// LaneMissParentsNotRead: the caller read no parent positions for a transaction that has
	// parents, as on the list path, which reads parents itself, and an outpoint-only spend.
	LaneMissParentsNotRead = "parents_not_read"
	// LaneMissParent: a parent read gave no valid position, as for a parent created before lane
	// positions were on.
	LaneMissParent = "parent_missing"
	// LaneMissEncode: the lane encoding refused the positions.
	LaneMissEncode = "encode_failed"
	// LaneMissDecode: a receiver could not decode a lane entry.
	LaneMissDecode = "decode_failed"
)

var (
	prometheusLaneTxMetaMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txmeta_messages_total",
			Help:      "Txmeta entries for created transactions at utxostore_laneMigration shadow, by side (sent by the validator, or received by the named consumer) and format (lane: own and parent lane positions; legacy: none); nothing is counted with lane positions off",
		},
		[]string{"side", "format"},
	)

	prometheusLaneTxMetaMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txmeta_position_misses_total",
			Help:      "Txmeta entries at utxostore_laneMigration shadow that carry no lane positions, by reason; counted, never an error",
		},
		[]string{"reason"},
	)
)

// CountLaneTxMetaSent counts one txmeta entry the validator sent at shadow: in the lane format when
// reason is "", otherwise in the legacy format for reason.
func CountLaneTxMetaSent(reason string) {
	if reason == "" {
		prometheusLaneTxMetaMessages.WithLabelValues("sent", "lane").Inc()
		return
	}

	prometheusLaneTxMetaMessages.WithLabelValues("sent", "legacy").Inc()
	prometheusLaneTxMetaMisses.WithLabelValues(reason).Inc()
}

// LaneTxMetaReceived counts the ADD and ADDLane entries one consumer receives, by format. The
// counters are resolved once, so a count is one atomic add.
type LaneTxMetaReceived struct {
	lane, legacy prometheus.Counter
}

// NewLaneTxMetaReceived returns the counters of consumer.
func NewLaneTxMetaReceived(consumer string) *LaneTxMetaReceived {
	return &LaneTxMetaReceived{
		lane:   prometheusLaneTxMetaMessages.WithLabelValues(consumer, "lane"),
		legacy: prometheusLaneTxMetaMessages.WithLabelValues(consumer, "legacy"),
	}
}

// Count counts one entry, in the lane format or the legacy one.
func (c *LaneTxMetaReceived) Count(laneFormat bool) {
	if laneFormat {
		c.lane.Inc()
	} else {
		c.legacy.Inc()
	}
}

// LaneMetaContent returns the MetaBytes a WireActionADDLane entry carries, which a plain ADD entry
// would carry, as a sub-slice of content with nothing copied or allocated (meta.LaneMetaBytesView).
// An entry that does not check out is counted as a miss (LaneMissDecode) and returned as an error,
// for the receiver to drop like any malformed entry.
func LaneMetaContent(content []byte) ([]byte, error) {
	legacy, err := meta.LaneMetaBytesView(content)
	if err != nil {
		prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode).Inc()
		return nil, err
	}

	return legacy, nil
}
