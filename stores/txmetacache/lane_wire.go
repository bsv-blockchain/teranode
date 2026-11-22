package txmetacache

import (
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lane positions on the txmeta topic, for the Phase 1 soak at utxostore_laneMigration shadow. The
// validator sends an entry in the lane metadata encoding (WireActionADDLane) when it knows the
// transaction's own lane position and every parent's, and a plain ADD entry otherwise, counting why.
// A receiver takes both. Nothing decides on a lane position yet, so a missing one is counted, never
// an error.

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
			Help:      "Txmeta entries for created transactions, by side (sent by the validator, or received by the named consumer) and format (lane: own and parent lane positions; legacy: none). The validator counts a sent entry only while its own utxostore_laneMigration is shadow or later; a consumer counts every ADDLane entry it receives, even with its own setting off, and a plain ADD entry only while its own setting is shadow or later; a malformed ADDLane entry still counts as format=lane",
		},
		[]string{"side", "format"},
	)

	prometheusLaneTxMetaMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txmeta_position_misses_total",
			Help:      "Txmeta entries that carry no lane positions, by reason; counted, never an error. The validator counts its reasons only at utxostore_laneMigration shadow or later; decode_failed is counted by any consumer that receives an ADDLane entry it cannot decode, whatever its own setting",
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

// AppendLaneCacheContent appends the txmeta cache form of a WireActionADDLane entry, its MetaBytes
// and own lane position (meta.CacheBytesInto), to dst, for a receiver at shadow whose cache keeps
// the position for the block gate. The form is shorter than the entry, so a dst with room for the
// whole message never grows. An entry that does not check out is counted as a miss
// (LaneMissDecode) and returned as an error, with dst unchanged.
func AppendLaneCacheContent(dst, content []byte) ([]byte, error) {
	out, err := meta.AppendCacheFromLaneMetaBytes(dst, content)
	if err != nil {
		prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode).Inc()
		return dst, err
	}

	return out, nil
}
