package aerospike

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The slot probe answers false whenever it cannot prove the server records the position, and a
// store that probed false counts its spends as not carrying one. The server here runs a copy of
// teranode.lua with the slot write taken out, the shape of a server that predates the lanePos key.
func TestLaneSlotProbeAnswersFalseWithoutProof(t *testing.T) {
	// Not parallel: it reads the process-wide lane spend counts, which other tests' spends add to.
	s, client := setupStoreForLockTestOwnServer(t)
	ctx := t.Context()

	s.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	_, child := restoreTestTxs(t, s, 1)
	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	registerLua := func(t *testing.T, script []byte) {
		t.Helper()

		task, err := client.RegisterUDF(nil, script, LuaPackage+".lua", aerospike.LUA)
		require.NoError(t, err)
		require.NoError(t, <-task.OnComplete())
	}

	// spendAndCount spends child with a lane position and returns how much each carried count
	// rose by, then unspends so the next case starts from an unspent parent.
	spendAndCount := func(t *testing.T) (carried, missing float64) {
		t.Helper()

		yes, no := laneSpendCount(t, "yes"), laneSpendCount(t, "no")

		spent, err := s.SpendWithLanePosition(ctx, child, 2, lane.Pack(9, 9), utxo.IgnoreFlags{})
		require.NoError(t, err)
		require.NoError(t, s.Unspend(ctx, spent))

		return laneSpendCount(t, "yes") - yes, laneSpendCount(t, "no") - no
	}

	t.Run("a server that does not store the position", func(t *testing.T) {
		const slotWrite = "slotLanes = setSlotLanePos(slotLanes, offset, lanePos)"

		require.Contains(t, string(teranodeLUA), slotWrite)
		registerLua(t, []byte(strings.Replace(string(teranodeLUA), slotWrite, "", 1)))
		t.Cleanup(func() { registerLua(t, teranodeLUA) })

		s.laneSlotWrites.Store(s.probeLaneSlotWrites(ctx))
		require.False(t, s.LaneSlotPositionsRecorded(), "the probe read back no position")

		carried, missing := spendAndCount(t)
		require.InDelta(t, 0, carried, 0)
		require.InDelta(t, 1, missing, 0, "a spend on that server counts as not carrying its position")

		registerLua(t, teranodeLUA)

		s.laneSlotWrites.Store(s.probeLaneSlotWrites(ctx))
		require.True(t, s.LaneSlotPositionsRecorded(), "the real script passes the probe")

		carried, missing = spendAndCount(t)
		require.InDelta(t, 1, carried, 0)
		require.InDelta(t, 0, missing, 0)
	})

	t.Run("a probe spend that reports an error", func(t *testing.T) {
		// The spend reaches the server and records the position, but the call reports an error,
		// so the probe must not trust what it would read back.
		s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			_ = s.client.BatchOperate(policy, records)
			return aerospike.ErrNetwork
		}
		t.Cleanup(func() { s.batchOperateFn = nil })

		require.False(t, s.probeLaneSlotWrites(ctx), "a failed probe spend proves nothing")
	})

	t.Run("a probe spend whose response is an error", func(t *testing.T) {
		// The spend reaches the server and records the position, but the script's answer is an
		// error, so the probe must not trust what it would read back.
		s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			if err := s.client.BatchOperate(policy, records); err != nil {
				return err
			}

			records[0].BatchRec().Record.Bins[LuaSuccess.String()] = map[interface{}]interface{}{"status": string(LuaStatusError)}

			return nil
		}
		t.Cleanup(func() { s.batchOperateFn = nil })

		require.False(t, s.probeLaneSlotWrites(ctx), "a refused probe spend proves nothing")
	})
}

// laneSpendCount reads teranode_lane_spend_positions_total{store="aerospike", carried=carried}.
func laneSpendCount(t *testing.T, carried string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "teranode_lane_spend_positions_total" {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["store"] == laneStoreName && labels["carried"] == carried {
				return m.GetCounter().GetValue()
			}
		}
	}

	return 0
}

// The probe record is no transaction other readers see: it has no txID, unminedSince or createdAt
// bin, no TTL, and its spend sets no deleteAtHeight. It is gone when the probe returns.
func TestLaneSlotProbeRecordIsInvisibleAndRemoved(t *testing.T) {
	t.Parallel()

	s, client := setupStoreForLockTest(t)
	ctx := t.Context()

	var (
		probeKey      *aerospike.Key
		before, after *aerospike.Record
	)

	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		probeKey = records[0].BatchRec().Key

		var err error

		before, err = client.Get(nil, probeKey)
		require.NoError(t, err)

		opErr := s.client.BatchOperate(policy, records)

		after, err = client.Get(nil, probeKey)
		require.NoError(t, err)

		return opErr
	}
	t.Cleanup(func() { s.batchOperateFn = nil })

	require.True(t, s.probeLaneSlotWrites(ctx), "the real script passes the probe")
	require.NotNil(t, before)
	require.NotNil(t, after)

	for _, bin := range []fields.FieldName{fields.TxID, fields.UnminedSince, fields.CreatedAt} {
		_, ok := before.Bins[bin.String()]
		require.False(t, ok, "the probe record has no %s bin", bin)
	}

	require.Equal(t, uint32(aerospike.TTLDontExpire), before.Expiration, "the probe record is written without a TTL")

	_, ok := after.Bins[fields.DeleteAtHeight.String()]
	require.False(t, ok, "the probe spend sets no deleteAtHeight")

	_, err := client.Get(nil, probeKey)
	require.True(t, isKeyNotFound(err), "no probe record is left behind: %v", err)
}
