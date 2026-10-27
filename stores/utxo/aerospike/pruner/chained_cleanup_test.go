package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// sentBatch is one captured BatchOperate: the digests it addressed, split by
// record kind.
type sentBatch struct {
	updates []string
	deletes []string
}

// captureBatches makes svc answer every batch as a mod-teranode success and
// returns the batches it was sent.
func captureBatches(svc *Service) *[]sentBatch {
	var sent []sentBatch

	svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		var b sentBatch

		for _, r := range records {
			digest := string(r.BatchRec().Key.Digest())

			switch r.(type) {
			case *aerospike.BatchDelete:
				b.deletes = append(b.deletes, digest)
			default:
				b.updates = append(b.updates, digest)
				r.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
					luaSuccessBin: map[interface{}]interface{}{fieldStatus: statusOK},
				}}
			}
		}

		sent = append(sent, b)

		return nil
	}

	return &sent
}

// chainRecord returns a prunable master record for txID that spends parent:0.
func chainRecord(t *testing.T, svc *Service, txID, parent chainhash.Hash) *aerospike.Result {
	t.Helper()

	key, err := aerospike.NewKey(svc.namespace, svc.set, txID[:])
	require.NoError(t, err)

	return &aerospike.Result{Record: &aerospike.Record{
		Key: key,
		Bins: aerospike.BinMap{
			svc.fieldTxID:           txID.CloneBytes(),
			svc.fieldInputs:         []interface{}{makeInputBytes(t, parent, 0)},
			svc.fieldExternal:       false,
			svc.fieldTotalExtraRecs: 0,
		},
	}}
}

func digestOf(t *testing.T, svc *Service, txID chainhash.Hash) string {
	t.Helper()

	key, err := aerospike.NewKey(svc.namespace, svc.set, txID[:])
	require.NoError(t, err)

	return string(key.Digest())
}

// A chain A→B→C pruned in one chunk: B is the parent of C and is itself
// deleted. The parent update for B must not be sent, so no batch addresses B
// twice; A, which survives, still gets its marker.
func TestFlushCleanup_ParentDeletedInSameChunkGetsNoUpdate(t *testing.T) {
	a := chainhash.Hash{0xa1}
	b := chainhash.Hash{0xb2}
	c := chainhash.Hash{0xc3}

	for _, defensive := range []bool{false, true} {
		name := "combined"
		if defensive {
			name = "defensive"
		}

		t.Run(name, func(t *testing.T) {
			svc := newTestService(t)
			svc.settings.Pruner.SkipDeletions = false
			svc.defensiveEnabled = defensive
			svc.luaPackage = "teranode"
			sent := captureBatches(svc)

			chunk := []*aerospike.Result{chainRecord(t, svc, b, a), chainRecord(t, svc, c, b)}

			processed, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
			require.NoError(t, err)
			require.Equal(t, 2, processed)

			var updates, deletes []string

			for i, batch := range *sent {
				seen := make(map[string]bool)

				for _, d := range append(append([]string{}, batch.updates...), batch.deletes...) {
					require.Falsef(t, seen[d], "batch %d addresses the same record twice", i)
					seen[d] = true
				}

				updates = append(updates, batch.updates...)
				deletes = append(deletes, batch.deletes...)
			}

			require.Equal(t, []string{digestOf(t, svc, a)}, updates, "only the surviving parent A is updated")
			require.ElementsMatch(t, []string{digestOf(t, svc, b), digestOf(t, svc, c)}, deletes)
		})
	}
}

// With deletions skipped nothing is removed, so every parent keeps its update.
func TestFlushCleanup_SkipDeletionsKeepsEveryParentUpdate(t *testing.T) {
	a := chainhash.Hash{0xa1}
	b := chainhash.Hash{0xb2}
	c := chainhash.Hash{0xc3}

	svc := newTestService(t)
	svc.luaPackage = "teranode"
	sent := captureBatches(svc)

	chunk := []*aerospike.Result{chainRecord(t, svc, b, a), chainRecord(t, svc, c, b)}

	_, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.NoError(t, err)

	require.Len(t, *sent, 1)
	require.Empty(t, (*sent)[0].deletes)
	require.ElementsMatch(t, []string{digestOf(t, svc, a), digestOf(t, svc, b)}, (*sent)[0].updates)
}

// Digest matching must not depend on the user key: records returned by a
// partition query carry only the digest.
func TestDropParentUpdatesOfDeletedRecords_MatchesByDigest(t *testing.T) {
	ensurePrometheusMetrics()

	b := chainhash.Hash{0xb2}

	withUserKey, err := aerospike.NewKey("test", "test", b[:])
	require.NoError(t, err)

	digestOnly, err := aerospike.NewKeyWithDigest("test", "test", nil, withUserKey.Digest())
	require.NoError(t, err)

	updates := map[string]*parentUpdateInfo{"b": {key: withUserKey, childHashes: []*chainhash.Hash{{0xc3}}}}

	dropParentUpdatesOfDeletedRecords(updates, []*aerospike.Key{digestOnly})
	require.Empty(t, updates)
}
