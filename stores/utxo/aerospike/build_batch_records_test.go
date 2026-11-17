package aerospike

import (
	"encoding/binary"
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

func newBuildRecordsStore() *Store {
	tSettings := settings.NewSettings()
	return &Store{namespace: "test", setName: "utxo", settings: tSettings}
}

func makeItems(n int) []*utxo.UnresolvedMetaData {
	items := make([]*utxo.UnresolvedMetaData, n)
	for i := range items {
		var h chainhash.Hash
		binary.LittleEndian.PutUint32(h[:], uint32(i+1))
		items[i] = &utxo.UnresolvedMetaData{Hash: h, Idx: i}
	}

	return items
}

func binNamesOf(t *testing.T, rec aerospike.BatchRecordIfc) []string {
	t.Helper()
	br, ok := rec.(*aerospike.BatchRead)
	require.True(t, ok, "record is not a *BatchRead")

	return br.BinNames
}

// Test_buildBatchRecords_UniformDefault verifies the shared-field-set path: with
// no caller fields, every item gets the expanded default field set and the same
// wire BinNames, the per-item key digest matches NewKey, and (the hoist's point)
// every item shares ONE backing array for the expanded fields rather than a
// fresh allocation each.
func Test_buildBatchRecords_UniformDefault(t *testing.T) {
	s := newBuildRecordsStore()
	policy := aerospike.NewBatchReadPolicy()

	items := makeItems(4)

	recs, err := s.buildBatchRecords(items, policy, nil)
	require.NoError(t, err)
	require.Len(t, recs, 4)

	wantFields := s.addAbstractedBins(defaultDecorateBins)
	wantNames := fields.FieldNamesToStrings(wantFields)

	for i, item := range items {
		require.Equal(t, wantFields, item.Fields, "item %d expanded fields", i)
		require.Equal(t, wantNames, binNamesOf(t, recs[i]), "item %d bin names", i)

		wantKey, kerr := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		require.NoError(t, kerr)
		require.Equal(t, wantKey.Digest(), recs[i].(*aerospike.BatchRead).Key.Digest(), "item %d key digest", i)
	}

	// Hoist invariant: all uniform items share the same backing array for the
	// expanded fields (no per-item re-allocation).
	require.Same(t, &items[0].Fields[0], &items[1].Fields[0],
		"uniform items must share one expanded-fields backing array")
}

// Test_buildBatchRecords_OptionalFields verifies a uniform caller-supplied field
// set is expanded once and applied to every item.
func Test_buildBatchRecords_OptionalFields(t *testing.T) {
	s := newBuildRecordsStore()
	policy := aerospike.NewBatchReadPolicy()

	items := makeItems(3)

	recs, err := s.buildBatchRecords(items, policy, []fields.FieldName{fields.Tx})
	require.NoError(t, err)

	wantFields := s.addAbstractedBins([]fields.FieldName{fields.Tx})
	for i, item := range items {
		require.Equal(t, wantFields, item.Fields)
		require.Equal(t, fields.FieldNamesToStrings(wantFields), binNamesOf(t, recs[i]))
	}
}

// Test_buildBatchRecords_PerItemFieldsOverride verifies an item carrying its own
// Fields is expanded individually and does not pick up the shared set, while the
// other items still use the shared set.
func Test_buildBatchRecords_PerItemFieldsOverride(t *testing.T) {
	s := newBuildRecordsStore()
	policy := aerospike.NewBatchReadPolicy()

	items := makeItems(3)
	items[1].Fields = []fields.FieldName{fields.BlockIDs}

	recs, err := s.buildBatchRecords(items, policy, []fields.FieldName{fields.Tx})
	require.NoError(t, err)

	sharedFields := s.addAbstractedBins([]fields.FieldName{fields.Tx})
	overrideFields := s.addAbstractedBins([]fields.FieldName{fields.BlockIDs})

	require.Equal(t, sharedFields, items[0].Fields)
	require.Equal(t, overrideFields, items[1].Fields)
	require.Equal(t, sharedFields, items[2].Fields)

	require.Equal(t, fields.FieldNamesToStrings(overrideFields), binNamesOf(t, recs[1]))
	require.NotEqual(t, binNamesOf(t, recs[0]), binNamesOf(t, recs[1]))
}
