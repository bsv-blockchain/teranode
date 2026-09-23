package meta

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// Test_CohortIsNotCarriedInBinaryEncodings pins the deliberate gap documented on
// the Data.Cohort field: the cohort label is NOT part of any of the four binary
// encodings on this type.
//
// The point of the test is the BlockIDs assertion. Bytes() ends with an
// unlengthed, unversioned run of 4-byte block IDs and NewDataFromBytes reads the
// whole remainder after the transaction as exactly that, to EOF. Appending four
// cohort bytes there would not fail to parse — it would come back as an extra
// block ID, which is mined-state corruption on the consensus path. So the test
// asserts the block IDs round-trip unchanged (proving nothing was appended), and
// asserts the cohort comes back zero (documenting the known gap, so that whoever
// later adds a versioned encoding has to come here and change it deliberately).
func Test_CohortIsNotCarriedInBinaryEncodings(t *testing.T) {
	t.Run("Bytes/NewDataFromBytes leaves the block IDs untouched", func(t *testing.T) {
		data := &Data{
			Fee:          100,
			SizeInBytes:  200,
			TxInpoints:   testInpointsHash3Hash4,
			Tx:           &bt.Tx{},
			BlockIDs:     []uint32{7, 11, 13},
			BlockHeights: []uint32{700, 1100, 1300},
			Cohort:       1_700_000_000,
		}

		b, err := data.Bytes()
		require.NoError(t, err)

		d, err := NewDataFromBytes(b)
		require.NoError(t, err)

		// The codec was not disturbed: exactly the block IDs that went in come
		// back out, with no extra trailing entry from the cohort.
		require.Equal(t, []uint32{7, 11, 13}, d.BlockIDs)

		// Known gap: the cohort is not carried by this encoding.
		require.Equal(t, uint32(0), d.Cohort)
	})

	t.Run("Bytes/NewDataFromBytes with no block IDs gains none", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints:  testInpointsHash3Hash4,
			Tx:          &bt.Tx{},
			Cohort:      1_700_000_000,
		}

		b, err := data.Bytes()
		require.NoError(t, err)

		d, err := NewDataFromBytes(b)
		require.NoError(t, err)

		require.Empty(t, d.BlockIDs)
		require.Equal(t, uint32(0), d.Cohort)
	})

	t.Run("a non-zero cohort does not change the serialized length", func(t *testing.T) {
		base := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints:  testInpointsHash3Hash4,
			Tx:          &bt.Tx{},
			BlockIDs:    []uint32{7},
		}

		stamped := *base
		stamped.Cohort = 1_700_000_000

		baseBytes, err := base.Bytes()
		require.NoError(t, err)

		stampedBytes, err := stamped.Bytes()
		require.NoError(t, err)

		require.Equal(t, baseBytes, stampedBytes)

		baseMeta, err := base.MetaBytes()
		require.NoError(t, err)

		stampedMeta, err := stamped.MetaBytes()
		require.NoError(t, err)

		require.Equal(t, baseMeta, stampedMeta)
	})

	t.Run("MetaBytes/NewMetaDataFromBytes does not carry the cohort", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints:  testInpointsHash3Hash4,
			Cohort:      1_700_000_000,
		}

		b, err := data.MetaBytes()
		require.NoError(t, err)

		var d Data
		require.NoError(t, NewMetaDataFromBytes(b, &d))

		require.Equal(t, uint32(0), d.Cohort)
	})
}
