package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// spentElement builds the 68-byte form: 32-byte utxo hash + 36-byte spending data.
func spentElement(t *testing.T, marker byte) []uint8 {
	t.Helper()

	var (
		utxoHash chainhash.Hash
		spendTx  chainhash.Hash
	)

	utxoHash[0] = marker
	spendTx[0] = marker + 1

	out := make([]uint8, 0, 68)
	out = append(out, utxoHash[:]...)
	out = append(out, spendpkg.NewSpendingData(&spendTx, 1).Bytes()...)

	require.Len(t, out, 68)

	return out
}

// unspentElement builds the 32-byte form: hash only, no spending data.
func unspentElement(marker byte) []uint8 {
	var utxoHash chainhash.Hash
	utxoHash[0] = marker

	return utxoHash[:]
}

// TestExtraRecordElementShapes pins the distinction issue 1440 turns on. Whether an
// output is spent is consensus state, and a nil slot reads as UNSPENT — so a
// torn extra record must fail the read rather than silently hand back a spent
// output as spendable. But 32 bytes is the LEGITIMATE unspent encoding, so
// rejecting everything that is not 68 bytes would break every unspent output
// on every large transaction. Both halves are pinned here.
func TestExtraRecordElementShapes(t *testing.T) {
	t.Run("68 bytes is spent and 32 bytes is unspent", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 2)

		require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			spentElement(t, 0x11),
			unspentElement(0x22),
		}, 0, spendingDatas, 2))

		require.NotNil(t, spendingDatas[0], "68-byte element must record spending data")
		require.Nil(t, spendingDatas[1], "32-byte element is a legitimate unspent output")
	})

	t.Run("a nil element is a provably-unspendable output, not damage", func(t *testing.T) {
		// GetBinsToStore leaves utxos[i] nil for every output that
		// utxo.ShouldStoreOutputAsUTXO rejects (OP_FALSE OP_RETURN in any era,
		// bare OP_RETURN and oversized scripts pre-Genesis). Those nils are
		// packed as msgpack nil and come back as nil list elements, so a large
		// transaction with a data output has them in its extra records.
		// Rejecting them would fail the read of a perfectly healthy record.
		spendingDatas := make([]*spendpkg.SpendingData, 3)

		require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			nil,
			spentElement(t, 0x55),
			nil,
		}, 0, spendingDatas, 3))

		require.Nil(t, spendingDatas[0], "an unspendable output has no spending data")
		require.NotNil(t, spendingDatas[1], "the spent output either side of it must still be read")
		require.Nil(t, spendingDatas[2])
	})

	t.Run("a short element fails the read", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{make([]uint8, 40)}, 0, spendingDatas, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 32 (unspent) or 68 (spent)")
	})

	t.Run("a non-byte element fails the read", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{"not bytes"}, 0, spendingDatas, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not bytes")
	})

	t.Run("a truncated record fails the read", func(t *testing.T) {
		// The same hazard as a torn element, arriving from the other direction:
		// the loop never visits the missing offsets, so those slots stay nil,
		// and nil reads as UNSPENT. Without the count check a truncated record
		// hands back spent outputs as spendable with no error anywhere.
		spendingDatas := make([]*spendpkg.SpendingData, 3)

		err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			spentElement(t, 0x11),
		}, 0, spendingDatas, 3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "holds 1 outputs, expected 3")
	})

	t.Run("a trailing nil is not mistaken for truncation", func(t *testing.T) {
		// A healthy batch whose tail is unspendable outputs stores trailing
		// nils, and they survive the round trip (pinned against a real
		// Aerospike by TestTrailingNilSlotsSurviveRoundTrip). Exact equality
		// must not reject it.
		spendingDatas := make([]*spendpkg.SpendingData, 3)

		require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			spentElement(t, 0x11),
			nil,
			nil,
		}, 0, spendingDatas, 3))

		require.NotNil(t, spendingDatas[0])
		require.Nil(t, spendingDatas[1])
		require.Nil(t, spendingDatas[2])
	})

	t.Run("more outputs than the transaction has fails instead of panicking", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		require.NotPanics(t, func() {
			err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
				unspentElement(0x33),
				unspentElement(0x44),
			}, 0, spendingDatas, 2)
			require.Error(t, err)
			require.Contains(t, err.Error(), "more outputs than the transaction has")
		})
	})
}

// TestApplyExtraRecordBins covers interpreting a fetched extra record. The
// fetch itself needs Aerospike; the interpretation does not, and it is where
// the torn-record decisions live.
func TestApplyExtraRecordBins(t *testing.T) {
	t.Run("a healthy record is applied", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 2)

		record := &aerospike.Record{Bins: aerospike.BinMap{
			fields.Utxos.String(): []interface{}{spentElement(t, 0x11), unspentElement(0x22)},
		}}

		require.NoError(t, applyExtraRecordBins(&chainhash.Hash{}, 1, record, 0, spendingDatas, 2))
		require.NotNil(t, spendingDatas[0])
		require.Nil(t, spendingDatas[1])
	})

	t.Run("a missing record fails instead of panicking", func(t *testing.T) {
		// Defence in depth: this client version reports an absent key as
		// ErrKeyNotFound, which getAllExtraUTXOs turns into a storage error before
		// reaching here, so a nil record should be unreachable in production. It
		// must still error rather than panic on the deref if it ever arrives.
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		require.NotPanics(t, func() {
			err := applyExtraRecordBins(&chainhash.Hash{}, 2, nil, 0, spendingDatas, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), "is missing")
		})
	})

	t.Run("a record with nil bins fails instead of panicking", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		require.NotPanics(t, func() {
			err := applyExtraRecordBins(&chainhash.Hash{}, 2, &aerospike.Record{}, 0, spendingDatas, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), "is missing")
		})
	})

	t.Run("a missing or malformed utxos bin fails the read", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		record := &aerospike.Record{Bins: aerospike.BinMap{fields.Utxos.String(): "not a list"}}

		err := applyExtraRecordBins(&chainhash.Hash{}, 3, record, 0, spendingDatas, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing or malformed utxos bin")
		require.Contains(t, err.Error(), "string", "the offending type must be named so the damage is diagnosable")
	})

	t.Run("an error from the element loop is propagated", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		record := &aerospike.Record{Bins: aerospike.BinMap{
			fields.Utxos.String(): []interface{}{make([]uint8, 40)},
		}}

		err := applyExtraRecordBins(&chainhash.Hash{}, 4, record, 0, spendingDatas, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 32 (unspent) or 68 (spent)")
	})
}

// TestClassifyUTXOReadError pins the rule that keeps local storage damage out
// of the consensus verdict path. ErrTxInvalid makes ValidateBlock persist the
// block as permanently invalid and flags the peer that served it, so a torn
// read of our own records must never be dressed up as one.
func TestClassifyUTXOReadError(t *testing.T) {
	t.Run("a storage error keeps its class", func(t *testing.T) {
		err := classifyUTXOReadError(errors.NewStorageError("torn record"))

		require.True(t, errors.Is(err, errors.ErrStorageError))
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "storage damage must not become a consensus verdict")
	})

	t.Run("a cancelled context is not a consensus verdict", func(t *testing.T) {
		// getAllExtraUTXOs returns ctx.Err() unwrapped, and processUTXOs passes it
		// straight up. Shutting a node down mid-read must not permanently invalidate
		// a valid block or flag the peer that served it.
		require.False(t, errors.Is(classifyUTXOReadError(context.Canceled), errors.ErrTxInvalid))
		require.False(t, errors.Is(classifyUTXOReadError(context.DeadlineExceeded), errors.ErrTxInvalid))
		require.ErrorIs(t, classifyUTXOReadError(context.Canceled), context.Canceled)
	})

	t.Run("a processing error is not a consensus verdict", func(t *testing.T) {
		err := classifyUTXOReadError(errors.NewProcessingError("failed to create key for extra record"))

		require.True(t, errors.Is(err, errors.ErrProcessing))
		require.False(t, errors.Is(err, errors.ErrTxInvalid))
	})

	t.Run("an error that already is a consensus verdict stays one", func(t *testing.T) {
		err := classifyUTXOReadError(errors.NewTxInvalidError("genuinely invalid"))

		require.True(t, errors.Is(err, errors.ErrTxInvalid))
	})
}

// TestProcessUTXOsBoundsCheck covers the master-record reader refusing to index
// past the end of its result slice when the totalUtxos bin disagrees with the
// stored list. With no totalExtraRecs bin this needs no Aerospike client.
func TestProcessUTXOsBoundsCheck(t *testing.T) {
	t.Run("more outputs than totalUtxos says fails instead of panicking", func(t *testing.T) {
		s := &Store{}

		bins := aerospike.BinMap{
			fields.TotalUtxos.String(): 1,
			fields.Utxos.String():      []interface{}{unspentElement(0x11), unspentElement(0x22)},
		}

		require.NotPanics(t, func() {
			_, err := s.processUTXOs(context.Background(), &chainhash.Hash{}, bins)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrStorageError))
			require.Contains(t, err.Error(), "more outputs than totalUtxos says")
		})
	})

	t.Run("a missing utxos bin is storage damage, not a consensus verdict", func(t *testing.T) {
		// Every record this store writes carries a utxos bin, so its absence means
		// our own record is malformed. It must not become a TxInvalid verdict: the
		// extra-record reader already classifies the identical condition as storage.
		s := &Store{}

		bins := aerospike.BinMap{fields.TotalUtxos.String(): 1}

		_, err := s.processUTXOs(context.Background(), &chainhash.Hash{}, bins)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		require.False(t, errors.Is(err, errors.ErrTxInvalid))
		require.Contains(t, err.Error(), "missing or malformed utxos bin")
	})

	t.Run("a wrong-typed utxos bin is storage damage", func(t *testing.T) {
		s := &Store{}

		bins := aerospike.BinMap{
			fields.TotalUtxos.String(): 1,
			fields.Utxos.String():      "not a list",
		}

		_, err := s.processUTXOs(context.Background(), &chainhash.Hash{}, bins)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		require.Contains(t, err.Error(), "string", "the offending type must be named so the damage is diagnosable")
	})

	t.Run("a missing totalUtxos bin is storage damage", func(t *testing.T) {
		s := &Store{}

		_, err := s.processUTXOs(context.Background(), &chainhash.Hash{}, aerospike.BinMap{})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
	})

	t.Run("a consistent record is read normally", func(t *testing.T) {
		s := &Store{}

		bins := aerospike.BinMap{
			fields.TotalUtxos.String(): 2,
			fields.Utxos.String():      []interface{}{spentElement(t, 0x33), unspentElement(0x44)},
		}

		spendingDatas, err := s.processUTXOs(context.Background(), &chainhash.Hash{}, bins)
		require.NoError(t, err)
		require.Len(t, spendingDatas, 2)
		require.NotNil(t, spendingDatas[0])
		require.Nil(t, spendingDatas[1])
	})
}
