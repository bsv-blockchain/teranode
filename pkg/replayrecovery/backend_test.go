package replayrecovery

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestEvidenceBackendCanonicalExternalInputs(t *testing.T) {
	for _, classification := range []string{FullySpent, Live, Unknown} {
		t.Run(classification, func(t *testing.T) {
			native, source, tx, _, _ := recoveryFixture(t)
			ev := source.evidence[tx.TxID()]
			ev.Classification = classification
			source.evidence[tx.TxID()] = ev
			backend := NewEvidenceBackend(native, source, source.tip, nil)
			inputs, err := backend.Inputs(context.Background(), tx.TxID())

			require.NoError(t, err)
			require.Equal(t, []string{tx.Inputs[0].PreviousTxIDChainHash().String()}, inputs)
			raw, err := backend.Transaction(context.Background(), tx.TxID())
			require.NoError(t, err)
			require.Equal(t, tx.Bytes(), raw.Bytes())
			snapshot, err := backend.Snapshot(context.Background(), tx)
			require.NoError(t, err)
			require.Equal(t, tx.TxID(), snapshot.TxID)
			require.Zero(t, native.writes)
		})
	}
}

func TestEvidenceBackendRetainedUnminedRaw(t *testing.T) {
	native, source, tx, _, _ := recoveryFixture(t)
	ev := source.evidence[tx.TxID()]
	ev.Classification = Unknown
	source.evidence[tx.TxID()] = ev
	backend := NewEvidenceBackend(native, source, source.tip, func(context.Context, string) (*bt.Tx, error) { return tx, nil })
	inputs, err := backend.Inputs(context.Background(), tx.TxID())
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	unchanged, err := source.Check(context.Background(), tx.TxID(), source.tip)
	require.NoError(t, err)
	require.Equal(t, Unknown, unchanged.Classification, "raw archive availability must not manufacture confirmation")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = backend.Transaction(ctx, tx.TxID())
	require.ErrorIs(t, err, context.Canceled)
}

func TestEvidenceBackendRejectsWrongArchiveIdentity(t *testing.T) {
	native, source, tx, _, _ := recoveryFixture(t)
	delete(source.evidence, tx.TxID())
	other := tx.Clone()
	other.LockTime++
	backend := NewEvidenceBackend(native, source, source.tip, func(context.Context, string) (*bt.Tx, error) { return other, nil })
	_, err := backend.Transaction(context.Background(), tx.TxID())
	require.Error(t, err)
}

type inlineEvidenceBackend struct {
	*recordBackend
	tx *bt.Tx
}

func (b *inlineEvidenceBackend) Transaction(context.Context, string) (*bt.Tx, error) {
	return b.tx, nil
}

func TestEvidenceBackendInlineIdentityAndParentVerification(t *testing.T) {
	native, source, tx, _, _ := recoveryFixture(t)
	underlying := &inlineEvidenceBackend{recordBackend: native, tx: tx}
	backend := NewEvidenceBackend(underlying, source, source.tip, nil)
	got, err := backend.Transaction(context.Background(), tx.TxID())
	require.NoError(t, err)
	require.Equal(t, tx.Bytes(), got.Bytes())
	parent := native.records["parent"]
	parent.Data = []byte("spent:" + tx.TxID() + ":marked")
	native.records["parent"] = parent
	require.NoError(t, backend.VerifyParent(context.Background(), Parent{Record: parent, Child: tx.TxID()}))
	parent.Data = []byte("changed-spend")
	native.records["parent"] = parent
	require.Error(t, backend.VerifyParent(context.Background(), Parent{Record: parent, Child: tx.TxID()}))
	other := tx.Clone()
	other.LockTime++
	underlying.tx = other
	_, err = backend.Transaction(context.Background(), tx.TxID())
	require.Error(t, err, "wrong inline identity must not be hidden by valid source fallback")
}
