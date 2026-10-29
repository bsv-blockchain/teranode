package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

type batchTestItem struct {
	spend             *Spend
	ignoreConflicting bool
	err               error
	answered          bool
	local             bool
}

type batchTestBackend struct {
	start  func(*batchTestItem) error
	parts  [][]*batchTestItem
	answer func(*Spend) error
}

func (b *batchTestBackend) run(items []*batchTestItem) {
	ProcessSpendBatch(items, SpendBatchHandler[*batchTestItem]{
		Start: b.start,
		Spend: func(item *batchTestItem) *Spend {
			if item == nil {
				return nil
			}
			return item.spend
		},
		Process: func(part []*batchTestItem) {
			b.parts = append(b.parts, part)
			for _, item := range part {
				if item == nil {
					continue
				}
				item.err = b.answer(item.spend)
				item.answered = true
			}
		},
		Result: func(item *batchTestItem) (error, bool) {
			if item == nil {
				return nil, false
			}
			return item.err, item.answered
		},
		Reject: func(item *batchTestItem, err error) {
			item.err = err
			item.answered = true
			item.local = true
		},
		SameChecks: func(a, b *batchTestItem) bool {
			return a.ignoreConflicting == b.ignoreConflicting && SameUTXOHash(a.spend, b.spend)
		},
	})
}

func requireSpentBy(t *testing.T, err error, owner *spendpkg.SpendingData) {
	t.Helper()
	require.ErrorIs(t, err, errors.ErrSpent)

	var data *errors.UtxoSpentErrData
	require.True(t, errors.AsData(err, &data))
	require.Equal(t, owner.Bytes(), data.SpendingData.Bytes())
}

func TestProcessSpendBatch(t *testing.T) {
	parent := chainhash.HashH([]byte("parent"))
	utxoHash := chainhash.HashH([]byte("utxo"))
	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))
	c := chainhash.HashH([]byte("c"))
	claim := func(vout uint32, txID *chainhash.Hash) *batchTestItem {
		return &batchTestItem{spend: &Spend{
			TxID: &parent, Vout: vout, UTXOHash: &utxoHash,
			SpendingData: spendpkg.NewSpendingData(txID, 0),
		}}
	}
	ownerA := spendpkg.NewSpendingData(&a, 0)
	ownerB := spendpkg.NewSpendingData(&b, 0)
	storedOwner := func(owner *spendpkg.SpendingData) func(*Spend) error {
		return func(spend *Spend) error {
			if sameSpendingData(owner, spend.SpendingData) {
				return nil
			}
			return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, owner)
		}
	}

	t.Run("a batch without competing claims is one call", func(t *testing.T) {
		items := []*batchTestItem{claim(0, &a), claim(0, &a), claim(1, &b)}
		backend := &batchTestBackend{answer: storedOwner(ownerA)}
		backend.run(items)

		require.Len(t, backend.parts, 1)
		require.Equal(t, items, backend.parts[0])
	})

	t.Run("alternating claims cost two calls and name the stored owner", func(t *testing.T) {
		items := make([]*batchTestItem, 0, 17)
		for range 8 {
			items = append(items, claim(0, &a), claim(0, &b))
		}
		items = append(items, claim(1, &c))

		backend := &batchTestBackend{answer: func(spend *Spend) error {
			if sameSpendingData(ownerB, spend.SpendingData) {
				return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, ownerA)
			}
			return nil
		}}
		backend.run(items)

		require.Len(t, backend.parts, 2)
		for _, item := range items {
			require.True(t, item.answered)
			if sameSpendingData(ownerB, item.spend.SpendingData) {
				require.True(t, item.local)
				requireSpentBy(t, item.err, ownerA)
				continue
			}
			require.False(t, item.local)
			require.NoError(t, item.err)
		}
	})

	t.Run("an owner re-spend behind a losing claim succeeds", func(t *testing.T) {
		loser, owner := claim(0, &b), claim(0, &a)
		backend := &batchTestBackend{answer: storedOwner(ownerA)}
		backend.run([]*batchTestItem{loser, owner})

		require.Equal(t, [][]*batchTestItem{{loser}, {owner}}, backend.parts)
		require.NoError(t, owner.err)
		requireSpentBy(t, loser.err, ownerA)
		require.False(t, loser.local)
	})

	t.Run("UTXO_SPENT naming the claim itself leaves the owner unknown", func(t *testing.T) {
		first, second, third := claim(0, &a), claim(0, &b), claim(0, &c)
		backend := &batchTestBackend{answer: func(spend *Spend) error {
			if sameSpendingData(ownerA, spend.SpendingData) {
				return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
			}
			return nil
		}}
		backend.run([]*batchTestItem{first, second, third})

		require.Equal(t, [][]*batchTestItem{{first}, {second}}, backend.parts)
		require.False(t, second.local)
		require.NoError(t, second.err)
		require.True(t, third.local)
		requireSpentBy(t, third.err, ownerB)
	})

	t.Run("a failure without an owner is reused for matching checks", func(t *testing.T) {
		items := []*batchTestItem{claim(0, &a), claim(0, &b), claim(0, &a)}
		backend := &batchTestBackend{answer: func(*Spend) error {
			return errors.NewStorageError("backend down")
		}}
		backend.run(items)

		require.Equal(t, [][]*batchTestItem{{items[0]}}, backend.parts)
		for i, item := range items {
			require.Equal(t, i > 0, item.local)
			require.ErrorIs(t, item.err, errors.ErrStorageError)
		}
	})

	t.Run("a claim matching a known owner still reaches the backend", func(t *testing.T) {
		first, loser, again := claim(0, &a), claim(0, &c), claim(0, &a)
		backend := &batchTestBackend{answer: storedOwner(ownerA)}
		backend.run([]*batchTestItem{first, loser, again})

		require.Equal(t, [][]*batchTestItem{{first}, {again}}, backend.parts)
		require.True(t, loser.local)
		requireSpentBy(t, loser.err, ownerA)
		require.NoError(t, again.err)
	})

	t.Run("a claim with other checks than the decider reaches the backend", func(t *testing.T) {
		winner, conflicting, plain := claim(0, &a), claim(0, &b), claim(0, &c)
		winner.ignoreConflicting = true
		backend := &batchTestBackend{answer: func(spend *Spend) error {
			if sameSpendingData(ownerA, spend.SpendingData) {
				return nil
			}
			if sameSpendingData(ownerB, spend.SpendingData) {
				return errors.NewTxConflictingError("parent is conflicting")
			}
			return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, ownerA)
		}}
		backend.run([]*batchTestItem{winner, conflicting, plain})

		require.Equal(t, [][]*batchTestItem{{winner}, {conflicting}}, backend.parts)
		require.False(t, conflicting.local)
		require.ErrorIs(t, conflicting.err, errors.ErrTxConflicting)
		require.True(t, plain.local)
		require.ErrorIs(t, plain.err, errors.ErrTxConflicting)
	})

	t.Run("a claim with another UTXO hash reaches the backend", func(t *testing.T) {
		otherHash := chainhash.HashH([]byte("other utxo"))
		first, mismatch := claim(0, &a), claim(0, &b)
		mismatch.spend.UTXOHash = &otherHash
		backend := &batchTestBackend{answer: func(spend *Spend) error {
			if *spend.UTXOHash == otherHash {
				return errors.NewUtxoHashMismatchError("hash mismatch")
			}
			return storedOwner(ownerA)(spend)
		}}
		backend.run([]*batchTestItem{first, mismatch})

		require.Equal(t, [][]*batchTestItem{{first}, {mismatch}}, backend.parts)
		require.False(t, mismatch.local)
		require.ErrorIs(t, mismatch.err, errors.ErrUtxoHashMismatch)
	})

	t.Run("invalid items reach the backend", func(t *testing.T) {
		invalid := &batchTestItem{spend: &Spend{}}
		first, other := claim(0, &a), claim(0, &b)
		backend := &batchTestBackend{answer: func(spend *Spend) error {
			if spend.TxID == nil {
				return errors.NewProcessingError("invalid spend")
			}
			return storedOwner(ownerA)(spend)
		}}
		backend.run([]*batchTestItem{nil, invalid, first, other})

		require.Equal(t, [][]*batchTestItem{{nil, invalid, first}}, backend.parts)
		require.ErrorIs(t, invalid.err, errors.ErrProcessing)
		require.True(t, other.local)
		requireSpentBy(t, other.err, ownerA)
	})

	t.Run("empty", func(t *testing.T) {
		backend := &batchTestBackend{answer: storedOwner(ownerA)}
		backend.run(nil)

		require.Empty(t, backend.parts)
	})
}

func TestProcessSpendBatchWithoutSameChecks(t *testing.T) {
	parent, hash, a, b := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}, chainhash.Hash{4}
	first := &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&a, 0)}
	second := &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&b, 0)}
	var parts [][]*Spend
	ProcessSpendBatch([]*Spend{first, second}, SpendBatchHandler[*Spend]{
		Spend: func(s *Spend) *Spend { return s },
		Process: func(part []*Spend) {
			parts = append(parts, part)
			for _, s := range part {
				if s == second {
					s.Err = errors.NewUtxoSpentError(parent, 0, hash, first.SpendingData)
				}
			}
		},
		Result: func(s *Spend) (error, bool) { return s.Err, true },
		Reject: func(*Spend, error) { t.Fatal("no checks supplied: the backend must decide") },
	})
	require.Equal(t, [][]*Spend{{first}, {second}}, parts)
	require.NoError(t, first.Err)
	requireSpentBy(t, second.Err, first.SpendingData)
}

func TestSpendBatchFailureReuseBoundsBackendCalls(t *testing.T) {
	for _, failure := range []error{
		errors.NewStorageError("backend timeout"), errors.NewTxNotFoundError("parent missing"),
		errors.NewTxLockedError("parent locked"), errors.NewTxConflictingError("parent conflicting"),
		errors.NewUtxoHashMismatchError("wrong hash"),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			parent, hash := chainhash.Hash{1}, chainhash.Hash{2}
			items := make([]*batchTestItem, 1024)
			for i := range items {
				spender := chainhash.Hash{byte(i%2 + 3)}
				items[i] = &batchTestItem{spend: &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&spender, 0)}}
			}
			backend := &batchTestBackend{answer: func(*Spend) error { return failure }}
			backend.run(items)
			require.Len(t, backend.parts, 1)
			for _, item := range items {
				require.Same(t, failure, item.err)
				require.True(t, item.answered)
			}
		})
	}
}

func TestSpendBatchAdmissionErrorIsNotReused(t *testing.T) {
	parent, hash, a, b := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}, chainhash.Hash{4}
	first := &batchTestItem{spend: &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&a, 0)}}
	second := &batchTestItem{spend: &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&b, 0)}}
	backend := &batchTestBackend{
		start: func(item *batchTestItem) error {
			if item == first {
				return errors.NewServiceUnavailableError("caller expired")
			}
			return nil
		},
		answer: func(*Spend) error { return nil },
	}
	backend.run([]*batchTestItem{first, second})
	require.Equal(t, [][]*batchTestItem{{second}}, backend.parts)
	require.Error(t, first.err)
	require.NoError(t, second.err)
	require.False(t, second.local)
}

func TestSpendBatchRechecksAdmissionForPendingUnrelatedClaim(t *testing.T) {
	parent, other, hash, a, b := chainhash.Hash{1}, chainhash.Hash{9}, chainhash.Hash{2}, chainhash.Hash{3}, chainhash.Hash{4}
	first := &batchTestItem{spend: &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&a, 0)}}
	pending := &batchTestItem{spend: &Spend{TxID: &other, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&a, 0)}}
	conflict := &batchTestItem{spend: &Spend{TxID: &parent, UTXOHash: &hash, SpendingData: spendpkg.NewSpendingData(&b, 0)}}
	abandoned := false
	backend := &batchTestBackend{
		start: func(item *batchTestItem) error {
			if abandoned && item == pending {
				return errors.NewServiceUnavailableError("caller expired")
			}
			return nil
		},
		answer: func(*Spend) error { abandoned = true; return nil },
	}
	backend.run([]*batchTestItem{first, conflict, pending})
	require.Equal(t, [][]*batchTestItem{{first}}, backend.parts)
	require.Error(t, pending.err)
}
