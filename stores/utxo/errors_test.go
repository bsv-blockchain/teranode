package utxo

import (
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestErrSpentExtra(t *testing.T) {
	err := NewErrSpentExtra(nil)
	assert.ErrorIs(t, err, ErrSpent)
	assert.Equal(t, "utxo already spent (invalid use of ErrSpentExtra as spendingTxID is not set)", err.Error())

	spendingTxID := chainhash.Hash{}
	err = NewErrSpentExtra(&spendingTxID)
	assert.ErrorIs(t, err, ErrSpent)
	assert.Equal(t, "utxo already spent by txid 0000000000000000000000000000000000000000000000000000000000000000", err.Error())
}

func TestErrLockTimeExtra(t *testing.T) {
	err := NewErrLockTimeExtra(0, 0)
	assert.ErrorIs(t, err, ErrLockTime)
	assert.Equal(t, "utxo not spendable (invalid use of ErrLockTimeExtra as locktime is zero)", err.Error())

	// Following test cases taken from https://learnmeabitcoin.com/technical/locktime

	err = NewErrLockTimeExtra(452845, 0)
	assert.ErrorIs(t, err, ErrLockTime)
	assert.Equal(t, "utxo not spendable until block 452845 (height check: 0)", err.Error())

	err = NewErrLockTimeExtra(1494557702, 0)
	assert.ErrorIs(t, err, ErrLockTime)
	assert.Equal(t, "utxo not spendable until 2017-05-12T02:55:02Z", err.Error())

	err = NewErrLockTimeExtra(868373681, 0)
	assert.ErrorIs(t, err, ErrLockTime)
	assert.Equal(t, "utxo not spendable until 1997-07-08T14:54:41Z", err.Error())
}
