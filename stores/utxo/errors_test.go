package utxo_test

import (
	"io"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestErrSpent(t *testing.T) {
	err := utxo.NewErrSpent(nil)
	assert.ErrorIs(t, err, utxo.ErrTypeSpent)
	assert.Equal(t, "utxo already spent (invalid use of ErrSpent as spendingTxID is not set)", err.Error())

	spendingTxID := chainhash.Hash{}
	err = utxo.NewErrSpent(&spendingTxID)
	assert.ErrorIs(t, err, utxo.ErrTypeSpent)
	assert.Equal(t, "utxo already spent by txid 0000000000000000000000000000000000000000000000000000000000000000", err.Error())
}

func TestErrLockTime(t *testing.T) {
	err := utxo.NewErrLockTime(0, 0)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.Equal(t, "utxo is locked (invalid use of ErrLockTime as locktime is zero)", err.Error())

	// Following test cases taken from https://learnmeabitcoin.com/technical/locktime

	err = utxo.NewErrLockTime(452845, 0)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.Equal(t, "utxo is locked until block 452845 (height check: 0)", err.Error())

	err = utxo.NewErrLockTime(1494557702, 0)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.Equal(t, "utxo is locked until 2017-05-12T02:55:02Z", err.Error())

	err = utxo.NewErrLockTime(868373681, 0)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.Equal(t, "utxo is locked until 1997-07-08T14:54:41Z", err.Error())
}

func TestSpentWithOptionalError(t *testing.T) {
	spendingTxID := chainhash.Hash{}
	err := utxo.NewErrSpent(&spendingTxID, io.EOF)

	assert.ErrorIs(t, err, utxo.ErrTypeSpent)
	assert.ErrorIs(t, err, io.EOF)
}
