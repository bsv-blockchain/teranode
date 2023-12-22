package utxo_test

import (
	"io"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
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

	err2 := utxo.NewErrSpent(nil)
	assert.NotEqual(t, err, err2)

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
	/* create two errors with different optional errors. Do this before asserts */
	/* want to make sure that err and err2 are not the same instance */
	spendingTxID, _ := chainhash.NewHashFromStr("01")
	spendingTxID2, _ := chainhash.NewHashFromStr("02")
	err := utxo.NewErrSpent(spendingTxID, io.EOF)
	err2 := utxo.NewErrSpent(spendingTxID2, io.ErrNoProgress)

	/* err asserts */
	assert.ErrorIs(t, err, utxo.ErrTypeSpent)
	assert.ErrorIs(t, err, io.EOF)
	assert.NotErrorIs(t, err, io.ErrNoProgress)
	assert.NotErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, err, utxo.ErrTypeLockTime)

	/* err2 asserts */
	assert.ErrorIs(t, err2, utxo.ErrTypeSpent)
	assert.ErrorIs(t, err2, io.ErrNoProgress)
	assert.NotErrorIs(t, err2, io.EOF)
	assert.NotErrorIs(t, err2, io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, err2, utxo.ErrTypeLockTime)
}

func TestDoubleWrappedErrorString(t *testing.T) {
	err := ubsverrors.Wrap(utxo.ErrNotFound, utxo.NewErrLockTime(1, 1, utxo.ErrAlreadyExists))
	assert.Equal(t, "utxo not found: utxo is locked until block 1 (height check: 1): utxo already exists", err.Error())
}

func TestStaticSentinelErrorIs(t *testing.T) {
	err := utxo.ErrNotFound
	assert.ErrorIs(t, err, utxo.ErrNotFound)
	assert.NotErrorIs(t, err, utxo.ErrAlreadyExists)
}

func TestDynamicSentinelError(t *testing.T) {
	err := utxo.NewErrLockTime(1, 1)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.NotErrorIs(t, err, utxo.ErrAlreadyExists)
	var errLockTime *utxo.ErrLockTime
	assert.ErrorAs(t, err, &errLockTime)
}

func TestWrappedSentinelErrorIs(t *testing.T) {
	err := utxo.NewErrLockTime(1, 1, utxo.ErrChainHash)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.ErrorIs(t, err, utxo.ErrChainHash)
	assert.NotErrorIs(t, err, utxo.ErrAlreadyExists)

	var errLockTime *utxo.ErrLockTime
	assert.ErrorAs(t, err, &errLockTime)
}

func TestDoubleWrappedSentinelErrorIs(t *testing.T) {
	err := ubsverrors.Wrap(utxo.ErrNotFound, utxo.NewErrLockTime(1, 1, utxo.ErrChainHash))
	assert.ErrorIs(t, err, utxo.ErrNotFound)
	assert.ErrorIs(t, err, utxo.ErrTypeLockTime)
	assert.ErrorIs(t, err, utxo.ErrChainHash)
	assert.NotErrorIs(t, err, utxo.ErrAlreadyExists)

	var errLockTime *utxo.ErrLockTime
	assert.ErrorAs(t, err, &errLockTime)

	var errError *ubsverrors.Error
	assert.ErrorAs(t, err, &errError)
}
