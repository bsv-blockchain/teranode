package util

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
)

func TestIsTransactionFinal(t *testing.T) {
	type args struct {
		tx              *bt.Tx
		blockHeight     uint32
		medianBlockTime uint32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test IsTransactionFinal - empty tx",
			args: args{
				tx:              &bt.Tx{},
				blockHeight:     0,
				medianBlockTime: 0,
			},
			want: false,
		},
		{
			name: "Test IsTransactionFinal - lock time is bigger than block height",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						// lock time only works with sequence number != 0xffffffff
						SequenceNumber: 123,
					}},
					LockTime: 123,
				},
				blockHeight:     100,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: false,
		},
		{
			name: "Test IsTransactionFinal - lock time is equal to block height",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						// lock time only works with sequence number != 0xffffffff
						SequenceNumber: 123,
					}},
					LockTime: 123,
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is equal to block height, and final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 0xffffffff,
					}},
					LockTime: 123,
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is equal to block height, but non final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 123,
					}, {
						SequenceNumber: 0xffffffff,
					}},
					LockTime: 123,
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is time in the past",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						// lock time only works with sequence number != 0xffffffff
						SequenceNumber: 123,
					}},
					LockTime: uint32(time.Now().Add(-10 * time.Minute).Unix()),
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is time in the future",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						// lock time only works with sequence number != 0xffffffff
						SequenceNumber: 123,
					}},
					LockTime: uint32(time.Now().Add(10 * time.Minute).Unix()),
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: false,
		},
		{
			name: "Test IsTransactionFinal - lock time is time in the future with final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 0xffffffff,
					}},
					LockTime: uint32(time.Now().Add(10 * time.Minute).Unix()),
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is time in the past with sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 123,
					}},
					LockTime: uint32(time.Now().Add(-10 * time.Minute).Unix()),
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - lock time is time in the past with final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 0xffffffff,
					}},
					LockTime: uint32(time.Now().Add(10 * time.Minute).Unix()),
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - no lock time with final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 0xffffffff,
					}},
					LockTime: 0,
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
		{
			name: "Test IsTransactionFinal - no lock time with non-final sequence number",
			args: args{
				tx: &bt.Tx{
					Inputs: []*bt.Input{{
						SequenceNumber: 123,
					}},
					LockTime: 0,
				},
				blockHeight:     123,
				medianBlockTime: uint32(time.Now().Unix()),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsTransactionFinal(tt.args.tx, tt.args.blockHeight, tt.args.medianBlockTime)
			if tt.want {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_IsTransactionFinal_FromRequirements(t *testing.T) {
	// Sequence nr							0xfff	!=0xfff

	txFinal := &bt.Tx{Inputs: []*bt.Input{{SequenceNumber: 0xffffffff}}, LockTime: 500000005}
	txNonFinal := &bt.Tx{Inputs: []*bt.Input{{SequenceNumber: 0xfffffff0}}, LockTime: 500000005}

	// Locktime >= 500M, Time lower			TRUE	FALSE
	assert.NoError(t, IsTransactionFinal(txFinal, 123, 500000004))
	assert.True(t, errors.Is(IsTransactionFinal(txNonFinal, 123, 500000004), errors.ErrLockTime))
	// Locktime >= 500M, Time equal			TRUE	TRUE
	assert.NoError(t, IsTransactionFinal(txFinal, 123, 500000005))
	assert.NoError(t, IsTransactionFinal(txNonFinal, 123, 500000005))
	// Locktime >= 500M, Time higher		TRUE	TRUE
	assert.NoError(t, IsTransactionFinal(txFinal, 123, 500000006))
	assert.NoError(t, IsTransactionFinal(txNonFinal, 123, 500000006))

	txFinal = &bt.Tx{Inputs: []*bt.Input{{SequenceNumber: 0xffffffff}}, LockTime: 123}
	txNonFinal = &bt.Tx{Inputs: []*bt.Input{{SequenceNumber: 0xfffffff0}}, LockTime: 123}

	// Locktime < 500M, Block Height lower	TRUE	FALSE
	assert.NoError(t, IsTransactionFinal(txFinal, 122, 500000004))
	assert.True(t, errors.Is(IsTransactionFinal(txNonFinal, 122, 500000004), errors.ErrLockTime))
	// Locktime < 500M, Block Height equal	TRUE	TRUE
	assert.NoError(t, IsTransactionFinal(txFinal, 123, 500000005))
	assert.NoError(t, IsTransactionFinal(txNonFinal, 123, 500000005))
	// Locktime < 500M, Block Height higher	TRUE	TRUE
	assert.NoError(t, IsTransactionFinal(txFinal, 124, 500000006))
	assert.NoError(t, IsTransactionFinal(txNonFinal, 124, 500000006))
}
