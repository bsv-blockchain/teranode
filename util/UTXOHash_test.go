package util

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

var previousTxScript, _ = hex.DecodeString("76a914d687c76d6ee133c9cc42bd96e3947d8a84bdf60288ac")
var hash, _ = chainhash.NewHashFromStr("8ef53bb4c9c4b849c30ec75243bad8a7eafd83f407407a154a6d9ec80d83dd00")

func Test_getInputUtxoHash(t *testing.T) {
	type args struct {
		input *bt.Input
	}

	input1 := &bt.Input{
		PreviousTxSatoshis: 11.4999616 * 1e8,
		PreviousTxScript:   bscript.NewFromBytes(previousTxScript),
		PreviousTxOutIndex: 0,
		SequenceNumber:     4294967294,
	}
	_ = input1.PreviousTxIDAddStr("2fb09ea4d1d282f55b4f4b5b1eec92fa314e1ba5a5a009e897f63d155b4dba82")

	tests := []struct {
		name    string
		args    args
		want    *chainhash.Hash
		wantErr bool
	}{
		{
			name: "input 2fb09ea4d1d282f55b4f4b5b1eec92fa314e1ba5a5a009e897f63d155b4dba82:0",
			args: args{
				input: input1,
			},
			want:    hash,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UTXOHashFromInput(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetInputUtxoHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetInputUtxoHash() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOutputUtxoHash(t *testing.T) {
	type args struct {
		txID   []byte
		output *bt.Output
		vOut   uint32
	}

	txID, _ := utils.DecodeAndReverseHexString("2fb09ea4d1d282f55b4f4b5b1eec92fa314e1ba5a5a009e897f63d155b4dba82")

	tests := []struct {
		name    string
		args    args
		want    *chainhash.Hash
		wantErr bool
	}{
		{
			name: "output 2fb09ea4d1d282f55b4f4b5b1eec92fa314e1ba5a5a009e897f63d155b4dba82:0",
			args: args{
				txID: txID,
				output: &bt.Output{
					Satoshis:      11.4999616 * 1e8,
					LockingScript: bscript.NewFromBytes(previousTxScript),
				},
				vOut: 0,
			},
			want:    hash,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txid, _ := chainhash.NewHash(tt.args.txID)
			got, err := UTXOHashFromOutput(txid, tt.args.output, tt.args.vOut)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetOutputUtxoHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetOutputUtxoHash() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollision(t *testing.T) {
	// tx id 955c4de19e3428a2620891dad1f4f2f7e5948c6a61b1602372fee47370bb23c2
	script, err := bscript.NewFromHexString("76a9146d6baa116674e3ccc2afef914145f0d020a0366088ac")
	require.NoError(t, err)
	input1 := &bt.Input{
		PreviousTxSatoshis: 100000,
		PreviousTxScript:   script,
		PreviousTxOutIndex: 0,
		SequenceNumber:     4294967295,
	}
	_ = input1.PreviousTxIDAddStr("d59b5e09a3a62166ac262c04d7c6a5cde05a40e82427144bf8060b21b39861b5")

	utxoHash1, _ := UTXOHashFromInput(input1)

	// tx id 2c27c886af61c5e0be4e47cae793864870874ca067d34a7fe012b177a811ee09
	script, err = bscript.NewFromHexString("76a9143cadb60136ecb6e9dde1c3d02d6642a78540c14b88ac")
	require.NoError(t, err)
	input2 := &bt.Input{
		PreviousTxSatoshis: 100000,
		PreviousTxScript:   script,
		PreviousTxOutIndex: 0,
		SequenceNumber:     4294967295,
	}
	_ = input2.PreviousTxIDAddStr("3cc3306864bb9e89ff5c0d0ed7208223c0203c6195cb1cad09c53e425c2c7e9c")

	utxoHash2, _ := UTXOHashFromInput(input2)

	assert.NotEqual(t, utxoHash1, utxoHash2, "Collision detected for different inputs")
	t.Logf("utxoHash1: %s", utxoHash1.String())
	t.Logf("utxoHash2: %s", utxoHash2.String())
}
