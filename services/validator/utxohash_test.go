package validator

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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
			got, err := getInputUtxoHash(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("getInputUtxoHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInputUtxoHash() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOutputUtxoHash(t *testing.T) {
	type args struct {
		txID   []byte
		output *bt.Output
		vOut   uint64
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
			got, err := getOutputUtxoHash(tt.args.txID, tt.args.output, tt.args.vOut)
			if (err != nil) != tt.wantErr {
				t.Errorf("getOutputUtxoHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getOutputUtxoHash() got = %v, want %v", got, tt.want)
			}
		})
	}
}
