package catchup

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestParseBlockHeaders(t *testing.T) {
	type args struct {
		headerHex string
	}

	hashPrevBlock, err := chainhash.NewHashFromStr("00000000000000000a10035612d303b96fa7528b30c7e8d81df76f4095f627e5")
	assert.NoError(t, err)
	hashMerkleRoot, err := chainhash.NewHashFromStr("d093e016213aceed8c5ebd46bfae2a40158d725c38b559bfffa8fcfddae9a8cd")
	assert.NoError(t, err)
	bits, err := hex.DecodeString("ebc22118")
	assert.NoError(t, err)

	// Example valid header
	var validSingleHeader = &model.BlockHeader{
		Version:        973078528,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      1756169335,
		Bits:           model.NBit(bits),
		Nonce:          600846451,
	}

	tests := []struct {
		name    string
		args    args
		want    []*model.BlockHeader
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "Valid single header",
			args: args{
				headerHex: "0000003a" + // Version
					"e527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000" + // Previous block
					"cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d0" + // Merkle root
					"7704ad68" + // Timestamp
					"ebc22118" + // Bits
					"7330d023", // Nonce

			},
			want: []*model.BlockHeader{
				validSingleHeader,
			},
			wantErr: assert.NoError,
		},
		{
			name: "Valid multiple headers",
			args: args{
				headerHex: "0000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d0230000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d023",
			},
			want: []*model.BlockHeader{
				validSingleHeader,
				validSingleHeader,
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid multiple header length",
			args: args{
				headerHex: "0000003ae7f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d0230000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d023",
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "invalid single header length",
			args: args{
				headerHex: "00000033edce3a2116e093d07704ad68ebc221187330d023",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerBytes, err := hex.DecodeString(tt.args.headerHex)
			if err != nil {
				t.Fatalf("Failed to decode header hex: %v", err)
			}
			got, err := ParseBlockHeaders(headerBytes)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseBlockHeaders(%x)", tt.args.headerHex)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ParseBlockHeaders(%x)", tt.args.headerHex)
		})
	}
}
