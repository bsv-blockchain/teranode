package sql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var blockCount = 10

func TestSQL_LocateBlockHeaders(t *testing.T) {
	dbURL, _ := url.Parse("sqlitememory:///")

	s, err := New(ulogger.TestLogger{}, dbURL)
	require.NoError(t, err)

	blocks := generateBlocks(t, blockCount)
	for _, block := range blocks {
		_, _, err := s.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	var blockLocator []*model.BlockHeader
	for i := 0; i < blockCount; i++ {
		blockLocator = append(blockLocator, blocks[i].Header)
	}

	locator := []*chainhash.Hash{blocks[9].Hash()}
	lastLocator := blocks[0].Hash()
	expectedBlocks := reverseSlice(blockLocator)

	type args struct {
		ctx       context.Context
		locator   []*chainhash.Hash
		hashStop  *chainhash.Hash
		maxHashes uint32
	}

	var buf bytes.Buffer
	err = chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
	require.NoError(t, err)
	genesisBlock, err := model.NewBlockFromBytes(buf.Bytes())
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	tests := []struct {
		name    string
		args    args
		want    []*model.BlockHeader
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "TestSQL_LocateBlockHashes empty",
			args: args{
				ctx:       context.Background(),
				locator:   nil,
				hashStop:  &chainhash.Hash{},
				maxHashes: 0,
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "TestSQL_LocateBlockHashes not known",
			args: args{
				ctx:       context.Background(),
				locator:   nil,
				hashStop:  &chainhash.Hash{},
				maxHashes: 64,
			},
			want:    []*model.BlockHeader{},
			wantErr: assert.NoError,
		},
		{
			name: "TestSQL_LocateBlockHashes genesis",
			args: args{
				ctx:       context.Background(),
				locator:   []*chainhash.Hash{genesisBlock.Hash()},
				hashStop:  genesisBlock.Hash(),
				maxHashes: 64,
			},
			want:    []*model.BlockHeader{genesisBlock.Header},
			wantErr: assert.NoError,
		},
		{
			name: "TestSQL_LocateBlockHashes for 10 blocks",
			args: args{
				ctx:      context.Background(),
				locator:  locator,
				hashStop: lastLocator,
				// nolint: gosec
				maxHashes: uint32(blockCount),
			},
			want:    expectedBlocks,
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.LocateBlockHeaders(tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)
			if !tt.wantErr(t, err, fmt.Sprintf("LocateBlockHashes(%v, %v, %v, %v)", tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)) {
				return
			}

			assert.Equalf(t, tt.want, got, "LocateBlockHashes(%v, %v, %v, %v)", tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)
		})
	}
}

func reverseSlice(slice []*model.BlockHeader) []*model.BlockHeader {
	for i := 0; i < len(slice)/2; i++ {
		j := len(slice) - 1 - i
		slice[i], slice[j] = slice[j], slice[i]
	}

	return slice
}
