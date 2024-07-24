package sql

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestSQL_LocateBlockHashes(t *testing.T) {
	dbUrl, _ := url.Parse("sqlitememory:///")

	type args struct {
		ctx       context.Context
		locator   []*chainhash.Hash
		hashStop  *chainhash.Hash
		maxHashes uint32
	}
	tests := []struct {
		name    string
		args    args
		want    []*chainhash.Hash
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
			want:    []*chainhash.Hash{},
			wantErr: assert.NoError,
		},
		{
			name: "TestSQL_LocateBlockHashes genesis",
			args: args{
				ctx:       context.Background(),
				locator:   []*chainhash.Hash{model.GenesisBlockHeader.Hash()},
				hashStop:  model.GenesisBlockHeader.Hash(),
				maxHashes: 64,
			},
			want:    []*chainhash.Hash{model.GenesisBlockHeader.Hash()},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(ulogger.TestLogger{}, dbUrl)
			require.NoError(t, err)
			got, err := s.LocateBlockHashes(tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)
			if !tt.wantErr(t, err, fmt.Sprintf("LocateBlockHashes(%v, %v, %v, %v)", tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)) {
				return
			}
			assert.Equalf(t, tt.want, got, "LocateBlockHashes(%v, %v, %v, %v)", tt.args.ctx, tt.args.locator, tt.args.hashStop, tt.args.maxHashes)
		})
	}
}
