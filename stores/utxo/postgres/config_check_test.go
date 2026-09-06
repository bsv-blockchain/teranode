package postgres

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestCheckBatcherPoolConfig(t *testing.T) {
	tests := []struct {
		name          string
		maxConns      int32
		maxConcurrent int
		numBatchers   int
		wantWarn      bool
		wantErr       bool
	}{
		{
			name:          "uncapped batcher relies on pool back-pressure",
			maxConns:      80,
			maxConcurrent: 0,
			numBatchers:   4,
			wantWarn:      false,
			wantErr:       false,
		},
		{
			name:          "ample headroom is silent",
			maxConns:      256,
			maxConcurrent: 32,
			numBatchers:   4,
			wantWarn:      false,
			wantErr:       false,
		},
		{
			name:          "default config warns about thin headroom",
			maxConns:      80,
			maxConcurrent: 64,
			numBatchers:   4,
			wantWarn:      true,
			wantErr:       false,
		},
		{
			name:          "single batcher exceeding pool is a hard error",
			maxConns:      100,
			maxConcurrent: 512,
			numBatchers:   4,
			wantWarn:      false,
			wantErr:       true,
		},
		{
			name:          "maxConcurrent equal to pool passes hard-fail but warns",
			maxConns:      64,
			maxConcurrent: 64,
			numBatchers:   4,
			wantWarn:      true,
			wantErr:       false,
		},
		{
			name:          "sum within pool is silent",
			maxConns:      300,
			maxConcurrent: 64,
			numBatchers:   4,
			wantWarn:      false,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warn, err := checkBatcherPoolConfig(tt.maxConns, tt.maxConcurrent, tt.numBatchers)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.wantWarn {
				require.NotEmpty(t, warn)
			} else {
				require.Empty(t, warn)
			}
		})
	}
}

// TestNew_RejectsUnsafeBatcherPoolConfig verifies the check is wired into New() and
// fails fast — before any connection is opened — on a deadlock-prone config. The DSN
// host is never dialled because New must return the configuration error first.
func TestNew_RejectsUnsafeBatcherPoolConfig(t *testing.T) {
	storeURL, err := url.Parse("postgres://teranode:teranode@127.0.0.1:1/teranode?pool_max_conns=100")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.BatcherMaxConcurrent = 512 // > pool_max_conns=100

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.Error(t, err)
	require.Nil(t, store)
	require.Contains(t, err.Error(), "batcherMaxConcurrent")
}
