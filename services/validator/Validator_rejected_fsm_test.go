package validator

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidate_RejectedTxPublishedOnlyInRunning pins the rejected-tx gate in
// ValidateWithOptions. IDLE is suppressed: an operator STOP can park a node while
// its catchup batch is still validating historical transactions.
func TestValidate_RejectedTxPublishedOnlyInRunning(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	for _, tt := range []struct {
		state     blockchain.FSMStateType
		published int
	}{
		{blockchain.FSMStateRUNNING, 1},
		{blockchain.FSMStateCATCHINGBLOCKS, 0},
		{blockchain.FSMStateIDLE, 0},
	} {
		t.Run(tt.state.String(), func(t *testing.T) {
			tx, err := bt.NewTxFromString("010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000")
			require.NoError(t, err)

			// Outputs exceeding inputs fail the sanity check with ErrTxInvalid.
			for _, output := range tx.Outputs {
				output.Satoshis = tx.Inputs[0].PreviousTxSatoshis * 2
			}

			utxoStore, _ := nullstore.NewNullStore()
			_ = utxoStore.SetBlockHeight(257727)
			//nolint:gosec
			_ = utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

			tSettings := settings.NewSettings()
			tSettings.ChainCfgParams = &chaincfg.MainNetParams

			state := tt.state
			blockchainClient := &blockchain.Mock{}
			blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)
			rejected := kafka.NewKafkaAsyncProducerMock()

			v := &Validator{
				logger:                        ulogger.TestLogger{},
				settings:                      tSettings,
				txValidator:                   NewTxValidator(ulogger.TestLogger{}, tSettings),
				utxoStore:                     utxoStore,
				blockchainClient:              blockchainClient,
				stats:                         gocore.NewStat("validator"),
				txmetaKafkaProducerClient:     kafka.NewKafkaAsyncProducerMock(),
				rejectedTxKafkaProducerClient: rejected,
			}

			_, err = v.Validate(context.Background(), tx, 100, WithSkipPolicyChecks(false))
			require.Error(t, err)
			require.Equal(t, tt.published, len(rejected.PublishChannel()))
		})
	}
}
