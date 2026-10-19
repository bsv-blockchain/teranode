package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestBlockValidEnforcesPowLimit(t *testing.T) {
	for _, params := range []*chaincfg.Params{&chaincfg.MainNetParams, &chaincfg.RegressionNetParams, &chaincfg.StnParams} {
		t.Run(params.Name, func(t *testing.T) {
			block := &Block{Header: &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Bits: nBitFor(t, "207fffff")}}
			for {
				if valid, _, _ := block.Header.HasMetTargetDifficulty(); valid {
					break
				}
				block.Header.Nonce++
			}
			cfg := settings.NewSettings()
			cfg.ChainCfgParams = params
			// No coinbase or stores: a valid network target must advance to the body gate.
			valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, nil, nil, nil, nil, nil, cfg, nil)
			require.False(t, valid)
			if params == &chaincfg.MainNetParams {
				require.ErrorContains(t, err, "network proof-of-work limit")
			} else {
				require.ErrorContains(t, err, "coinbase")
			}
		})
	}
}
