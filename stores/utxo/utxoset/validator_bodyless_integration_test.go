package utxoset

import (
	"testing"

	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidatorFillsBodylessParentFromCoinTable is the one cross-package proof in this
// change: everything else exercising getUtxoBlockHeightAndExtendForParentTx's coin-table
// fallback (services/validator package) does so against utxo.MockUtxostore, asserting only
// that the validator calls PreviousOutputsDecorate correctly. This test instead runs a REAL
// *validator.Validator against a REAL *utxoset.Store backed by the Postgres testcontainer
// (see main_test.go), so the thing actually asserted is that this store's own
// BatchPreviousOutputsDecorate SQL (decorate.go) — not a mock standing in for it — is what
// makes an honest spend of a body-less parent validate end to end: signature check, value
// conservation, and all.
//
// The parent is mined below the store's checkpoint floor with
// utxostore_skipTxBodyBelowCheckpoint on, so s.Get answers Tx: nil for it, exactly as it does
// on mainnet below the real 945,000 checkpoint — the case this whole change exists for.
func TestValidatorFillsBodylessParentFromCoinTable(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("utxoset bodyless coin-table fallback test key32"))

	parent := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/utxoset bodyless fallback/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)

	// parentHeight leaves 150 blocks of headroom below checkpointFloor: enough for the
	// store's own (default, 100-block) coinbase maturity rule to clear by spendHeight,
	// while parentHeight itself stays comfortably at-or-below the checkpoint so the
	// body is skipped (see newCheckpointStore / tx_body_checkpoint_test.go).
	const parentHeight = checkpointFloor - 150
	const spendHeight = parentHeight + 101

	require.NoError(t, s.SetBlockHeight(spendHeight-1))
	require.NoError(t, s.SetMedianBlockTime(1_700_000_000))

	_, err := s.Create(ctx, parent, parentHeight,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: parentHeight, OnLongestChain: true}))
	require.NoError(t, err)
	require.Equal(t, 0, bodyRows(t, s, ctx, parent),
		"the parent must be body-less for this test to actually exercise the coin-table fallback")

	child := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	v, err := validator.New(ctx, logger, tSettings, s,
		kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(),
		nil, nil)
	require.NoError(t, err)

	_, err = v.ValidateWithOptions(ctx, child, spendHeight, &validator.Options{AddTXToBlockAssembly: false})
	require.NoError(t, err, "an honest spend of a body-less parent must validate via the real store's coin-table fallback")
}
