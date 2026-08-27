package aerospike_test

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestExternalOutputsOnlyParentIsReadableForExtension covers the second consumer
// of the UTXO-set reconstruction: the validator, extending a non-extended
// transaction from its parent's outputs.
//
// The configuration is the snapshot-seeded parent. cmd/seeder builds an
// input-less transaction; create.go marks it external once its outputs exceed
// MaxTxSizeInStoreInBytes and then writes ONLY a .outputs blob for it, never a
// .tx — the two are exclusive. services/validator/Validator.go asks for such a
// parent with fields.Tx so it can read Outputs[vout], and it nil-guards both the
// slice and the element, so the reconstruction is exactly what it needs.
//
// This is the case no other test in the repo covers, which is why the suite
// stayed green through a change that made this read fail. It asserts the store
// boundary — the Get the validator issues — not the validator end to end.
func TestExternalOutputsOnlyParentIsReadableForExtension(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	// Own the external store so the test can inspect which representation was
	// written rather than assuming it.
	ext := memory.New()
	store.SetExternalStore(ext)

	// Input-less tx (what cmd/seeder builds), with enough outputs to pass the
	// 32KB threshold so create.go stores it externally.
	ls, err := bscript.NewFromHexString("76a914" + strings.Repeat("00", 20) + "88ac")
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	for i := 0; i < 1500; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: uint64(1000 + i), LockingScript: ls})
	}

	require.Empty(t, tx.Inputs, "seeded parent must be input-less")

	txHash := tx.TxIDChainHash()

	_, err = store.Create(ctx, tx, 100)
	require.NoError(t, err, "create input-less external tx")

	// The premise: .outputs written, .tx absent. If this ever stops holding, the
	// scenario below stops being the one we mean to cover.
	hasOutputs, err := ext.Exists(ctx, txHash[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)

	hasTx, err := ext.Exists(ctx, txHash[:], fileformat.FileTypeTx)
	require.NoError(t, err)

	require.True(t, hasOutputs, "premise: seeded parent must have a .outputs blob")
	require.False(t, hasTx, "premise: seeded parent must NOT have a .tx blob")

	// Exactly the read services/validator/Validator.go issues to extend a child:
	//   f := []fields.FieldName{fields.BlockIDs, fields.BlockHeights}
	//   if extend { f = append(f, fields.Tx) }
	//   v.utxoStore.Get(gCtx, &parentTxHash, f...)
	md, err := store.Get(ctx, txHash, fields.BlockIDs, fields.BlockHeights, fields.Tx)
	require.NoError(t, err, "the extension path must be able to read a seeded parent")
	require.NotNil(t, md.Tx, "parent tx must be non-nil for extension")

	// What the extend loop actually consumes.
	require.NotNil(t, md.Tx.Outputs[0], "parent output 0 must be readable for extension")
	require.Equal(t, uint64(1000), md.Tx.Outputs[0].Satoshis)
	require.NotNil(t, md.Tx.Outputs[0].LockingScript)

	// And the reconstruction must still be recognisable as one, so the consumers
	// that serialize keep refusing it.
	require.False(t, md.TxIsSerializable(),
		"a snapshot reconstruction must not pass the gate that serializing consumers use")
}
