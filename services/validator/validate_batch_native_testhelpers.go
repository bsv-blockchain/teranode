package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// setBatchStoreForTest installs a batchUtxoStore stub on the Validator,
// bypassing the normal utxoStore type assertion in getBatchUtxoStore.
// This is a test seam — production code never calls this method.
func (v *Validator) setBatchStoreForTest(s batchUtxoStore) {
	v.batchStoreOverride = s
}

// setBlockStateForTest overrides the value returned by GetBlockState so unit
// tests can present a "ready" block state (non-zero MedianTime) without a
// fully-initialised chain. This is a test seam — production never calls it.
func (v *Validator) setBlockStateForTest(bs utxo.BlockState) {
	v.blockStateOverride = &bs
}

// overrideCPUValidationForTest installs a function that replaces the real
// TxValidator CPU-validation calls inside runCPUValidation. This allows
// tests to inject controlled per-tx failures without constructing a fully
// extended, signed transaction. Production code never calls this method.
func (v *Validator) overrideCPUValidationForTest(fn func(*bt.Tx) error) {
	v.cpuOverride = fn
}

// overrideBASubmitForTest installs a function that replaces the real
// submitToBlockAssemblyBatch implementation. Allows unit tests to control
// per-tx BA accept/reject without a live BlockAssembly service.
// Production code never calls this method.
func (v *Validator) overrideBASubmitForTest(fn func(ctx context.Context, txs []*bt.Tx) map[chainhash.Hash]error) {
	v.blockAssemblySubmitOverride = fn
}

// overrideTxMetaPublishForTest installs a function that replaces the real
// sendTxMetaToKafka call inside Phase F of ValidateBatch. Allows unit
// tests to capture which txs are published without a live Kafka producer.
// Production code never calls this method.
func (v *Validator) overrideTxMetaPublishForTest(fn func(tx *bt.Tx, m *meta.Data)) {
	v.txMetaPublishOverride = fn
}

// OverrideCPUValidationForBench is a test/benchmark seam exposing the
// internal cpuOverride field to external packages. Use only from
// _test.go / bench files. Production code MUST NOT call this.
func (v *Validator) OverrideCPUValidationForBench(fn func(*bt.Tx) error) {
	v.overrideCPUValidationForTest(fn)
}

// OverrideBASubmitForBench is a test/benchmark seam exposing the
// internal blockAssemblySubmitOverride field to external packages. Use only
// from _test.go / bench files. Production code MUST NOT call this.
func (v *Validator) OverrideBASubmitForBench(fn func(ctx context.Context, txs []*bt.Tx) map[chainhash.Hash]error) {
	v.overrideBASubmitForTest(fn)
}

// OverrideTxMetaPublishForBench is a test/benchmark seam exposing the
// internal txMetaPublishOverride field to external packages. Use only from
// _test.go / bench files. Production code MUST NOT call this.
func (v *Validator) OverrideTxMetaPublishForBench(fn func(tx *bt.Tx, m *meta.Data)) {
	v.overrideTxMetaPublishForTest(fn)
}
