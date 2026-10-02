package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// BlockBatchChecker is implemented by the in-process Validator only. Block
// validation uses it to check a whole list of already-extended transactions on
// all cores, and then writes them itself with utxo.Store.SpendAndCreateMulti.
// The gRPC client does not implement it: the resolved previous outputs a check
// depends on do not survive the wire, so remote callers keep ValidateWithOptions.
type BlockBatchChecker interface {
	// CheckExtendedTransaction runs every check ValidateWithOptions runs on tx,
	// and nothing else: it reads no parent and writes nothing. Every input must
	// already carry its previous output's value and locking script, taken from
	// the store or from a same-block parent keyed by a txid the caller computed.
	// utxoHeights holds each input's parent height, with the unconfirmed-parent
	// sentinel for a parent not yet mined, one entry per input.
	CheckExtendedTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error

	// PublishTxMeta publishes the metadata of a transaction the caller created
	// in the UTXO store, as ValidateWithOptions publishes it after a create.
	PublishTxMeta(txMeta *meta.Data, txHash *chainhash.Hash, inBlock bool)
}

var _ BlockBatchChecker = (*Validator)(nil)

// CheckExtendedTransaction implements BlockBatchChecker.
func (v *Validator) CheckExtendedTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	ctx, _, deferFn := tracing.Tracer("validator").Start(ctx, "CheckExtendedTransaction")
	defer deferFn()

	txID := tx.TxIDChainHash().String()

	if validationOptions.OutpointOnlySpend {
		return errors.NewProcessingError("[CheckExtendedTransaction][%s] OutpointOnlySpend is not supported", txID)
	}

	if len(utxoHeights) != len(tx.Inputs) {
		return errors.NewProcessingError("[CheckExtendedTransaction][%s] %d parent heights for %d inputs", txID, len(utxoHeights), len(tx.Inputs))
	}

	// The caller extends; the check never does. validateTransaction would
	// otherwise extend an unextended transaction from the store.
	for i, in := range tx.Inputs {
		if in.PreviousTxScript == nil {
			return errors.NewProcessingError("[CheckExtendedTransaction][%s] input %d is not extended", txID, i)
		}
	}

	tx.SetExtended(true)

	blockState := v.GetBlockState()

	if blockHeight == 0 {
		blockHeight = blockState.Height + 1
	}

	if err := v.precheckTransaction(tx, txID, blockHeight, blockState, validationOptions); err != nil {
		return err
	}

	// validateTransaction substitutes the unconfirmed-parent sentinel in place
	// when UnconfirmedParentsAtCandidateHeight is set, so work on a copy.
	heights := append([]uint32(nil), utxoHeights...)

	if err := v.validateTransaction(ctx, tx, blockHeight, heights, validationOptions); err != nil {
		return errors.NewProcessingError("[Validate][%s] error validating transaction", txID, err)
	}

	return nil
}

// PublishTxMeta implements BlockBatchChecker.
func (v *Validator) PublishTxMeta(txMeta *meta.Data, txHash *chainhash.Hash, inBlock bool) {
	if v.txmetaKafkaProducerClient == nil || txMeta == nil {
		return
	}

	txMeta.InBlock = inBlock

	if err := v.sendTxMetaToKafka(txMeta, txHash); err != nil {
		v.logger.Errorf("[PublishTxMeta][%s] failed to serialize/enqueue txmeta for kafka: %v", txHash, err)
	}
}
