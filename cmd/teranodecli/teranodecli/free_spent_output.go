package teranodecli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

const freeSpentOutputLongHelp = `Frees one output of the configured UTXO store that is still spent by a transaction this
store holds no record of. That is the state an earlier failed spend can leave behind, and once the
spend is half the pruning retention old (144 blocks at the default 288), a block that mines the
spender is refused with error 78 (ERR_UTXO_SPENDING_TX_PRUNED) on every retry. Read "When a spend
is refused with error 78" in docs/topics/stores/utxo.md first: it shows how to tell that stall from
a real replay of a pruned transaction, which must stay refused.

The command refuses to act when the spender's record exists, because the output is then spent by a
stored transaction and freeing it would let a second spender take it. It frees the output only while
the output still names --spender: the store's Unspend checks the stored spend before clearing it, so
a spend that changed in the meantime is left alone. Afterwards it reads the output back and fails if
the spend is still there. The node can keep running; the parked block is retried on its own.`

type freeSpentOutputFlags struct {
	parent  *string
	vout    *uint
	spender *string
}

func registerFreeSpentOutputFlags(fs *flag.FlagSet) *freeSpentOutputFlags {
	return &freeSpentOutputFlags{
		parent:  fs.String("parent", "", "Transaction id of the parent whose output is spent (required)"),
		vout:    fs.Uint("vout", 0, "Output index in the parent"),
		spender: fs.String("spender", "", "Transaction id the output names as its spender (required)"),
	}
}

// freeSpentOutputExecute opens the configured UTXO store and frees the output the flags name.
func freeSpentOutputExecute(logger ulogger.Logger, tSettings *settings.Settings, f *freeSpentOutputFlags, out io.Writer) func(args []string) error {
	return func(args []string) error {
		if *f.parent == "" || *f.spender == "" {
			return errors.NewInvalidArgumentError("--parent and --spender are required")
		}

		if *f.vout > math.MaxUint32 {
			return errors.NewInvalidArgumentError("--vout %d is above %d", *f.vout, uint64(math.MaxUint32))
		}

		parent, err := chainhash.NewHashFromStr(*f.parent)
		if err != nil {
			return errors.NewInvalidArgumentError("--parent is not a transaction id", err)
		}

		spender, err := chainhash.NewHashFromStr(*f.spender)
		if err != nil {
			return errors.NewInvalidArgumentError("--spender is not a transaction id", err)
		}

		ctx := context.Background()

		store, err := utxofactory.NewStore(ctx, logger, tSettings, "free-spent-output", false)
		if err != nil {
			return errors.NewStorageError("failed to open the UTXO store", err)
		}

		defer func() { _ = store.Close(ctx) }()

		return freeSpentOutput(ctx, store, parent, uint32(*f.vout), spender, out) //nolint:gosec // bounded above
	}
}

// freeSpentOutput frees parent:vout when it is spent by spender and spender has no record.
func freeSpentOutput(ctx context.Context, store utxostore.Store, parent *chainhash.Hash, vout uint32, spender *chainhash.Hash, out io.Writer) error {
	if _, err := store.Get(ctx, spender, fields.TxID); err == nil {
		return errors.NewProcessingError("spender %s has a record in this store, so %s:%d is spent by a stored transaction; refusing to free it", spender, parent, vout)
	} else if !errors.Is(err, errors.ErrTxNotFound) {
		return errors.NewStorageError("failed to look up spender %s", spender, err)
	}

	parentMeta, err := store.Get(ctx, parent, fields.Tx)
	if err != nil {
		return errors.NewStorageError("failed to read parent %s", parent, err)
	}

	if parentMeta.Tx == nil || int(vout) >= len(parentMeta.Tx.Outputs) {
		return errors.NewInvalidArgumentError("parent %s has no output %d", parent, vout)
	}

	utxoHash, err := util.UTXOHashFromOutput(parent, parentMeta.Tx.Outputs[vout], vout)
	if err != nil {
		return errors.NewProcessingError("failed to hash %s:%d", parent, vout, err)
	}

	spend := &utxostore.Spend{TxID: parent, Vout: vout, UTXOHash: utxoHash}

	current, err := store.GetSpend(ctx, spend)
	if err != nil {
		return errors.NewStorageError("failed to read the spend of %s:%d", parent, vout, err)
	}

	if current.SpendingData == nil || current.SpendingData.TxID == nil || !current.SpendingData.TxID.IsEqual(spender) {
		return errors.NewProcessingError("%s:%d is not spent by %s (it records %s); nothing freed", parent, vout, spender, describeSpender(current))
	}

	spend.SpendingData = current.SpendingData

	if err = store.Unspend(ctx, []*utxostore.Spend{spend}); err != nil {
		return errors.NewStorageError("failed to free %s:%d", parent, vout, err)
	}

	after, err := store.GetSpend(ctx, spend)
	if err != nil {
		return errors.NewStorageError("freed %s:%d but could not read it back", parent, vout, err)
	}

	if after.SpendingData != nil {
		return errors.NewProcessingError("%s:%d still records %s after the unspend; the spend changed while the command ran", parent, vout, describeSpender(after))
	}

	_, _ = fmt.Fprintf(out, "Freed %s:%d, which was spent by %s (input %d). A parked block that mines %s is retried on its own.\n",
		parent, vout, spender, current.SpendingData.Vin, spender)

	return nil
}

func describeSpender(resp *utxostore.SpendResponse) string {
	if resp == nil || resp.SpendingData == nil || resp.SpendingData.TxID == nil {
		return "no spend"
	}

	return fmt.Sprintf("spender %s input %d", resp.SpendingData.TxID, resp.SpendingData.Vin)
}
