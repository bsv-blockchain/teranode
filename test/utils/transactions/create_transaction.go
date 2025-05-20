package transactions

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

const coinbaseSequenceNumber uint32 = 0xFFFFFFFF

// TxOption is a function that modifies a transaction creation options
type TxOption func(*TxOptions)

type input struct {
	tx              *bt.Tx
	vout            uint32
	privKey         *bec.PrivateKey
	unlockingScript *bscript.Script
	sequenceNumber  uint32
}

type output struct {
	script *bscript.Script
	pubKey *bec.PublicKey
	amount uint64
}

// TxOptions holds all the configurable options for transaction creation
type TxOptions struct {
	ctx             context.Context
	fallbackPrivKey *bec.PrivateKey
	inputs          []input
	outputs         []output
	isCoinbase      bool
	changeOutput    *output
}

func WithContextForSigning(ctx context.Context) TxOption {
	return func(opts *TxOptions) {
		opts.ctx = ctx
	}
}

// WithPrivateKey specifies a fallback private key to use for signing inputs and creating P2PKH outputs
// when no specific key is provided. This is optional - keys can also be provided per-input or per-p2pkh
// output.
func WithPrivateKey(privKey *bec.PrivateKey) TxOption {
	return func(opts *TxOptions) {
		opts.fallbackPrivKey = privKey
	}
}

// WithInput specifies an input to use.  You can add this option multiple times to add multiple inputs.
func WithInput(tx *bt.Tx, vout uint32, priv ...*bec.PrivateKey) TxOption {
	var p *bec.PrivateKey
	if len(priv) > 0 {
		p = priv[0]
	}

	return func(opts *TxOptions) {
		opts.inputs = append(opts.inputs, input{tx: tx, vout: vout, privKey: p})
	}
}

func WithCoinbaseData(blockHeight uint32, minerInfo string) TxOption {
	return func(opts *TxOptions) {
		opts.isCoinbase = true

		blockHeightBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(blockHeightBytes, blockHeight)

		arbitraryData := make([]byte, 0)
		arbitraryData = append(arbitraryData, 0x03)
		arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
		arbitraryData = append(arbitraryData, []byte(minerInfo)...)

		opts.inputs = append(opts.inputs, input{
			sequenceNumber:  coinbaseSequenceNumber,
			unlockingScript: bscript.NewFromBytes(arbitraryData),
		})
	}
}

// WithOpReturnData specifies exact data to use in the OP_RETURN output
func WithOpReturnData(data []byte) TxOption {
	return func(opts *TxOptions) {
		length := len(data) + 2 // + 2 bytes for OP_FALSE and OP_RETURN

		lengthBytes := bt.VarInt(uint64(length)).Bytes() //nolint: gosec

		b := make([]byte, 0, length+len(lengthBytes))

		b = append(b, lengthBytes...)
		b = append(b, 0x00) // OP_FALSE
		b = append(b, 0x6a) // OP_RETURN
		b = append(b, data...)

		opts.outputs = append(opts.outputs, output{script: bscript.NewFromBytes(b), amount: 0})
	}
}

func WithOpReturnSize(size int) TxOption {
	// Create empty data of the specified size
	data := make([]byte, size)

	// Delegate to WithOpReturnData
	return WithOpReturnData(data)
}

func WithOutput(amount uint64, script *bscript.Script) TxOption {
	return func(opts *TxOptions) {
		opts.outputs = append(opts.outputs, output{script: script, amount: amount})
	}
}

// WithAdditionalOutputs specifies additional outputs to use.  You can add this option multiple times to add multiple outputs.
func WithP2PKHOutputs(numOutputs int, amount uint64, pubKey ...*bec.PublicKey) TxOption {
	var p *bec.PublicKey
	if len(pubKey) > 0 {
		p = pubKey[0]
	}

	return func(opts *TxOptions) {
		for i := 0; i < numOutputs; i++ {
			opts.outputs = append(opts.outputs, output{pubKey: p, amount: amount})
		}
	}
}

func WithChangeOutput(pubKey ...*bec.PublicKey) TxOption {
	var p *bec.PublicKey
	if len(pubKey) > 0 {
		p = pubKey[0]
	}

	return func(opts *TxOptions) {
		opts.changeOutput = &output{pubKey: p}
	}
}

// Create creates a new transaction with configurable options.
// At least one parent transaction must be provided using WithParentTx or WithParentTxs.
func Create(t *testing.T, options ...TxOption) *bt.Tx {
	// Initialize options with defaults
	opts := &TxOptions{}

	// Apply all provided options
	for _, option := range options {
		option(opts)
	}

	// Ensure we have at least one parent transaction
	require.GreaterOrEqual(t, len(opts.inputs), 1, "No inputs - need at least one input")
	require.GreaterOrEqual(t, len(opts.outputs), 1, "No outputs - need at least one output")

	tx := bt.NewTx()

	var totalAmount uint64

	for _, input := range opts.inputs {
		if !opts.isCoinbase && input.privKey == nil && opts.fallbackPrivKey == nil {
			require.Fail(t, "No private key provided for input and no fallback private key set")
		}

		if input.tx != nil {
			err := tx.FromUTXOs(&bt.UTXO{
				TxIDHash:      input.tx.TxIDChainHash(),
				Vout:          input.vout,
				LockingScript: input.tx.Outputs[input.vout].LockingScript,
				Satoshis:      input.tx.Outputs[input.vout].Satoshis,
			})
			require.NoError(t, err)

			totalAmount += input.tx.Outputs[input.vout].Satoshis
		} else {
			input := &bt.Input{
				SequenceNumber:     input.sequenceNumber,
				UnlockingScript:    input.unlockingScript,
				PreviousTxSatoshis: 0,
				PreviousTxOutIndex: coinbaseSequenceNumber,
			}
			zeroHash := new(chainhash.Hash)
			err := input.PreviousTxIDAdd(zeroHash)
			require.NoError(t, err)

			tx.Inputs = append(tx.Inputs, input)
		}
	}

	// Create a bt.Output for each opts.output.
	// If a script is not nil, use it
	// If a pubKey is not nil, create a P2PKH script from it
	// If both are nil, use the public key from the default private key
	for _, output := range opts.outputs {
		if !opts.isCoinbase {
			require.GreaterOrEqual(t, totalAmount, output.amount, "output amount %d is greater than total input amount %d", output.amount, totalAmount)
		}

		script := output.script

		if script == nil {
			var err error

			pubKey := output.pubKey
			if pubKey == nil {
				require.NotNil(t, opts.fallbackPrivKey, "no public key provided for output and no default private key set")
				pubKey = opts.fallbackPrivKey.PubKey()
			}

			script, err = bscript.NewP2PKHFromPubKeyBytes(pubKey.SerialiseCompressed())
			require.NoError(t, err)
		}

		tx.AddOutput(&bt.Output{
			Satoshis:      output.amount,
			LockingScript: script,
		})

		totalAmount -= output.amount
	}

	if opts.changeOutput != nil {
		feeQuote := bt.NewFeeQuote()

		pubKey := opts.changeOutput.pubKey
		if pubKey == nil {
			require.NotNil(t, opts.fallbackPrivKey, "no public key provided for output and no default private key set")
			pubKey = opts.fallbackPrivKey.PubKey()
		}

		script, err := bscript.NewP2PKHFromPubKeyBytes(pubKey.SerialiseCompressed())
		require.NoError(t, err)

		require.NoError(t, tx.Change(script, feeQuote))
	}

	if !opts.isCoinbase {
		// Sign the transaction with each private key that we know about
		privKeys := make(map[*bec.PrivateKey]struct{})

		for _, input := range opts.inputs {
			if input.privKey != nil {
				privKeys[input.privKey] = struct{}{}
			} else {
				require.NotNil(t, opts.fallbackPrivKey, "no private key provided for input and no default private key set")

				privKeys[opts.fallbackPrivKey] = struct{}{}
			}
		}

		ctx := opts.ctx
		if ctx == nil {
			ctx = context.Background()
		}

		for privKey := range privKeys {
			err := tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privKey})
			require.NoError(t, err)
		}

		// Check the transaction is signed
		for i, input := range tx.Inputs {
			require.GreaterOrEqual(t, len(*input.UnlockingScript), 1, "Input %d is not signed", i)
		}
	}

	return tx
}
