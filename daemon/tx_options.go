package daemon

import (
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
)

// TxOption is a function that modifies a transaction creation options
type TxOption func(*TxOptions)

type input struct {
	tx      *bt.Tx
	vout    uint32
	privKey *bec.PrivateKey
}

type output struct {
	script *bscript.Script
	pubKey *bec.PublicKey
	amount uint64
}

// TxOptions holds all the configurable options for transaction creation
type TxOptions struct {
	inputs    []input
	outputs   []output
	skipCheck bool
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

func WithSkipCheck() TxOption {
	return func(opts *TxOptions) {
		opts.skipCheck = true
	}
}
