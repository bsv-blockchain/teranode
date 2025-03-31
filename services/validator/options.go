/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements option patterns for both general validation options and
transaction validator-specific options, providing flexible configuration for
validation operations.
*/
package validator

// Options defines the configuration options for validation operations
type Options struct {
	// SkipUtxoCreation determines whether UTXO creation should be skipped
	// When true, the validator won't create new UTXOs for transaction outputs
	SkipUtxoCreation bool

	// AddTXToBlockAssembly determines whether transactions should be added to block assembly
	// When true, validated transactions are forwarded to the block assembly process
	AddTXToBlockAssembly bool

	// SkipPolicyChecks determines whether policy checks should be skipped
	// this is done when validating transaction from a block that has been mined
	SkipPolicyChecks bool

	// CreateConflicting determines whether to allow conflicting transactions
	// this is done when validating transaction from a block that has been mined
	CreateConflicting bool

	// IgnoreConflicting determines whether to ignore transactions marked as conflicting when spending
	IgnoreConflicting bool

	// IgnoreUnspendable determines whether to ignore transactions marked as unspendable when spending
	IgnoreUnspendable bool

	// disableConsensus determine the consensus parameter to execute script interpreter
	// By default it is false, meaning the consensus is true
	disableConsensus bool
}

// Option defines a function type for setting options
// This follows the functional options pattern for flexible configuration
type Option func(*Options)

// NewDefaultOptions creates a new Options instance with default settings
// Default configuration:
//   - skipUtxoCreation: false (UTXOs will be created)
//   - addTXToBlockAssembly: true (transactions will be added to block assembly)
//
// Returns:
//   - *Options: New options instance with default settings
func NewDefaultOptions() *Options {
	return &Options{
		SkipUtxoCreation:     false,
		AddTXToBlockAssembly: true,
		SkipPolicyChecks:     false,
		CreateConflicting:    false,
	}
}

// ProcessOptions applies the provided options to a new Options instance
// Parameters:
//   - opts: Variable number of Option functions to apply
//
// Returns:
//   - *Options: Configured options instance
func ProcessOptions(opts ...Option) *Options {
	options := NewDefaultOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// WithSkipUtxoCreation creates an option to control UTXO creation
// Parameters:
//   - skip: When true, UTXO creation will be skipped
//
// Returns:
//   - Option: Function that sets the skipUtxoCreation option
func WithSkipUtxoCreation(skip bool) Option {
	return func(o *Options) {
		o.SkipUtxoCreation = skip
	}
}

// WithAddTXToBlockAssembly creates an option to control block assembly integration (allows the transaction to be added to the block assembly or not)
// Parameters:
//   - add: When true, transactions will be added to block assembly
//
// Returns:
//   - Option: Function that sets the addTXToBlockAssembly option
func WithAddTXToBlockAssembly(add bool) Option {
	return func(o *Options) {
		o.AddTXToBlockAssembly = add
	}
}

// WithSkipPolicyChecks creates an option to control policy checks
// Parameters:
//   - skip: When true, policy checks will be skipped
//
// Returns:
//   - Option: Function that sets the skipPolicyChecks option
func WithSkipPolicyChecks(skip bool) Option {
	return func(o *Options) {
		o.SkipPolicyChecks = skip
	}
}

// WithCreateConflicting creates an option to control whether a conflicting transaction is created
// Parameters:
//   - create: When true, a conflicting transaction will be created
//
// Returns:
//   - Option: Function that sets the createConflicting option
func WithCreateConflicting(create bool) Option {
	return func(o *Options) {
		o.CreateConflicting = create
	}
}

// WithIgnoreConflicting creates an option to control whether a conflicting transaction is ignored
// Parameters:
//   - ignore: When true, a conflicting transaction will be ignored
//
// Returns:
//   - Option: Function that sets the ignoreConflicting option
func WithIgnoreConflicting(ignore bool) Option {
	return func(o *Options) {
		o.IgnoreConflicting = ignore
	}
}

// WithIgnoreUnspendable creates an option to control whether transactions marked as unspendable are spendable
// Parameters:
//   - ignoreUnspendable: When true, transactions marked as unspendable are spendable
//
// Returns:
//   - Option: Function that sets the ignoreUnspendable option
func WithIgnoreUnspendable(ignoreUnspendable bool) Option {
	return func(o *Options) {
		o.IgnoreUnspendable = ignoreUnspendable
	}
}

// WithDisableConsensus creates an option to control consensus
// Parameters:
//   - disable: When true, consensus will be disabled
//
// Returns:
//   - Option: Function that sets the disableConsensus option
func WithDisableConsensus(disable bool) Option {
	return func(o *Options) {
		o.disableConsensus = disable
	}
}

// TxValidatorOptions defines configuration options specific to transaction validation
type TxValidatorOptions struct {
	skipPolicyChecks bool
}

// TxValidatorOption defines a function type for setting transaction validator options
// This follows the functional options pattern for flexible configuration
type TxValidatorOption func(*TxValidatorOptions)

func WithTxValidatorSkipPolicyChecks(skip bool) TxValidatorOption {
	return func(o *TxValidatorOptions) {
		o.skipPolicyChecks = skip
	}
}
