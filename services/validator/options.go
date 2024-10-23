package validator

type Options struct {
	skipUtxoCreation     bool
	addTXToBlockAssembly bool
}

// Option is a function that sets some option on the Options struct
type Option func(*Options)

func NewDefaultOptions() *Options {
	return &Options{
		skipUtxoCreation:     false,
		addTXToBlockAssembly: true,
	}
}

func ProcessOptions(opts ...Option) *Options {
	options := NewDefaultOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// WithSkipUtxoCreation is an option that skips the creation of UTXOs
func WithSkipUtxoCreation(skip bool) Option {
	return func(o *Options) {
		o.skipUtxoCreation = skip
	}
}

// WithAddTXToBlockAssembly is an option that allows the transaction to be added to the block assembly or not
func WithAddTXToBlockAssembly(add bool) Option {
	return func(o *Options) {
		o.addTXToBlockAssembly = add
	}
}
