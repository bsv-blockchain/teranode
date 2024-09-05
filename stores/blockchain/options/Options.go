package options

type StoreBlockOptions struct {
	MinedSet    bool
	SubtreesSet bool
}

type StoreBlockOption func(*StoreBlockOptions)

func WithMinedSet(b bool) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.MinedSet = b
	}
}

func WithSubtreesSet(b bool) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.SubtreesSet = b
	}
}
