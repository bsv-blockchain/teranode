package options

import (
	"time"
)

type Options func(s *SetOptions)

type SetOptions struct {
	TTL          time.Duration
	Extension    string
	SubDirectory string
}

func NewSetOptions(opts ...Options) *SetOptions {
	options := &SetOptions{}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithTTL(ttl time.Duration) Options {
	return func(s *SetOptions) {
		s.TTL = ttl
	}
}

func WithFileExtension(extension string) Options {
	return func(s *SetOptions) {
		s.Extension = extension
	}
}

func WithSubDirectory(subDirectory string) Options {
	return func(s *SetOptions) {
		s.SubDirectory = subDirectory
	}
}
