package options

import (
	"time"
)

type Options func(s *SetOptions)

type SetOptions struct {
	TTL             time.Duration
	Filename        string
	Extension       string
	SubDirectory    string
	PrefixDirectory int
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

func WithPrefixDirectory(numberOfCharsInPrefix int) Options {
	return func(s *SetOptions) {
		s.PrefixDirectory = numberOfCharsInPrefix
	}
}

func WithFileName(name string) Options {
	return func(s *SetOptions) {
		s.Filename = name
	}
}
