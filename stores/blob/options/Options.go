package options

import (
	"os"
	"path/filepath"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/ordishs/go-utils"
)

type Options struct {
	TTL          *time.Duration
	Filename     string
	Extension    string
	SubDirectory string
	HashPrefix   int
}

type StoreOption func(*Options)
type FileOption func(*Options)

func NewStoreOptions(opts ...StoreOption) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func NewFileOptions(opts ...FileOption) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// Store creation options
func WithDefaultTTL(ttl time.Duration) StoreOption {
	return func(s *Options) {
		s.TTL = &ttl
	}
}

func WithSubDirectory(subDirectory string) StoreOption {
	return func(s *Options) {
		s.SubDirectory = subDirectory
	}
}

// WithHashPrefix configures the use of hash prefixes in file naming.
// Parameters:
//   - length: The number of characters from the hash to use.
//     Positive: Use prefix from start of hash.
//     Negative: Use suffix from end of hash.
//     Zero: Don't use a hash in the filename.
//
// This option helps in organizing files and avoiding name collisions.
func WithHashPrefix(length int) StoreOption {
	return func(s *Options) {
		s.HashPrefix = length
	}
}

// Per-call options
func WithTTL(ttl time.Duration) FileOption {
	return func(s *Options) {
		s.TTL = &ttl
	}
}

func WithFileName(name string) FileOption {
	return func(s *Options) {
		s.Filename = name
	}
}

func WithFileExtension(extension string) FileOption {
	return func(s *Options) {
		s.Extension = extension
	}
}

// MergeOptions combines StoreOptions and FileOptions into a single MergedOptions struct
func MergeOptions(storeOpts *Options, fileOpts []FileOption) *Options {
	options := &Options{}

	if storeOpts != nil {
		if storeOpts.TTL != nil {
			options.TTL = storeOpts.TTL
		}

		options.SubDirectory = storeOpts.SubDirectory
		options.HashPrefix = storeOpts.HashPrefix
	}

	for _, opt := range fileOpts {
		opt(options)
	}

	return options
}

func (o *Options) ConstructFilename(basePath string, hash []byte) (string, error) {
	var (
		filename string
		prefix   string
	)

	if len(o.Filename) > 0 {
		filename = o.Filename
	} else {
		filename = utils.ReverseAndHexEncodeSlice(hash)
	}

	// For negative HashPrefix, take characters from the end
	// For positive HashPrefix, take characters from the beginning
	prefix = o.CalculatePrefix(filename)

	// Build the folder to use based on the StoreOption SubDirectory and the calculated prefix
	folder := filepath.Join(basePath, o.SubDirectory, prefix)

	// Create the folder if it doesn't exist but only if we have a prefix as the subdirectory
	// would already have been created by StoreOptions
	if prefix != "" {
		if err := os.MkdirAll(folder, 0755); err != nil {
			return "", errors.NewProcessingError("failed to create directory %s", folder, err)
		}
	}

	filename = filepath.Join(folder, filename)

	// Create full file name
	if o.Extension != "" {
		filename += "." + o.Extension
	}

	return filename, nil
}

func (o *Options) CalculatePrefix(filename string) string {
	var prefix string

	if o.HashPrefix != 0 {
		if o.HashPrefix < 0 {
			start := len(filename) + o.HashPrefix
			if start < 0 {
				start = 0
			}

			prefix = filename[start:]
		} else {
			end := o.HashPrefix
			if end > len(filename) {
				end = len(filename)
			}

			prefix = filename[:end]
		}
	}

	return prefix
}
