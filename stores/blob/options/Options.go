package options

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/ordishs/go-utils"
)

type Options struct {
	TTL            *time.Duration
	Filename       string
	Extension      string
	SubDirectory   string
	HashPrefix     int
	AllowOverwrite bool
	Header         []byte
	Footer         *Footer
	GenerateSHA256 bool
}

type StoreOption func(*Options)
type FileOption func(*Options)

func NewStoreOptions(opts ...StoreOption) *Options {
	options := &Options{
		GenerateSHA256: false,
	}
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

// WithDefaultTTL configures the default TTL for the store.
func WithDefaultTTL(ttl time.Duration) StoreOption {
	return func(s *Options) {
		s.TTL = &ttl
	}
}

// WithDefaultSubDirectory configures the default subdirectory for the store.
func WithDefaultSubDirectory(subDirectory string) StoreOption {
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

// WithTTL configures the TTL for the file.
func WithTTL(ttl time.Duration) FileOption {
	return func(s *Options) {
		s.TTL = &ttl
	}
}

// WithFilename configures the filename for the file.
func WithFilename(name string) FileOption {
	return func(s *Options) {
		s.Filename = name
	}
}

// WithFileExtension configures the file extension for the file.
func WithFileExtension(extension string) FileOption {
	return func(s *Options) {
		s.Extension = extension
	}
}

// WithSubDirectory configures the subdirectory for the file.
func WithSubDirectory(subDirectory string) FileOption {
	return func(s *Options) {
		s.SubDirectory = subDirectory
	}
}

// WithAllowOverwrite configures whether to allow overwriting of the file.
func WithAllowOverwrite(allowOverwrite bool) FileOption {
	return func(s *Options) {
		s.AllowOverwrite = allowOverwrite
	}
}

func WithHeader(header []byte) StoreOption {
	return func(s *Options) {
		s.Header = header
	}
}

func WithFooter(footer *Footer) StoreOption {
	return func(o *Options) {
		o.Footer = footer
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
		options.Header = storeOpts.Header
		options.Footer = storeOpts.Footer
		options.GenerateSHA256 = storeOpts.GenerateSHA256
	}

	for _, opt := range fileOpts {
		opt(options)
	}

	return options
}

// FileOptionsToQuery converts FileOptions to URL query parameters
func FileOptionsToQuery(opts ...FileOption) url.Values {
	options := NewFileOptions(opts...)
	query := url.Values{}

	if options.TTL != nil {
		query.Set("ttl", strconv.FormatInt(int64(*options.TTL), 10))
	}

	if options.Filename != "" {
		query.Set("filename", options.Filename)
	}

	if options.Extension != "" {
		query.Set("extension", options.Extension)
	}

	if options.AllowOverwrite {
		query.Set("allowOverwrite", "true")
	}

	return query
}

// QueryToFileOptions converts URL query parameters to FileOptions
func QueryToFileOptions(query url.Values) []FileOption {
	var opts []FileOption

	if ttlStr := query.Get("ttl"); ttlStr != "" {
		if ttl, err := strconv.ParseInt(ttlStr, 10, 64); err == nil {
			opts = append(opts, WithTTL(time.Duration(ttl)*time.Second))
		}
	}

	if filename := query.Get("filename"); filename != "" {
		opts = append(opts, WithFilename(filename))
	}

	if extension := query.Get("extension"); extension != "" {
		opts = append(opts, WithFileExtension(extension))
	}

	if allowOverwrite := query.Get("allowOverwrite"); allowOverwrite == "true" {
		opts = append(opts, WithAllowOverwrite(true))
	}

	return opts
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
			start := len(filename) + o.HashPrefix // in this case, the hash prefix is negative, so we start from the end of the filename
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

func WithSHA256Checksum() StoreOption {
	return func(o *Options) {
		o.GenerateSHA256 = true
	}
}
