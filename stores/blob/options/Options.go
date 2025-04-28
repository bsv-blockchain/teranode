package options

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/ordishs/go-utils"
)

type Options struct {
	BlockHeightRetention uint32 // This is the block height retention for the store (StoreOption)
	DAH                  uint32 // This is the delete at height for a file (FileOption)
	Filename             string
	Extension            string
	SubDirectory         string
	HashPrefix           int
	AllowOverwrite       bool
	Header               []byte
	Footer               *Footer
	GenerateSHA256       bool
	PersistSubDir        string
	LongtermStoreURL     *url.URL
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

// WithDefaultBlockHeightRetention configures the default block height retention for the store.
func WithDefaultBlockHeightRetention(blockHeightRetention uint32) StoreOption {
	return func(s *Options) {
		s.BlockHeightRetention = blockHeightRetention
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

// WithDeleteAt configures the DAH for the file.
func WithDeleteAt(dah uint32) FileOption {
	return func(s *Options) {
		s.DAH = dah
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

// WithLongtermStorage configures longterm storage options for the store.
// This enables the three-layer storage functionality (primary local, persistent local, and longterm store, like S3).
// Parameters:
//   - persistSubDir: The subdirectory for persistent storage
//   - longtermStoreURL: The URL for longterm storage (can be nil to disable longterm storage)
//
// The idea here is that an external process will handle persisting all items in the persistSubDir
// to whatever longterm storage is required.  The longtermStoreURL is used to initialize the
// longterm store client that can access that longterm storage for retrieval operations.
func WithLongtermStorage(persistSubDir string, longtermStoreURL *url.URL) StoreOption {
	return func(s *Options) {
		s.PersistSubDir = persistSubDir

		s.LongtermStoreURL = longtermStoreURL
	}
}

// ReplaceExtention replaces the file extension in the given FileOptions.
// Parameters:
//   - fileOpts: The FileOptions to replace the extension in.
//   - oldExt: The old file extension to replace.
//   - newExt: The new file extension to replace with.
//
// Returns:
//   - A new slice of FileOptions with the old extension replaced with the new extension.
func ReplaceExtention(fileOpts []FileOption, oldExt string, newExt string) []FileOption {
	newOpts := make([]FileOption, 0, len(fileOpts))

	for _, opt := range fileOpts {
		tempOpt := &Options{}
		opt(tempOpt)

		if tempOpt.Extension == oldExt {
			newOpts = append(newOpts, WithFileExtension(newExt))
		} else {
			newOpts = append(newOpts, opt)
		}
	}

	return newOpts
}

// MergeOptions combines StoreOptions and FileOptions into a single MergedOptions struct
func MergeOptions(storeOpts *Options, fileOpts []FileOption) *Options {
	options := &Options{}

	if storeOpts != nil {
		options.BlockHeightRetention = storeOpts.BlockHeightRetention
		options.SubDirectory = storeOpts.SubDirectory
		options.HashPrefix = storeOpts.HashPrefix
		options.Header = storeOpts.Header
		options.Footer = storeOpts.Footer
		options.GenerateSHA256 = storeOpts.GenerateSHA256
		options.PersistSubDir = storeOpts.PersistSubDir
		options.LongtermStoreURL = storeOpts.LongtermStoreURL
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

	if options.DAH > 0 {
		query.Set("dah", strconv.FormatUint(uint64(options.DAH), 10))
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

	if blockHeightRetentionStr := query.Get("blockHeightRetention"); blockHeightRetentionStr != "" {
		if blockHeightRetention, err := strconv.ParseUint(blockHeightRetentionStr, 10, 32); err == nil {
			opts = append(opts, WithDeleteAt(uint32(blockHeightRetention)))
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
