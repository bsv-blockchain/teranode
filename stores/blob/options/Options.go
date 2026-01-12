// Package options provides configuration structures and functions for blob store operations.
// It defines a flexible set of options that can be applied at both the store level (global
// configuration) and at the individual file operation level (per-operation configuration).
// The package supports various blob storage features including custom filenames, extensions,
// Delete-At-Height (DAH) values, directory organization, and metadata handling.
package options

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/ordishs/go-utils"
)

// Options represents the complete set of configuration options for blob operations.
// It combines both store-level options (which apply globally) and file-level options
// (which apply to individual operations). The structure is designed to be flexible
// and extensible to accommodate various blob storage configuration needs.
type Options struct {
	// BlockHeightRetention is the default number of blocks to retain blobs (StoreOption)
	// When a blob is stored without an explicit DAH, it will be set to expire after
	// the current block height plus this retention period
	BlockHeightRetention uint32
	// DAH (Delete-At-Height) is the blockchain height at which a blob should be deleted (FileOption)
	// When this height is reached or exceeded, the blob becomes eligible for deletion
	DAH uint32
	// Filename is an optional custom name for storing a blob instead of using its hash
	Filename string
	// SubDirectory is an optional subdirectory for organizing blobs within the store
	SubDirectory string
	// HashPrefix controls how hash-based directory structures are created
	// Positive: Use first N characters of hash as directory
	// Negative: Use last N characters of hash as directory
	// Zero: Don't use hash-based directories
	HashPrefix int
	// AllowOverwrite determines if existing blobs can be overwritten
	AllowOverwrite bool
	// SkipHeader determines if the file header should be skipped for easier CLI readability
	SkipHeader bool
	// PersistSubDir is a subdirectory for persistent storage in tiered storage models
	PersistSubDir string
	// LongtermStoreURL is the URL for a longterm storage backend in tiered storage models
	LongtermStoreURL *url.URL
	// BlockHeightCh is a channel for tracking block heights
	BlockHeightCh chan uint32
	// DisableDAH disables all Delete-At-Height functionality for this store (StoreOption)
	// When true, the store will never create .dah files or participate in DAH-based cleanup
	// This is useful for external stores where lifecycle management is handled by other systems
	DisableDAH bool
}

// StoreOption is a function type for configuring store-level options.
// These options apply globally to all operations performed by a store instance.
type StoreOption func(*Options)

// FileOption is a function type for configuring file-level options.
// These options apply only to the specific blob operation being performed.
type FileOption func(*Options)

// NewStoreOptions creates a new Options instance configured with store-level options.
// This function is typically used when creating a new blob store to establish its
// default behavior and configuration.
//
// Parameters:
//   - opts: Variable number of StoreOption functions to apply
//
// Returns:
//   - *Options: A configured Options instance with store-level defaults
func NewStoreOptions(opts ...StoreOption) *Options {
	options := &Options{}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

// NewFileOptions creates a new Options instance configured with file-level options.
// This function is typically used when performing individual blob operations to
// specify operation-specific behavior.
//
// Parameters:
//   - opts: Variable number of FileOption functions to apply
//
// Returns:
//   - *Options: A configured Options instance with file-level settings
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

// WithDisableDAH disables all Delete-At-Height functionality for the store.
// When enabled, the store will never create .dah files or participate in DAH-based cleanup.
// This is useful for external stores where lifecycle management is handled by other systems
// (e.g., Aerospike pruner service managing external file cleanup).
func WithDisableDAH(disable bool) StoreOption {
	return func(s *Options) {
		s.DisableDAH = disable
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

// WithSkipHeader configures whether to skip writing the file header for easier CLI readability.
func WithSkipHeader(skipHeader bool) FileOption {
	return func(s *Options) {
		s.SkipHeader = skipHeader
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

// WithBlockHeightCh configures the block height channel for the store.
func WithBlockHeightCh(blockHeightCh chan uint32) StoreOption {
	return func(s *Options) {
		s.BlockHeightCh = blockHeightCh
	}
}

// MergeOptions combines StoreOptions and FileOptions into a single MergedOptions struct
// MergeOptions combines store-level options with file-level options.
// This function is used to create a final configuration that incorporates both
// the store's default settings and any operation-specific overrides. Store-level
// options provide defaults, while file-level options take precedence when specified.
//
// Parameters:
//   - storeOpts: The store-level options to use as defaults
//   - fileOpts: The file-level options to override defaults with
//
// Returns:
//   - *Options: A new Options instance with the combined configuration
func MergeOptions(storeOpts *Options, fileOpts []FileOption) *Options {
	options := &Options{}

	if storeOpts != nil {
		options.BlockHeightRetention = storeOpts.BlockHeightRetention
		options.SubDirectory = storeOpts.SubDirectory
		options.HashPrefix = storeOpts.HashPrefix
		options.SkipHeader = storeOpts.SkipHeader
		options.PersistSubDir = storeOpts.PersistSubDir
		options.LongtermStoreURL = storeOpts.LongtermStoreURL
		options.DisableDAH = storeOpts.DisableDAH
	}

	for _, opt := range fileOpts {
		opt(options)
	}

	return options
}

// FileOptionsToQuery converts FileOptions to URL query parameters
// FileOptionsToQuery converts FileOptions to URL query parameters.
// This is useful for transmitting blob options over HTTP or other URL-based protocols.
// The function encodes options into standard URL query parameters that can be decoded
// on the receiving end.
//
// Parameters:
//   - fileType: The file type to use
//   - opts: The file options to convert
//
// Returns:
//   - url.Values: URL query parameters representing the options
func FileOptionsToQuery(fileType fileformat.FileType, opts ...FileOption) url.Values {
	options := NewFileOptions(opts...)
	query := url.Values{}

	query.Set("fileType", fileType.String())

	if options.DAH > 0 {
		query.Set("dah", strconv.FormatUint(uint64(options.DAH), 10))
	}

	if options.Filename != "" {
		query.Set("filename", options.Filename)
	}

	if options.AllowOverwrite {
		query.Set("allowOverwrite", "true")
	}

	return query
}

// QueryToFileOptions converts URL query parameters to FileOptions
// QueryToFileOptions converts URL query parameters to FileOptions.
// This is the inverse of FileOptionsToQuery and is typically used on the receiving
// end of an HTTP request to reconstruct the original file options from query parameters.
//
// Parameters:
//   - query: URL query parameters to convert
//
// Returns:
//   - []FileOption: File options reconstructed from the query parameters
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

	if allowOverwrite := query.Get("allowOverwrite"); allowOverwrite == "true" {
		opts = append(opts, WithAllowOverwrite(true))
	}

	return opts
}

// validatePathWithinBase ensures the resolved path stays within basePath to prevent
// path traversal attacks. It resolves both paths to absolute form and checks that
// the target path is a subdirectory of the base path.
func validatePathWithinBase(basePath, targetPath string) error {
	absBase, err := filepath.Abs(basePath)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return err
	}

	// Clean paths to remove any . or .. components
	absBase = filepath.Clean(absBase)
	absTarget = filepath.Clean(absTarget)

	// Ensure target is within base (with proper separator handling)
	// The target must either equal the base or start with base + separator
	if absTarget != absBase && !strings.HasPrefix(absTarget, absBase+string(os.PathSeparator)) {
		return errors.NewInvalidArgumentError("path escapes base directory")
	}

	return nil
}

func (o *Options) ConstructFilename(basePath string, key []byte, fileType fileformat.FileType) (string, error) {
	var (
		filename string
		prefix   string
	)

	// Validate SubDirectory doesn't contain path traversal sequences
	if strings.Contains(o.SubDirectory, "..") {
		return "", errors.NewInvalidArgumentError("subdirectory contains path traversal sequence")
	}

	// Validate Filename doesn't contain path traversal or separator characters
	if strings.Contains(o.Filename, "..") || strings.ContainsAny(o.Filename, `/\`) {
		return "", errors.NewInvalidArgumentError("filename contains invalid path characters")
	}

	if len(o.Filename) > 0 {
		filename = o.Filename
	} else {
		filename = utils.ReverseAndHexEncodeSlice(key)
	}

	// For negative HashPrefix, take characters from the end
	// For positive HashPrefix, take characters from the beginning
	prefix = o.CalculatePrefix(filename)

	// Build the folder to use based on the StoreOption SubDirectory and the calculated prefix
	folder := filepath.Join(basePath, o.SubDirectory, prefix)

	// Validate the folder path stays within basePath
	if err := validatePathWithinBase(basePath, folder); err != nil {
		return "", err
	}

	// Create the folder if it doesn't exist but only if we have a prefix as the subdirectory
	// would already have been created by StoreOptions
	if prefix != "" {
		if err := os.MkdirAll(folder, 0755); err != nil {
			return "", errors.NewProcessingError("failed to create directory %s", folder, err)
		}
	}

	finalPath := filepath.Join(folder, filename) + "." + fileType.String()

	// Final validation that the complete path stays within basePath
	if err := validatePathWithinBase(basePath, finalPath); err != nil {
		return "", err
	}

	return finalPath, nil
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
