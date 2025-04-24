package options

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsCalculatePrefix(t *testing.T) {
	tests := []struct {
		filename   string
		hashPrefix int
		expected   string
	}{
		{"1234567890abcdef", 0, ""},
		{"1234567890abcdef", 1, "1"},
		{"1234567890abcdef", 2, "12"},
		{"1234567890abcdef", 3, "123"},
		{"1234567890abcdef", 4, "1234"},
		{"1234567890abcdef", 5, "12345"},
		{"1234567890abcdef", 6, "123456"},
		{"1234567890abcdef", -1, "f"},
		{"1234567890abcdef", -2, "ef"},
		{"1234567890abcdef", -3, "def"},
		{"1234567890abcdef", -4, "cdef"},
		{"1234567890abcdef", -5, "bcdef"},
		{"1234567890abcdef", -6, "abcdef"},
	}

	for _, test := range tests {
		o := &Options{
			HashPrefix: test.hashPrefix,
		}
		assert.Equal(t, test.expected, o.CalculatePrefix(test.filename))
	}
}

func TestOptionsConstructFilename(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name         string
		key          []byte
		storeOptions []StoreOption
		fileOptions  []FileOption
		expected     string
	}{
		{
			name:         "Default",
			key:          []byte("key"),
			storeOptions: nil,
			fileOptions:  nil,
			expected:     tempDir + "/79656b",
		},
		{
			name:         "With FileName",
			key:          []byte("key"),
			storeOptions: nil,
			fileOptions:  []FileOption{WithFilename("filename-1234")},
			expected:     tempDir + "/filename-1234",
		},
		{
			name:         "With SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data/")},
			fileOptions:  nil,
			expected:     tempDir + "/data/79656b",
		},
		{
			name:         "With FileName and SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data2/")},
			fileOptions:  []FileOption{WithFilename("filename-5678")},
			expected:     tempDir + "/data2/filename-5678",
		},
		{
			name:         "With Absolute SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithDefaultSubDirectory("/data3/")},
			fileOptions:  nil,
			expected:     tempDir + "/data3/79656b",
		},
		{
			name:        "With FileExtension",
			key:         []byte("key"),
			fileOptions: []FileOption{WithFileExtension("meta")},
			expected:    tempDir + "/79656b.meta",
		},
		{
			name:         "With FileName and FileExtension",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data4/")},
			fileOptions:  []FileOption{WithFilename("filename-1234"), WithFileExtension("meta")},
			expected:     tempDir + "/data4/filename-1234.meta",
		},
		// WithHashPrefix
		{
			name:         "WithHashPrefix",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithHashPrefix(1)},
			fileOptions:  nil,
			expected:     tempDir + "/7/79656b",
		},
		{
			name:         "WithHashPrefix and SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data5/"), WithHashPrefix(2)},
			fileOptions:  nil,
			expected:     tempDir + "/data5/79/79656b",
		},
		{
			name:         "WithHashPrefix and FileName",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithHashPrefix(-1)},
			fileOptions:  []FileOption{WithFilename("filename-1234a")},
			expected:     tempDir + "/a/filename-1234a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeOptions := NewStoreOptions(tt.storeOptions...)

			o := MergeOptions(storeOptions, tt.fileOptions)

			result, err := o.ConstructFilename(tempDir, tt.key)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewFileOptions(t *testing.T) {
	t.Run("Empty options", func(t *testing.T) {
		opts := NewFileOptions()
		assert.NotNil(t, opts)
		assert.Equal(t, uint32(0), opts.BlockHeightRetention)
		assert.Empty(t, opts.Filename)
		assert.Empty(t, opts.Extension)
		assert.Empty(t, opts.SubDirectory)
		assert.False(t, opts.AllowOverwrite)
	})

	t.Run("With multiple options", func(t *testing.T) {
		opts := NewFileOptions(
			WithFilename("test.txt"),
			WithFileExtension("meta"),
			WithSubDirectory("subdir"),
		)
		assert.Equal(t, "test.txt", opts.Filename)
		assert.Equal(t, "meta", opts.Extension)
		assert.Equal(t, "subdir", opts.SubDirectory)
	})
}

func TestTTLOptions(t *testing.T) {
	bhr := uint32(5)

	t.Run("WithDefaultTTL", func(t *testing.T) {
		opts := NewStoreOptions(WithDefaultBlockHeightRetention(bhr))
		assert.Equal(t, bhr, opts.BlockHeightRetention)
	})

	t.Run("WithTTL", func(t *testing.T) {
		opts := NewFileOptions(WithDeleteAt(bhr))
		assert.Equal(t, bhr, opts.BlockHeightRetention)
	})
}

func TestDirectoryOptions(t *testing.T) {
	t.Run("WithSubDirectory", func(t *testing.T) {
		opts := NewFileOptions(WithSubDirectory("custom/path"))
		assert.Equal(t, "custom/path", opts.SubDirectory)
	})
}

func TestAllowOverwriteOption(t *testing.T) {
	t.Run("WithAllowOverwrite", func(t *testing.T) {
		opts := NewFileOptions(WithAllowOverwrite(true))
		assert.True(t, opts.AllowOverwrite)
	})
}

func TestHeaderFooterOptions(t *testing.T) {
	t.Run("WithHeader", func(t *testing.T) {
		header := []byte("header-data")
		opts := NewStoreOptions(WithHeader(header))
		assert.Equal(t, header, opts.Header)
	})

	t.Run("WithFooter", func(t *testing.T) {
		footer := &Footer{} // Add appropriate footer data if needed
		opts := NewStoreOptions(WithFooter(footer))
		assert.Equal(t, footer, opts.Footer)
	})
}

func TestFileOptionsToQuery(t *testing.T) {
	t.Run("Empty options", func(t *testing.T) {
		query := FileOptionsToQuery()
		assert.Empty(t, query)
	})

	t.Run("All options", func(t *testing.T) {
		bhr := uint32(5)

		opts := []FileOption{
			WithDeleteAt(bhr),
			WithFilename("test.txt"),
			WithFileExtension("meta"),
			WithAllowOverwrite(true),
		}

		query := FileOptionsToQuery(opts...)

		assert.Equal(t, "5", query.Get("blockHeightRetention"))
		assert.Equal(t, "test.txt", query.Get("filename"))
		assert.Equal(t, "meta", query.Get("extension"))
		assert.Equal(t, "true", query.Get("allowOverwrite"))
	})
}

func TestQueryToFileOptions(t *testing.T) {
	t.Run("Empty query", func(t *testing.T) {
		query := url.Values{}
		opts := QueryToFileOptions(query)
		assert.Empty(t, opts)
	})

	t.Run("All query parameters", func(t *testing.T) {
		query := url.Values{
			"blockHeightRetention": []string{"5"},
			"filename":             []string{"test.txt"},
			"extension":            []string{"meta"},
			"allowOverwrite":       []string{"true"},
		}

		opts := QueryToFileOptions(query)
		options := NewFileOptions(opts...)

		assert.Equal(t, uint32(5), options.BlockHeightRetention)
		assert.Equal(t, "test.txt", options.Filename)
		assert.Equal(t, "meta", options.Extension)
		assert.True(t, options.AllowOverwrite)
	})

	t.Run("Invalid DAH", func(t *testing.T) {
		query := url.Values{
			"blockHeightRetention": []string{"invalid"},
		}

		opts := QueryToFileOptions(query)
		options := NewFileOptions(opts...)
		assert.Equal(t, uint32(0), options.BlockHeightRetention)
	})
}

func TestWithSHA256Checksum(t *testing.T) {
	t.Run("Default value", func(t *testing.T) {
		opts := NewStoreOptions()
		assert.False(t, opts.GenerateSHA256)
	})

	t.Run("With SHA256 enabled", func(t *testing.T) {
		opts := NewStoreOptions(WithSHA256Checksum())
		assert.True(t, opts.GenerateSHA256)
	})
}
