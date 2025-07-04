package options

import (
	"net/url"
	"os"
	"testing"

	"github.com/bitcoin-sv/teranode/pkg/fileformat"
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

	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	tests := []struct {
		name         string
		key          []byte
		fileType     fileformat.FileType
		storeOptions []StoreOption
		fileOptions  []FileOption
		expected     string
	}{
		{
			name:         "Default",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: nil,
			fileOptions:  nil,
			expected:     tempDir + "/79656b.testing",
		},
		{
			name:         "With_FileName",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: nil,
			fileOptions:  []FileOption{WithFilename("filename-1234")},
			expected:     tempDir + "/filename-1234.testing",
		},
		{
			name:         "With_SubDirectory",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data/")},
			fileOptions:  nil,
			expected:     tempDir + "/data/79656b.testing",
		},
		{
			name:         "With_FileName_and_SubDirectory",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data2/")},
			fileOptions:  []FileOption{WithFilename("filename-5678")},
			expected:     tempDir + "/data2/filename-5678.testing",
		},
		{
			name:         "With_Absolute_SubDirectory",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithDefaultSubDirectory("/data3/")},
			fileOptions:  nil,
			expected:     tempDir + "/data3/79656b.testing",
		},
		{
			name:        "With_FileExtension",
			key:         []byte("key"),
			fileType:    fileformat.FileTypeTesting,
			fileOptions: nil,
			expected:    tempDir + "/79656b.testing",
		},
		{
			name:         "With_FileName_and_FileExtension",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data4/")},
			fileOptions:  []FileOption{WithFilename("filename-1234")},
			expected:     tempDir + "/data4/filename-1234.testing",
		},
		// WithHashPrefix
		{
			name:         "WithHashPrefix",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithHashPrefix(1)},
			fileOptions:  nil,
			expected:     tempDir + "/7/79656b.testing",
		},
		{
			name:         "WithHashPrefix and SubDirectory",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithDefaultSubDirectory("./data5/"), WithHashPrefix(2)},
			fileOptions:  nil,
			expected:     tempDir + "/data5/79/79656b.testing",
		},
		{
			name:         "WithHashPrefix and FileName",
			key:          []byte("key"),
			fileType:     fileformat.FileTypeTesting,
			storeOptions: []StoreOption{WithHashPrefix(-1)},
			fileOptions:  []FileOption{WithFilename("filename-1234a")},
			expected:     tempDir + "/a/filename-1234a.testing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeOptions := NewStoreOptions(tt.storeOptions...)

			o := MergeOptions(storeOptions, tt.fileOptions)

			result, err := o.ConstructFilename(tempDir, tt.key, tt.fileType)
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
		assert.Empty(t, opts.SubDirectory)
		assert.False(t, opts.AllowOverwrite)
	})

	t.Run("With multiple options", func(t *testing.T) {
		opts := NewFileOptions(
			WithFilename("test.txt"),
			WithSubDirectory("subdir"),
		)
		assert.Equal(t, "test.txt", opts.Filename)
		assert.Equal(t, "subdir", opts.SubDirectory)
	})
}

func TestTTLOptions(t *testing.T) {
	bhr := uint32(5)

	t.Run("WithDefaultTTL", func(t *testing.T) {
		opts := NewStoreOptions(WithDefaultBlockHeightRetention(bhr))
		assert.Equal(t, bhr, opts.BlockHeightRetention)
	})

	t.Run("WithDAH", func(t *testing.T) {
		opts := NewFileOptions(WithDeleteAt(bhr))
		assert.Equal(t, bhr, opts.DAH)
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

func TestFileOptionsToQuery(t *testing.T) {
	t.Run("Empty options", func(t *testing.T) {
		query := FileOptionsToQuery(fileformat.FileTypeTesting)
		assert.Len(t, query, 1)
		assert.Equal(t, fileformat.FileTypeTesting.String(), query.Get("fileType"))
	})

	t.Run("All options", func(t *testing.T) {
		bhr := uint32(5)

		opts := []FileOption{
			WithDeleteAt(bhr),
			WithFilename("test.txt"),
			WithAllowOverwrite(true),
		}

		query := FileOptionsToQuery(fileformat.FileTypeSubtreeMeta, opts...)

		assert.Equal(t, "5", query.Get("dah"))
		assert.Equal(t, "test.txt", query.Get("filename"))
		assert.Equal(t, fileformat.FileTypeSubtreeMeta.String(), query.Get("fileType"))
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
			"allowOverwrite":       []string{"true"},
		}

		opts := QueryToFileOptions(query)
		options := NewFileOptions(opts...)

		assert.Equal(t, uint32(5), options.DAH)
		assert.Equal(t, "test.txt", options.Filename)
		assert.True(t, options.AllowOverwrite)
	})

	t.Run("Invalid DAH", func(t *testing.T) {
		query := url.Values{
			"dah": []string{"invalid"},
		}

		opts := QueryToFileOptions(query)
		options := NewFileOptions(opts...)
		assert.Equal(t, uint32(0), options.DAH)
	})
}
