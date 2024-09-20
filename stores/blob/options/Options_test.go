package options

import (
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
			fileOptions:  []FileOption{WithFileName("filename-1234")},
			expected:     tempDir + "/filename-1234",
		},
		{
			name:         "With SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithSubDirectory("./data/")},
			fileOptions:  nil,
			expected:     tempDir + "/data/79656b",
		},
		{
			name:         "With FileName and SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithSubDirectory("./data2/")},
			fileOptions:  []FileOption{WithFileName("filename-5678")},
			expected:     tempDir + "/data2/filename-5678",
		},
		{
			name:         "With Absolute SubDirectory",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithSubDirectory("/data3/")},
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
			storeOptions: []StoreOption{WithSubDirectory("./data4/")},
			fileOptions:  []FileOption{WithFileName("filename-1234"), WithFileExtension("meta")},
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
			storeOptions: []StoreOption{WithSubDirectory("./data5/"), WithHashPrefix(2)},
			fileOptions:  nil,
			expected:     tempDir + "/data5/79/79656b",
		},
		{
			name:         "WithHashPrefix and FileName",
			key:          []byte("key"),
			storeOptions: []StoreOption{WithHashPrefix(-1)},
			fileOptions:  []FileOption{WithFileName("filename-1234a")},
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
