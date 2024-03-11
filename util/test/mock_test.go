package test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GenerateBlock(t *testing.T) {
	subtreeStore := NewLocalSubtreeStore()
	testConfig := &TestConfig{
		FileDir:                      "./test-generated_test_data/",
		FileNameTemplate:             "./test-generated_test_data/subtree-%d.bin",
		FileNameTemplateMerkleHashes: "./test-generated_test_data/subtree-merkle-hashes.bin",
		FileNameTemplateBlock:        "./test-generated_test_data/block.bin",
		TxMetafileNameTemplate:       "./test-generated_test_data/txMeta.bin",
		SubtreeSize:                  1024,
		TxCount:                      4 * 1024,
		GenerateNewTestData:          true,
	}
	block, err := GenerateTestBlock(subtreeStore, testConfig)
	require.NoError(t, err)
	require.NotEmpty(t, block)
}
