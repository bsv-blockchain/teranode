package settings

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockchainStoreTimeoutDocumentationDefault(t *testing.T) {
	field, ok := reflect.TypeOf(BlockChainSettings{}).FieldByName("StoreDBTimeoutMillis")
	require.True(t, ok)
	documented, err := strconv.Atoi(field.Tag.Get("default"))
	require.NoError(t, err)
	require.Equal(t, DefaultBlockchainStoreDBTimeoutMillis, documented)
}
