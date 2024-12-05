//go:build test_all || test_stores || test_stores_sql

package sql

import (
	"context"
	"net/url"
	"testing"

	storesql "github.com/bitcoin-sv/ubsv/stores/blockchain/sql"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_stores_sql ./test/...

func Test_SetGetFSMState(t *testing.T) {
	connStr, teardown, err := helper.SetupTestPostgresContainer()

	if err != nil {
		t.Logf("Error setting up postgres container: %v", err)
	}

	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := storesql.New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	err = s.SetFSMState(context.Background(), "CATCHING_BLOCKS")
	require.NoError(t, err)

	state, err := s.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "CATCHING_BLOCKS", state)

	err = s.SetFSMState(context.Background(), "RUNNING")
	require.NoError(t, err)

	state, err = s.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "RUNNING", state)
}
