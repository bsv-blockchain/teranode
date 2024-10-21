package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func Test_SetGetFSMState(t *testing.T) {
	connStr, teardown, err := setupPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	err = s.SetFSMState(context.Background(), "CATCHING_BLOCKS")
	require.NoError(t, err)

	state, err := s.GetFSMState(context.Background())
	fmt.Println("State: ", state)
	require.NoError(t, err)
	require.Equal(t, "CATCHING_BLOCKS", state)
}
