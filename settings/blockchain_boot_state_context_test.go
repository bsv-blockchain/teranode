package settings

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireNoBootStateEnv(t *testing.T) {
	t.Helper()

	const key = "blockchain_initializeNodeInState"
	original, exists := os.LookupEnv(key)
	if !exists {
		return
	}

	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		require.NoError(t, os.Setenv(key, original))
	})
}

func TestInitializeNodeInState_ContextDefaults(t *testing.T) {
	requireNoBootStateEnv(t)

	tests := []struct {
		context string
		want    string
	}{
		{context: "operator", want: "IDLE"},
		{context: "operator.mainnet", want: "IDLE"},
		{context: "operator.testnet", want: "IDLE"},
		{context: "operator.teratestnet", want: "IDLE"},
		{context: "operator.teratestnet.prod", want: "IDLE"},
		{context: "docker.m", want: "IDLE"},
		{context: "dev", want: ""},
		{context: "test", want: ""},
		{context: "docker", want: ""},
		{context: "docker.ci", want: ""},
		{context: "docker.teranode1.test", want: ""},
		{context: "docker.host.teranode1", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.context, func(t *testing.T) {
			require.Equal(t, tt.want, getString("blockchain_initializeNodeInState", "", tt.context))
			require.Equal(t, tt.want, NewSettings(tt.context).BlockChain.InitializeNodeInState)
		})
	}
}

func TestInitializeNodeInState_TrimsEnvironmentWhitespace(t *testing.T) {
	for _, raw := range []string{" IDLE", "IDLE ", "  IDLE  ", "\tIDLE\n"} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			t.Setenv("blockchain_initializeNodeInState", raw)
			require.Equal(t, "IDLE", NewSettings("operator").BlockChain.InitializeNodeInState)
		})
	}
}
