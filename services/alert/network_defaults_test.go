package alert

import (
	"testing"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/stretchr/testify/require"
)

// upstreamNetwork returns go-alert-system's embedded defaults for environment.
func upstreamNetwork(t *testing.T, environment string) alertNetworkConfig {
	t.Helper()

	n, err := config.NetworkDefaults(environment)
	require.NoError(t, err)

	return alertNetworkConfig{GenesisKeys: n.GenesisKeys, ProtocolID: n.ProtocolID, TopicName: n.TopicName}
}

func TestResolveAlertNetworkConfig(t *testing.T) {
	mainnet := upstreamNetwork(t, "mainnet")
	testnet := upstreamNetwork(t, "testnet")
	stn := upstreamNetwork(t, "stn")

	// guard against the networks resolving to the same key set
	require.NotEqual(t, mainnet.GenesisKeys, testnet.GenesisKeys)

	tests := []struct {
		name       string
		network    string
		configured alertNetworkConfig
		want       alertNetworkConfig
		wantKnown  bool
	}{
		{
			name:    "mainnet defaults",
			network: "mainnet",
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys,
				ProtocolID:  "/bitcoin/alert-system/1.0.0",
				TopicName:   "bitcoin_alert_system",
			},
			wantKnown: true,
		},
		{
			name:    "testnet defaults",
			network: "testnet",
			want: alertNetworkConfig{
				GenesisKeys: testnet.GenesisKeys,
				ProtocolID:  "/bitcoin-testnet/alert-system/0.0.1",
				TopicName:   "bitcoin_alert_system_testnet",
			},
			wantKnown: true,
		},
		{
			name:    "stn defaults",
			network: "stn",
			want: alertNetworkConfig{
				GenesisKeys: stn.GenesisKeys,
				ProtocolID:  "/bitcoin-stn/alert-system/0.0.1",
				TopicName:   "bitcoin_alert_system_stn",
			},
			wantKnown: true,
		},
		{
			name:    "unknown network falls back to mainnet keys and protocol with suffixed topic",
			network: "regtest",
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys,
				ProtocolID:  mainnet.ProtocolID,
				TopicName:   "bitcoin_alert_system_regtest",
			},
			wantKnown: false,
		},
		{
			name:    "explicit settings override network defaults",
			network: "testnet",
			configured: alertNetworkConfig{
				GenesisKeys: []string{"custom_key"},
				ProtocolID:  "/custom/alert-system/1.0.0",
				TopicName:   "custom_topic",
			},
			want: alertNetworkConfig{
				GenesisKeys: []string{"custom_key"},
				ProtocolID:  "/custom/alert-system/1.0.0",
				TopicName:   "custom_topic",
			},
			wantKnown: true,
		},
		{
			name:    "explicit override of a single field keeps the other defaults",
			network: "testnet",
			configured: alertNetworkConfig{
				ProtocolID: "/custom/alert-system/1.0.0",
			},
			want: alertNetworkConfig{
				GenesisKeys: testnet.GenesisKeys,
				ProtocolID:  "/custom/alert-system/1.0.0",
				TopicName:   testnet.TopicName,
			},
			wantKnown: true,
		},
		{
			name:    "blank genesis keys are treated as unset",
			network: "testnet",
			configured: alertNetworkConfig{
				GenesisKeys: []string{"", "  "},
			},
			want:      testnet,
			wantKnown: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, known, err := resolveAlertNetworkConfig(tt.network, tt.configured)
			require.NoError(t, err)
			require.Equal(t, tt.wantKnown, known)
			require.Equal(t, tt.want, got)
		})
	}
}
