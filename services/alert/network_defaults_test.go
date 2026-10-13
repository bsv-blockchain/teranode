package alert

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveAlertNetworkConfig(t *testing.T) {
	mainnet := alertNetworkDefaults["mainnet"]
	testnet := alertNetworkDefaults["testnet"]
	stn := alertNetworkDefaults["stn"]

	tests := []struct {
		name       string
		network    string
		configured alertNetworkConfig
		want       alertNetworkConfig
		wantKnown  bool
	}{
		{
			name:      "mainnet defaults",
			network:   "mainnet",
			want:      mainnet,
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
			got, known := resolveAlertNetworkConfig(tt.network, tt.configured)
			require.Equal(t, tt.wantKnown, known)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveAlertNetworkConfig_doesNotAliasDefaults(t *testing.T) {
	got, _ := resolveAlertNetworkConfig("testnet", alertNetworkConfig{})
	got.GenesisKeys[0] = "mutated"

	require.NotEqual(t, "mutated", alertNetworkDefaults["testnet"].GenesisKeys[0])
}

func TestAlertNetworkDefaults_keysAreCompressedPubKeys(t *testing.T) {
	for network, defaults := range alertNetworkDefaults {
		require.Len(t, defaults.GenesisKeys, 5, network)

		for _, key := range defaults.GenesisKeys {
			require.Len(t, key, 66, network)
		}
	}
}
