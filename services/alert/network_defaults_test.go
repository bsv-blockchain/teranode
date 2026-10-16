package alert

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
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
		name         string
		network      string
		configured   alertNetworkConfig
		want         alertNetworkConfig
		wantFallback []string
	}{
		{
			name:    "mainnet defaults",
			network: "mainnet",
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys,
				ProtocolID:  "/bitcoin/alert-system/1.0.0",
				TopicName:   "bitcoin_alert_system",
			},
		},
		{
			name:    "testnet defaults",
			network: "testnet",
			want: alertNetworkConfig{
				GenesisKeys: testnet.GenesisKeys,
				ProtocolID:  "/bitcoin-testnet/alert-system/0.0.1",
				TopicName:   "bitcoin_alert_system_testnet",
			},
		},
		{
			name:    "stn defaults",
			network: "stn",
			want: alertNetworkConfig{
				GenesisKeys: stn.GenesisKeys,
				ProtocolID:  "/bitcoin-stn/alert-system/0.0.1",
				TopicName:   "bitcoin_alert_system_stn",
			},
		},
		{
			name:    "unknown network falls back to mainnet keys and protocol with suffixed topic",
			network: "regtest",
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys,
				ProtocolID:  mainnet.ProtocolID,
				TopicName:   "bitcoin_alert_system_regtest",
			},
			wantFallback: []string{"alert_genesis_keys", "alert_protocol_id"},
		},
		{
			name:    "unknown network with only the topic set still falls back for keys and protocol",
			network: "regtest",
			configured: alertNetworkConfig{
				TopicName: "private_topic",
			},
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys,
				ProtocolID:  mainnet.ProtocolID,
				TopicName:   "private_topic",
			},
			wantFallback: []string{"alert_genesis_keys", "alert_protocol_id"},
		},
		{
			name:    "private network with every field set uses no mainnet fallback",
			network: "regtest",
			configured: alertNetworkConfig{
				GenesisKeys: testnet.GenesisKeys,
				ProtocolID:  "/private/alert-system/0.0.1",
				TopicName:   "private_topic",
			},
			want: alertNetworkConfig{
				GenesisKeys: testnet.GenesisKeys,
				ProtocolID:  "/private/alert-system/0.0.1",
				TopicName:   "private_topic",
			},
		},
		{
			name:    "explicit settings override network defaults",
			network: "testnet",
			configured: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys[:3],
				ProtocolID:  "/custom/alert-system/1.0.0",
				TopicName:   "custom_topic",
			},
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys[:3],
				ProtocolID:  "/custom/alert-system/1.0.0",
				TopicName:   "custom_topic",
			},
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
		},
		{
			name:    "blank genesis keys are treated as unset",
			network: "testnet",
			configured: alertNetworkConfig{
				GenesisKeys: []string{"", "  "},
			},
			want: testnet,
		},
		{
			name:    "genesis keys are trimmed and stripped of blank and repeated entries",
			network: "testnet",
			configured: alertNetworkConfig{
				// a trailing separator in the setting yields a blank entry
				GenesisKeys: []string{" " + mainnet.GenesisKeys[0] + " ", strings.ToUpper(mainnet.GenesisKeys[0]), mainnet.GenesisKeys[1], "", mainnet.GenesisKeys[2]},
			},
			want: alertNetworkConfig{
				GenesisKeys: mainnet.GenesisKeys[:3],
				ProtocolID:  testnet.ProtocolID,
				TopicName:   testnet.TopicName,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fallback, err := resolveAlertNetworkConfig(tt.network, tt.configured)
			require.NoError(t, err)
			require.Equal(t, tt.wantFallback, fallback)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveAlertNetworkConfig_invalidGenesisKeys(t *testing.T) {
	mainnetKeys := upstreamNetwork(t, "mainnet").GenesisKeys
	valid, others := mainnetKeys[0], mainnetKeys[1:]

	tooMany := make([]string, 0, maxGenesisKeys+1)
	for i := 0; i <= maxGenesisKeys; i++ {
		priv, err := bec.NewPrivateKey()
		require.NoError(t, err)

		tooMany = append(tooMany, hex.EncodeToString(priv.PubKey().Compressed()))
	}

	tests := []struct {
		name    string
		keys    []string
		wantErr string
	}{
		{"placeholder", []string{valid, others[0], "custom_key"}, "alert_genesis_keys[2]"},
		{"too short", []string{valid[:64], others[0], others[1]}, "expected 66 hex characters"},
		{"uncompressed length", []string{valid + valid[2:], others[0], others[1]}, "expected 66 hex characters"},
		{"not hex", []string{"zz" + valid[2:], others[0], others[1]}, "is not a valid public key"},
		{"not on the curve", []string{"02" + strings.Repeat("00", 32), others[0], others[1]}, "is not a valid public key"},
		{"fewer keys than required signers", []string{valid, others[0]}, "alerts need 3 distinct signers"},
		{"repeated keys leave fewer than required signers", []string{valid, strings.ToUpper(valid), others[0]}, "alerts need 3 distinct signers"},
		{"more keys than are verified", tooMany, "at most 10 are supported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := resolveAlertNetworkConfig("testnet", alertNetworkConfig{GenesisKeys: tt.keys})
			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("max keys are accepted", func(t *testing.T) {
		got, _, err := resolveAlertNetworkConfig("testnet", alertNetworkConfig{GenesisKeys: tooMany[:maxGenesisKeys]})
		require.NoError(t, err)
		require.Equal(t, tooMany[:maxGenesisKeys], got.GenesisKeys)
	})
}
