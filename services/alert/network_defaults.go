package alert

import (
	"strings"
)

// alertNetworkConfig identifies an alert network: the genesis keys that may
// sign alerts, and the libp2p protocol and pubsub topic they travel on.
type alertNetworkConfig struct {
	GenesisKeys []string
	ProtocolID  string
	TopicName   string
}

// alertNetworkDefaults are the public alert networks, keyed by chaincfg network
// name. Source of truth: go-alert-system app/config/envs/{mainnet,testnet,stn}.json.
var alertNetworkDefaults = map[string]alertNetworkConfig{
	"mainnet": {
		GenesisKeys: []string{
			"02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768",
			"03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433",
			"03ddb2806f3cc48aa36bd4aea6b9f1c7ed3ffc8b9302b198ca963f15beff123678",
			"036846e3e8f4f944af644b6a6c6243889dd90d7b6c3593abb9ccf2acb8c9e606e2",
			"03e45c9dd2b34829c1d27c8b5d16917dd0dc2c88fa0d7bad7bffb9b542229a9304",
		},
		ProtocolID: "/bitcoin/alert-system/1.0.0",
		TopicName:  "bitcoin_alert_system",
	},
	"testnet": {
		GenesisKeys: []string{
			"0203aa8ca16b6b247b109b65e9d7a0f3a23ccae8f093f792e63bf5ce2567f572bc",
			"0289d3a1c6dacd0ee3de7e5a960d8d226ffbdb82b3af41b4c6a01b0230cc6e0dce",
			"0396d8808641463b41fe31b11717294e2e14941454ba6b27967ed571b9848ae768",
			"02aaf9583bd5aa8e5993666e11785ff3b1b0a97694003b22cbd8a2b7150ff27736",
			"02dfb76a88100c2b6cd7ad9c051bc9ef9daf74c9fa13a99cb870865a046a9772f1",
		},
		ProtocolID: "/bitcoin-testnet/alert-system/0.0.1",
		TopicName:  "bitcoin_alert_system_testnet",
	},
	"stn": {
		GenesisKeys: []string{
			"0203aa8ca16b6b247b109b65e9d7a0f3a23ccae8f093f792e63bf5ce2567f572bc",
			"0289d3a1c6dacd0ee3de7e5a960d8d226ffbdb82b3af41b4c6a01b0230cc6e0dce",
			"0396d8808641463b41fe31b11717294e2e14941454ba6b27967ed571b9848ae768",
			"02aaf9583bd5aa8e5993666e11785ff3b1b0a97694003b22cbd8a2b7150ff27736",
			"02dfb76a88100c2b6cd7ad9c051bc9ef9daf74c9fa13a99cb870865a046a9772f1",
		},
		ProtocolID: "/bitcoin-stn/alert-system/0.0.1",
		TopicName:  "bitcoin_alert_system_stn",
	},
}

// resolveAlertNetworkConfig fills every empty field of configured with the
// default for network; non-empty fields are explicit overrides and are kept.
// Networks without a public alert network (regtest, teratestnet, tstn) fall
// back to the mainnet keys and protocol on a network-suffixed topic, and
// report known=false.
func resolveAlertNetworkConfig(network string, configured alertNetworkConfig) (resolved alertNetworkConfig, known bool) {
	defaults, known := alertNetworkDefaults[network]
	if !known {
		mainnet := alertNetworkDefaults["mainnet"]
		defaults = alertNetworkConfig{
			GenesisKeys: mainnet.GenesisKeys,
			ProtocolID:  mainnet.ProtocolID,
			TopicName:   mainnet.TopicName + "_" + network,
		}
	}

	resolved = configured

	if !hasNonBlank(resolved.GenesisKeys) {
		// copy so callers cannot mutate the package defaults
		resolved.GenesisKeys = append([]string(nil), defaults.GenesisKeys...)
	}

	if resolved.ProtocolID == "" {
		resolved.ProtocolID = defaults.ProtocolID
	}

	if resolved.TopicName == "" {
		resolved.TopicName = defaults.TopicName
	}

	return resolved, known
}

func hasNonBlank(values []string) bool {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return true
		}
	}

	return false
}
