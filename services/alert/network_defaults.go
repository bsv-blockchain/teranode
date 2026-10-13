package alert

import (
	"strings"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/teranode/errors"
)

// alertNetworkConfig identifies an alert network: the genesis keys that may
// sign alerts, and the libp2p protocol and pubsub topic they travel on.
type alertNetworkConfig struct {
	GenesisKeys []string
	ProtocolID  string
	TopicName   string
}

// resolveAlertNetworkConfig fills every empty field of configured with the
// default for network, taken from go-alert-system's embedded config for that
// environment (mainnet, testnet, stn); non-empty fields are explicit overrides
// and are kept. Networks without a public alert network (regtest, teratestnet,
// tstn) fill empty genesis keys and protocol ID from mainnet and an empty topic
// with a network-suffixed one; mainnetFallback names the fields that took a
// mainnet value, and is empty when every field was set explicitly.
func resolveAlertNetworkConfig(network string, configured alertNetworkConfig) (resolved alertNetworkConfig, mainnetFallback []string, err error) {
	defaults, err := config.NetworkDefaults(network)
	known := err == nil

	if !known {
		if !errors.Is(err, config.ErrInvalidEnvironment) {
			return alertNetworkConfig{}, nil, err
		}

		if defaults, err = config.NetworkDefaults(config.EnvironmentMainnet); err != nil {
			return alertNetworkConfig{}, nil, err
		}

		defaults.TopicName += "_" + network
	}

	resolved = configured

	if !hasNonBlank(resolved.GenesisKeys) {
		resolved.GenesisKeys = defaults.GenesisKeys

		if !known {
			mainnetFallback = append(mainnetFallback, "alert_genesis_keys")
		}
	}

	if resolved.ProtocolID == "" {
		resolved.ProtocolID = defaults.ProtocolID

		if !known {
			mainnetFallback = append(mainnetFallback, "alert_protocol_id")
		}
	}

	if resolved.TopicName == "" {
		resolved.TopicName = defaults.TopicName
	}

	return resolved, mainnetFallback, nil
}

func hasNonBlank(values []string) bool {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return true
		}
	}

	return false
}
