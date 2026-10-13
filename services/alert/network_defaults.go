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
// tstn) fall back to the mainnet keys and protocol on a network-suffixed topic,
// and report known=false.
func resolveAlertNetworkConfig(network string, configured alertNetworkConfig) (resolved alertNetworkConfig, known bool, err error) {
	defaults, err := config.NetworkDefaults(network)

	switch {
	case err == nil:
		known = true
	case !errors.Is(err, config.ErrInvalidEnvironment):
		return alertNetworkConfig{}, false, err
	default:
		if defaults, err = config.NetworkDefaults(config.EnvironmentMainnet); err != nil {
			return alertNetworkConfig{}, false, err
		}

		defaults.TopicName += "_" + network
	}

	resolved = configured

	if !hasNonBlank(resolved.GenesisKeys) {
		resolved.GenesisKeys = defaults.GenesisKeys
	}

	if resolved.ProtocolID == "" {
		resolved.ProtocolID = defaults.ProtocolID
	}

	if resolved.TopicName == "" {
		resolved.TopicName = defaults.TopicName
	}

	return resolved, known, nil
}

func hasNonBlank(values []string) bool {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return true
		}
	}

	return false
}
