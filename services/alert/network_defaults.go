package alert

import (
	"strings"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
)

// maxGenesisKeys is the most genesis keys alerts can be verified against: go-alert-system
// reads the active keys for signature checks with a page size of 10 (models.GetActivePublicKey),
// so any further key would never count as a signer.
const maxGenesisKeys = 10

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
//
// The resolved genesis keys are trimmed, stripped of blank entries and deduplicated
// ignoring case; a key that is not a hex-encoded compressed public key, or more than
// maxGenesisKeys keys, is an error.
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
	resolved.GenesisKeys = uniqueKeys(configured.GenesisKeys)

	if len(resolved.GenesisKeys) == 0 {
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

	if err = validateGenesisKeys(resolved.GenesisKeys); err != nil {
		return alertNetworkConfig{}, nil, err
	}

	return resolved, mainnetFallback, nil
}

// uniqueKeys returns keys trimmed, without blank entries and without keys repeated in
// any case, in their original order.
func uniqueKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	unique := make([]string, 0, len(keys))

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		normalized := strings.ToLower(key)
		if _, ok := seen[normalized]; ok {
			continue
		}

		seen[normalized] = struct{}{}
		unique = append(unique, key)
	}

	return unique
}

// validateGenesisKeys checks that keys holds at most maxGenesisKeys keys and that each is a
// hex-encoded compressed public key, mirroring the check go-alert-system runs on the
// genesis keys of a config it loads itself.
func validateGenesisKeys(keys []string) error {
	if len(keys) > maxGenesisKeys {
		return errors.NewConfigurationError("alert_genesis_keys has %d keys, at most %d are supported", len(keys), maxGenesisKeys)
	}

	for i, key := range keys {
		if len(key) != 2*bec.PubKeyBytesLenCompressed {
			return errors.NewConfigurationError("alert_genesis_keys[%d] %q is not a compressed public key: expected %d hex characters", i, key, 2*bec.PubKeyBytesLenCompressed)
		}

		if _, err := bec.PublicKeyFromString(key); err != nil {
			return errors.NewConfigurationError("alert_genesis_keys[%d] %q is not a valid public key", i, key, err)
		}
	}

	return nil
}
