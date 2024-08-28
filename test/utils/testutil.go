package helper

import (
	"github.com/bitcoin-sv/ubsv/errors"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
)

var (
	framework *tf.BitcoinTestFramework
)

func SetupBitcoinTestFramework(composeFiles []string, settingsMap map[string]string) (*tf.BitcoinTestFramework, error) {
	framework = tf.NewBitcoinTestFramework(composeFiles)
	if err := framework.SetupNodes(settingsMap); err != nil {
		return nil, errors.NewConfigurationError("Error setting up nodes: %v\n", err)
	}
	return framework, nil
}

func TearDownBitcoinTestFramework(framework *tf.BitcoinTestFramework) error {
	if err := framework.StopNodesWithRmVolume(); err != nil {
		return errors.NewConfigurationError("Error stopping nodes: %v\n", err)
	}
	return nil
}
