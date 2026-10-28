package collect

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestTopicsFromSettings(t *testing.T) {
	k := settings.KafkaSettings{
		Subtrees:           "st",
		TxMeta:             "tm",
		Blocks:             "bl",
		BlocksFinal:        "bf",
		ValidatorTxsConfig: &url.URL{Scheme: "kafka", Host: "broker:9092", Path: "/validator-txs", RawQuery: "partitions=4"},
	}

	require.Equal(t, map[string]string{
		snapshot.FlowSubtrees:    "st",
		snapshot.FlowTxMeta:      "tm",
		snapshot.FlowBlocks:      "bl",
		snapshot.FlowBlocksFinal: "bf",
		snapshot.FlowTxInput:     "validator-txs",
	}, TopicsFromSettings(k))
}

func TestTopicsFromSettingsSkipsEmptyNames(t *testing.T) {
	k := settings.KafkaSettings{Subtrees: "st", Blocks: ""}

	require.Equal(t, map[string]string{snapshot.FlowSubtrees: "st"}, TopicsFromSettings(k))
}

func TestTopicsFromSettingsOmitsTxInputWhenKafkaPathUnset(t *testing.T) {
	k := settings.KafkaSettings{ValidatorTxsConfig: &url.URL{Scheme: "kafka", Host: "broker:9092"}}

	require.Empty(t, TopicsFromSettings(k))
}
