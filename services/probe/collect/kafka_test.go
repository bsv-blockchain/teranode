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

func TestWatchedTopicsListsEveryTopicOnceInOrder(t *testing.T) {
	k := settings.KafkaSettings{
		Subtrees: "st", TxMeta: "tm", Blocks: "bl", BlocksFinal: "bf",
		InvalidBlocks: "ib", InvalidSubtrees: "is", LegacyInv: "li",
		RejectedTx: "rt", TxPolicyRejected: "tp",
		ValidatorTxsConfig: &url.URL{Scheme: "kafka", Host: "broker:9092", Path: "/st"},
	}

	require.Equal(t, []string{"bf", "bl", "ib", "is", "li", "rt", "st", "tm", "tp"}, WatchedTopics(k))
}

func TestWatchedTopicsSkipsEmptyNames(t *testing.T) {
	require.Equal(t, []string{"st"}, WatchedTopics(settings.KafkaSettings{Subtrees: "st"}))
	require.Empty(t, WatchedTopics(settings.KafkaSettings{}))
}

func TestSortTopicLagsPutsTheWorstTopicFirstAndNoConsumersLast(t *testing.T) {
	lag := func(v int64) *int64 { return &v }

	topics := []snapshot.TopicLag{
		{Topic: "idle", Lag: lag(0)},
		{Topic: "orphan"},
		{Topic: "b-busy", Lag: lag(500)},
		{Topic: "a-busy", Lag: lag(500)},
		{Topic: "worst", Lag: lag(9000)},
		{Topic: "abandoned"},
	}

	sortTopicLags(topics)

	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = topic.Topic
	}

	require.Equal(t, []string{"worst", "a-busy", "b-busy", "idle", "abandoned", "orphan"}, names)
}

func TestTopicsFromSettingsSkipsEmptyNames(t *testing.T) {
	k := settings.KafkaSettings{Subtrees: "st", Blocks: ""}

	require.Equal(t, map[string]string{snapshot.FlowSubtrees: "st"}, TopicsFromSettings(k))
}

func TestTopicsFromSettingsOmitsTxInputWhenKafkaPathUnset(t *testing.T) {
	k := settings.KafkaSettings{ValidatorTxsConfig: &url.URL{Scheme: "kafka", Host: "broker:9092"}}

	require.Empty(t, TopicsFromSettings(k))
}
