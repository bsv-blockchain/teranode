package collect

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Kafka struct {
	client  *kgo.Client
	adm     *kadm.Client
	topics  map[string]string
	watched []string
	timeout time.Duration
}

func NewKafka(hosts []string, topics map[string]string, timeout time.Duration, opts ...kgo.Opt) (*Kafka, error) {
	if len(hosts) == 0 {
		return nil, errors.NewConfigurationError("probe kafka collector needs at least one broker")
	}

	if timeout <= 0 {
		return nil, errors.NewConfigurationError("probe kafka collector needs a positive timeout")
	}

	all := append([]kgo.Opt{
		kgo.SeedBrokers(hosts...),
		kgo.ClientID("teranode-probe"),
	}, opts...)

	client, err := kgo.NewClient(all...)
	if err != nil {
		return nil, errors.NewServiceError("could not create probe kafka client", err)
	}

	copied := make(map[string]string, len(topics))
	for id, topic := range topics {
		copied[id] = topic
	}

	return &Kafka{client: client, adm: kadm.NewClient(client), topics: copied, timeout: timeout}, nil
}

func TopicsFromSettings(k settings.KafkaSettings) map[string]string {
	topics := map[string]string{
		snapshot.FlowSubtrees:    k.Subtrees,
		snapshot.FlowTxMeta:      k.TxMeta,
		snapshot.FlowBlocks:      k.Blocks,
		snapshot.FlowBlocksFinal: k.BlocksFinal,
	}

	if k.ValidatorTxsConfig != nil {
		topics[snapshot.FlowTxInput] = strings.TrimPrefix(k.ValidatorTxsConfig.Path, "/")
	}

	for id, topic := range topics {
		if topic == "" {
			delete(topics, id)
		}
	}

	return topics
}

func WatchedTopics(k settings.KafkaSettings) []string {
	names := []string{
		k.Subtrees, k.TxMeta, k.Blocks, k.BlocksFinal, k.InvalidBlocks,
		k.InvalidSubtrees, k.LegacyInv, k.RejectedTx, k.TxPolicyRejected,
	}

	if k.ValidatorTxsConfig != nil {
		names = append(names, strings.TrimPrefix(k.ValidatorTxsConfig.Path, "/"))
	}

	seen := make(map[string]bool, len(names))
	topics := make([]string, 0, len(names))

	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}

		seen[name] = true
		topics = append(topics, name)
	}

	sort.Strings(topics)

	return topics
}

func (k *Kafka) Watch(topics []string) {
	k.watched = append([]string(nil), topics...)
}

func (k *Kafka) Name() string {
	return "kafka"
}

func (k *Kafka) Close() {
	k.client.Close()
}

func (k *Kafka) Collect(ctx context.Context) (snapshot.Patch, error) {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()

	lags, err := k.groupLags(ctx)
	if err != nil {
		return nil, err
	}

	totals := make(map[string]int64, len(k.topics))
	seen := make(map[string]bool, len(k.topics))

	wanted := make(map[string]bool, len(k.topics)+len(k.watched))
	for _, topic := range k.topics {
		wanted[topic] = true
	}

	for _, topic := range k.watched {
		wanted[topic] = true
	}

	for _, group := range lags {
		for topic, partitions := range group.Lag {
			if !wanted[topic] {
				continue
			}

			for partition, member := range partitions {
				if member.Err != nil {
					return nil, errors.NewServiceError("could not read lag of group %s on %s/%d", group.Group, topic, partition, member.Err)
				}

				seen[topic] = true
				totals[topic] += member.Lag
			}
		}
	}

	lagOf := func(topic string) *int64 {
		if !seen[topic] {
			return nil
		}

		total := totals[topic]

		return &total
	}

	byFlow := make(map[string]*int64, len(k.topics))

	for id, topic := range k.topics {
		byFlow[id] = lagOf(topic)
	}

	topicLags := make([]snapshot.TopicLag, 0, len(k.watched))

	for _, topic := range k.watched {
		topicLags = append(topicLags, snapshot.TopicLag{Topic: topic, Lag: lagOf(topic)})
	}

	sortTopicLags(topicLags)

	return func(s *snapshot.Snapshot) {
		for id, lag := range byFlow {
			if f := s.Flow(id); f != nil {
				f.Lag = lag
			}
		}

		s.Kafka = append([]snapshot.TopicLag(nil), topicLags...)
	}, nil
}

func sortTopicLags(topics []snapshot.TopicLag) {
	sort.Slice(topics, func(i, j int) bool {
		a, b := topics[i], topics[j]

		switch {
		case a.Lag != nil && b.Lag == nil:
			return true
		case a.Lag == nil && b.Lag != nil:
			return false
		case a.Lag != nil && b.Lag != nil && *a.Lag != *b.Lag:
			return *a.Lag > *b.Lag
		default:
			return a.Topic < b.Topic
		}
	})
}

func (k *Kafka) groupLags(ctx context.Context) (kadm.DescribedGroupLags, error) {
	listed, err := k.adm.ListGroups(ctx)
	if err != nil {
		return nil, errors.NewServiceError("could not list kafka consumer groups", err)
	}

	groups := listed.Groups()
	if len(groups) == 0 {
		return nil, nil
	}

	lags, err := k.adm.Lag(ctx, groups...)
	if err != nil {
		return nil, errors.NewServiceError("could not read kafka consumer group lag", err)
	}

	for _, group := range lags {
		if gerr := group.Error(); gerr != nil {
			return nil, errors.NewServiceError("could not read kafka consumer group %s", group.Group, gerr)
		}
	}

	return lags, nil
}
