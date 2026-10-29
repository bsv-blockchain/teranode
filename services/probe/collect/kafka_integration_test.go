//go:build test_kafka

package collect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaTestTimeout = 10 * time.Second

func newTopic(t *testing.T, env *kafkatest.Env, topic string, messages int) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(env.BrokerAddr))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	records := make([]*kgo.Record, messages)
	for i := range records {
		records[i] = &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("m%d", i))}
	}

	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

func commit(t *testing.T, env *kafkatest.Env, group, topic string, at int64) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(env.BrokerAddr))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: at})

	resp, err := kadm.NewClient(cl).CommitOffsets(ctx, group, offsets)
	require.NoError(t, err)
	require.NoError(t, resp.Error())
}

func collectLag(t *testing.T, k *Kafka, flow string) *int64 {
	t.Helper()

	patch, err := k.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	s := snapshot.NewSnapshot("test")
	patch(&s)

	f := s.Flow(flow)
	require.NotNil(t, f)

	return f.Lag
}

func newCollector(t *testing.T, env *kafkatest.Env, topics map[string]string) *Kafka {
	t.Helper()

	k, err := NewKafka([]string{env.BrokerAddr}, topics, kafkaTestTimeout)
	require.NoError(t, err)
	t.Cleanup(k.Close)

	return k
}

func TestLagAfterProducingWithoutConsuming(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "lagtopic", 50)
	commit(t, env, "group-a", "lagtopic", 20)

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "lagtopic"})

	lag := collectLag(t, k, snapshot.FlowSubtrees)
	require.NotNil(t, lag)
	require.Equal(t, int64(30), *lag)
}

func TestLagSumsAcrossConsumerGroups(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "sumtopic", 50)
	commit(t, env, "group-a", "sumtopic", 20)
	commit(t, env, "group-b", "sumtopic", 45)

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "sumtopic"})

	lag := collectLag(t, k, snapshot.FlowSubtrees)
	require.NotNil(t, lag)
	require.Equal(t, int64(35), *lag)
}

func TestLagZeroWhenCaughtUp(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "caughtup", 50)
	commit(t, env, "group-a", "caughtup", 50)

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "caughtup"})

	lag := collectLag(t, k, snapshot.FlowSubtrees)
	require.NotNil(t, lag)
	require.Equal(t, int64(0), *lag)
}

func TestNoGroupsMeansNoLag(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "nogroups", 50)

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "nogroups"})

	require.Nil(t, collectLag(t, k, snapshot.FlowSubtrees))
}

func TestGroupOnOtherTopicContributesNothing(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "mine", 10)
	newTopic(t, env, "other", 40)
	commit(t, env, "group-a", "other", 0)

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "mine"})

	require.Nil(t, collectLag(t, k, snapshot.FlowSubtrees))
}

func TestWatchedTopicsReportLagAndMissingConsumers(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "watched-behind", 50)
	newTopic(t, env, "watched-orphan", 10)
	commit(t, env, "group-a", "watched-behind", 20)

	k := newCollector(t, env, map[string]string{})
	k.Watch([]string{"watched-orphan", "watched-behind", "watched-absent"})

	patch, err := k.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	s := snapshot.NewSnapshot("test")
	patch(&s)

	behind := int64(30)

	require.Equal(t, []snapshot.TopicLag{
		{Topic: "watched-behind", Lag: &behind},
		{Topic: "watched-absent"},
		{Topic: "watched-orphan"},
	}, s.Kafka)
}

func TestBrokerDown(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "down", 5)
	commit(t, env, "group-a", "down", 1)

	timeout := 2 * time.Second
	k, err := NewKafka([]string{env.BrokerAddr}, map[string]string{snapshot.FlowSubtrees: "down"}, timeout)
	require.NoError(t, err)
	t.Cleanup(k.Close)

	require.NoError(t, env.Container.Stop(context.Background(), nil))

	start := time.Now()
	patch, err := k.Collect(context.Background())

	require.Error(t, err)
	require.Nil(t, patch)
	require.Less(t, time.Since(start), timeout+2*time.Second)
}

func TestUncommittedMemberCountsFullTopicAsLag(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())
	newTopic(t, env, "uncommitted", 50)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(env.BrokerAddr),
		kgo.ConsumerGroup("group-live"),
		kgo.ConsumeTopics("uncommitted"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		cl.Close()
	})

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "uncommitted"})

	require.Eventually(t, func() bool {
		patch, cerr := k.Collect(context.Background())
		if cerr != nil || patch == nil {
			return false
		}

		s := snapshot.NewSnapshot("test")
		patch(&s)

		f := s.Flow(snapshot.FlowSubtrees)

		return f != nil && f.Lag != nil && *f.Lag == 50
	}, 30*time.Second, 500*time.Millisecond)
}

func TestMultiPartitionLagSumsAcrossPartitionsAndGroups(t *testing.T) {
	env := kafkatest.MustStartEnv(t, context.Background())

	cl, err := kgo.NewClient(kgo.SeedBrokers(env.BrokerAddr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adm := kadm.NewClient(cl)
	_, err = adm.CreateTopic(ctx, 3, 1, nil, "multi")
	require.NoError(t, err)

	counts := []int{10, 20, 30}

	for p, n := range counts {
		for i := 0; i < n; i++ {
			require.NoError(t, cl.ProduceSync(ctx, &kgo.Record{Topic: "multi", Partition: int32(p), Value: []byte("m")}).FirstErr())
		}
	}

	commitAll := func(group string, at map[int32]int64) {
		offsets := kadm.Offsets{}
		for p, o := range at {
			offsets.Add(kadm.Offset{Topic: "multi", Partition: p, At: o})
		}

		resp, cerr := adm.CommitOffsets(ctx, group, offsets)
		require.NoError(t, cerr)
		require.NoError(t, resp.Error())
	}

	commitAll("group-a", map[int32]int64{0: 5, 1: 10, 2: 30})
	commitAll("group-b", map[int32]int64{0: 0, 1: 20})

	k := newCollector(t, env, map[string]string{snapshot.FlowSubtrees: "multi"})

	lag := collectLag(t, k, snapshot.FlowSubtrees)
	require.NotNil(t, lag)
	require.Equal(t, int64((5+10+0)+(10+0+30)), *lag)
}
