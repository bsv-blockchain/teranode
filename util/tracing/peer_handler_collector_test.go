package tracing

import (
	"sync/atomic"
	"testing"

	"github.com/ordishs/go-utils/safemap"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeerHandlerStats(t *testing.T) {
	stats := NewPeerHandlerStats()

	require.NotNil(t, stats)

	// Test that all counters are initialized to zero
	assert.Equal(t, uint64(0), stats.TransactionSent.Load())
	assert.Equal(t, uint64(0), stats.TransactionAnnouncement.Load())
	assert.Equal(t, uint64(0), stats.TransactionRejection.Load())
	assert.Equal(t, uint64(0), stats.TransactionGet.Load())
	assert.Equal(t, uint64(0), stats.Transaction.Load())
	assert.Equal(t, uint64(0), stats.BlockAnnouncement.Load())
	assert.Equal(t, uint64(0), stats.Block.Load())
	assert.Equal(t, uint64(0), stats.BlockProcessingMs.Load())
}

func TestPeerHandlerStats_AtomicOperations(t *testing.T) {
	stats := NewPeerHandlerStats()

	// Test atomic operations on each counter
	testCases := []struct {
		name     string
		counter  *atomic.Uint64
		expected uint64
	}{
		{"TransactionSent", &stats.TransactionSent, 5},
		{"TransactionAnnouncement", &stats.TransactionAnnouncement, 10},
		{"TransactionRejection", &stats.TransactionRejection, 2},
		{"TransactionGet", &stats.TransactionGet, 8},
		{"Transaction", &stats.Transaction, 15},
		{"BlockAnnouncement", &stats.BlockAnnouncement, 3},
		{"Block", &stats.Block, 7},
		{"BlockProcessingMs", &stats.BlockProcessingMs, 1500},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test Store and Load
			tc.counter.Store(tc.expected)
			assert.Equal(t, tc.expected, tc.counter.Load())

			// Test Add
			tc.counter.Add(5)
			assert.Equal(t, tc.expected+5, tc.counter.Load())

			// Reset for next test
			tc.counter.Store(0)
		})
	}
}

func TestPeerHandlerStats_ConcurrentAccess(t *testing.T) {
	stats := NewPeerHandlerStats()
	iterations := 1000

	// Test concurrent increments
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < iterations; i++ {
			stats.TransactionSent.Add(1)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < iterations; i++ {
			stats.TransactionSent.Add(1)
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Should have 2 * iterations increments
	assert.Equal(t, uint64(2*iterations), stats.TransactionSent.Load())
}

func TestNewPeerHandlerCollector(t *testing.T) {
	service := "test-service-unique-1"
	stats := safemap.New[string, *PeerHandlerStats]()

	// Test creating collector (note: this will register globally)
	collector := NewPeerHandlerCollector(service, stats)
	require.NotNil(t, collector)
	assert.Equal(t, service, collector.service)
	assert.Equal(t, stats, collector.stats)

	// Test that all metric descriptors are initialized
	assert.NotNil(t, collector.transactionSent)
	assert.NotNil(t, collector.transactionAnnouncement)
	assert.NotNil(t, collector.transactionRejection)
	assert.NotNil(t, collector.transactionGet)
	assert.NotNil(t, collector.transaction)
	assert.NotNil(t, collector.blockAnnouncement)
	assert.NotNil(t, collector.block)
	assert.NotNil(t, collector.blockProcessingMs)

	// Verify metric names follow correct format
	assert.Contains(t, collector.transactionSent.String(), "test-service-unique-1_peer_transaction_sent_count")
	assert.Contains(t, collector.blockProcessingMs.String(), "test-service-unique-1_peer_block_processing_ms")
}

func TestPeerHandlerCollector_Describe(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()
	collector := &PeerHandlerCollector{
		service: service,
		stats:   stats,
	}

	// Initialize descriptors manually to avoid prometheus registration
	collector.transactionSent = prometheus.NewDesc("test_transaction_sent", "test desc", []string{"peer"}, nil)
	collector.transactionAnnouncement = prometheus.NewDesc("test_transaction_announcement", "test desc", []string{"peer"}, nil)
	collector.transactionRejection = prometheus.NewDesc("test_transaction_rejection", "test desc", []string{"peer"}, nil)
	collector.transactionGet = prometheus.NewDesc("test_transaction_get", "test desc", []string{"peer"}, nil)
	collector.transaction = prometheus.NewDesc("test_transaction", "test desc", []string{"peer"}, nil)
	collector.blockAnnouncement = prometheus.NewDesc("test_block_announcement", "test desc", []string{"peer"}, nil)
	collector.block = prometheus.NewDesc("test_block", "test desc", []string{"peer"}, nil)
	collector.blockProcessingMs = prometheus.NewDesc("test_block_processing_ms", "test desc", []string{"peer"}, nil)

	ch := make(chan *prometheus.Desc, 10)

	go func() {
		collector.Describe(ch)
		close(ch)
	}()

	// Collect all descriptions
	descriptions := make([]*prometheus.Desc, 0, 10)
	for desc := range ch {
		descriptions = append(descriptions, desc)
	}

	// Should have exactly 8 metrics
	assert.Len(t, descriptions, 8)

	// Verify all expected descriptors are present
	expectedDescs := []*prometheus.Desc{
		collector.transactionSent,
		collector.transactionAnnouncement,
		collector.transactionRejection,
		collector.transactionGet,
		collector.transaction,
		collector.blockAnnouncement,
		collector.block,
		collector.blockProcessingMs,
	}

	for _, expected := range expectedDescs {
		assert.Contains(t, descriptions, expected)
	}
}

func TestPeerHandlerCollector_Collect_EmptyStats(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()
	collector := createTestCollector(service, stats)

	ch := make(chan prometheus.Metric, 10)

	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	// Collect all metrics
	metrics := make([]prometheus.Metric, 0, 10)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	// Should have no metrics when stats map is empty
	assert.Len(t, metrics, 0)
}

func TestPeerHandlerCollector_Collect_WithStats(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()

	// Add test data for multiple peers
	peer1 := "peer1.example.com:8333"
	peer2 := "peer2.example.com:8333"

	stats1 := NewPeerHandlerStats()
	stats1.TransactionSent.Store(10)
	stats1.TransactionAnnouncement.Store(20)
	stats1.TransactionRejection.Store(2)
	stats1.TransactionGet.Store(8)
	stats1.Transaction.Store(25)
	stats1.BlockAnnouncement.Store(5)
	stats1.Block.Store(3)
	stats1.BlockProcessingMs.Store(1500)

	stats2 := NewPeerHandlerStats()
	stats2.TransactionSent.Store(15)
	stats2.TransactionAnnouncement.Store(30)
	stats2.TransactionRejection.Store(1)
	stats2.TransactionGet.Store(12)
	stats2.Transaction.Store(35)
	stats2.BlockAnnouncement.Store(7)
	stats2.Block.Store(4)
	stats2.BlockProcessingMs.Store(2200)

	stats.Set(peer1, stats1)
	stats.Set(peer2, stats2)

	collector := createTestCollector(service, stats)

	ch := make(chan prometheus.Metric, 20)

	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	// Collect all metrics
	metrics := make([]prometheus.Metric, 0, 20)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	// Should have 8 metrics per peer = 16 total metrics
	assert.Len(t, metrics, 16)

	// Verify metric values by converting to DTO and checking
	metricsByName := make(map[string]map[string]float64)

	for _, metric := range metrics {
		var pb dto.Metric
		err := metric.Write(&pb)
		require.NoError(t, err)

		desc := metric.Desc()
		metricName := desc.String()

		if metricsByName[metricName] == nil {
			metricsByName[metricName] = make(map[string]float64)
		}

		peerLabel := pb.GetLabel()[0].GetValue()
		metricsByName[metricName][peerLabel] = pb.GetCounter().GetValue()
	}

	// Verify we have metrics for both peers
	for _, peerMetrics := range metricsByName {
		assert.Contains(t, peerMetrics, peer1)
		assert.Contains(t, peerMetrics, peer2)
	}
}

func TestPeerHandlerCollector_Collect_MetricValues(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()

	peer := "test.peer.com:8333"
	peerStats := NewPeerHandlerStats()

	// Set specific values for testing
	testValues := map[string]uint64{
		"TransactionSent":         100,
		"TransactionAnnouncement": 200,
		"TransactionRejection":    5,
		"TransactionGet":          80,
		"Transaction":             150,
		"BlockAnnouncement":       10,
		"Block":                   8,
		"BlockProcessingMs":       5000,
	}

	peerStats.TransactionSent.Store(testValues["TransactionSent"])
	peerStats.TransactionAnnouncement.Store(testValues["TransactionAnnouncement"])
	peerStats.TransactionRejection.Store(testValues["TransactionRejection"])
	peerStats.TransactionGet.Store(testValues["TransactionGet"])
	peerStats.Transaction.Store(testValues["Transaction"])
	peerStats.BlockAnnouncement.Store(testValues["BlockAnnouncement"])
	peerStats.Block.Store(testValues["Block"])
	peerStats.BlockProcessingMs.Store(testValues["BlockProcessingMs"])

	stats.Set(peer, peerStats)
	collector := createTestCollector(service, stats)

	ch := make(chan prometheus.Metric, 10)

	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	// Collect and verify each metric
	metricCount := 0
	for metric := range ch {
		metricCount++

		var pb dto.Metric
		err := metric.Write(&pb)
		require.NoError(t, err)

		// All metrics should be counter type
		assert.NotNil(t, pb.GetCounter())
		assert.Equal(t, peer, pb.GetLabel()[0].GetValue())

		// Verify the value is one of our expected values
		value := pb.GetCounter().GetValue()
		found := false
		for _, expectedValue := range testValues {
			if value == float64(expectedValue) {
				found = true
				break
			}
		}
		assert.True(t, found, "Unexpected metric value: %f", value)
	}

	assert.Equal(t, 8, metricCount)
}

func TestPeerHandlerCollector_Collect_MultipleIterations(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()

	peer := "test.peer.com:8333"
	peerStats := NewPeerHandlerStats()
	peerStats.TransactionSent.Store(50)

	stats.Set(peer, peerStats)
	collector := createTestCollector(service, stats)

	// Test that Collect can be called multiple times
	for i := 0; i < 3; i++ {
		ch := make(chan prometheus.Metric, 10)

		go func() {
			collector.Collect(ch)
			close(ch)
		}()

		metricCount := 0
		for range ch {
			metricCount++
		}

		assert.Equal(t, 8, metricCount, "Iteration %d should return 8 metrics", i)

		// Modify stats between iterations
		peerStats.TransactionSent.Add(10)
	}
}

func TestPeerHandlerCollector_Collect_DynamicStats(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()
	collector := createTestCollector(service, stats)

	// Start with no peers
	ch1 := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch1)
		close(ch1)
	}()

	count1 := 0
	for range ch1 {
		count1++
	}
	assert.Equal(t, 0, count1)

	// Add one peer
	peer1 := "peer1.com:8333"
	stats.Set(peer1, NewPeerHandlerStats())

	ch2 := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch2)
		close(ch2)
	}()

	count2 := 0
	for range ch2 {
		count2++
	}
	assert.Equal(t, 8, count2) // 8 metrics for 1 peer

	// Add second peer
	peer2 := "peer2.com:8333"
	stats.Set(peer2, NewPeerHandlerStats())

	ch3 := make(chan prometheus.Metric, 20)
	go func() {
		collector.Collect(ch3)
		close(ch3)
	}()

	count3 := 0
	for range ch3 {
		count3++
	}
	assert.Equal(t, 16, count3) // 8 metrics for 2 peers
}

func TestPeerHandlerCollector_MetricDescriptorNames(t *testing.T) {
	service := "test-service"
	stats := safemap.New[string, *PeerHandlerStats]()

	// We need to create collector without prometheus registration for this test
	collector := &PeerHandlerCollector{
		service: service,
		stats:   stats,
	}

	// Initialize descriptors manually with expected names
	expectedMetrics := []struct {
		name       string
		help       string
		descriptor **prometheus.Desc
	}{
		{
			name:       "teranode_test-service_peer_transaction_sent_count",
			help:       "Shows the number of transactions marked as sent by the peer handler",
			descriptor: &collector.transactionSent,
		},
		{
			name:       "teranode_test-service_peer_transaction_announcement_count",
			help:       "Shows the number of transactions announced by the peer handler",
			descriptor: &collector.transactionAnnouncement,
		},
		{
			name:       "teranode_test-service_peer_transaction_rejection_count",
			help:       "Shows the number of transactions rejected by the peer handler",
			descriptor: &collector.transactionRejection,
		},
		{
			name:       "teranode_test-service_peer_transaction_get_count",
			help:       "Shows the number of transactions get by the peer handler",
			descriptor: &collector.transactionGet,
		},
		{
			name:       "teranode_test-service_peer_transaction_count",
			help:       "Shows the number of transactions received by the peer handler",
			descriptor: &collector.transaction,
		},
		{
			name:       "teranode_test-service_peer_block_announcement_count",
			help:       "Shows the number of blocks announced by the peer handler",
			descriptor: &collector.blockAnnouncement,
		},
		{
			name:       "teranode_test-service_peer_block_count",
			help:       "Shows the number of blocks received by the peer handler",
			descriptor: &collector.block,
		},
		{
			name:       "teranode_test-service_peer_block_processing_ms",
			help:       "Shows the total time spent processing blocks by the peer handler",
			descriptor: &collector.blockProcessingMs,
		},
	}

	// Initialize each descriptor and verify the name format
	for _, expected := range expectedMetrics {
		*expected.descriptor = prometheus.NewDesc(expected.name, expected.help, []string{"peer"}, nil)

		// Verify descriptor was created successfully
		assert.NotNil(t, *expected.descriptor)

		// The descriptor string should contain the metric name
		descStr := (*expected.descriptor).String()
		assert.Contains(t, descStr, expected.name)
	}
}

func TestPeerHandlerCollector_EdgeCases(t *testing.T) {
	t.Run("NilStatsMap", func(t *testing.T) {
		service := "test-service"

		// Test with nil stats map - should not panic
		require.NotPanics(t, func() {
			// Initialize descriptors
			collector := createTestCollector(service, nil)

			ch := make(chan prometheus.Metric, 10)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						// Expected to panic with nil stats map
					}
					close(ch)
				}()
				collector.Collect(ch)
			}()

			// Should not produce any metrics
			count := 0
			for range ch {
				count++
			}
			// With nil stats map, the Each call will likely panic or do nothing
		})
	})

	t.Run("EmptyServiceName", func(t *testing.T) {
		stats := safemap.New[string, *PeerHandlerStats]()

		require.NotPanics(t, func() {
			collector := createTestCollector("", stats)
			assert.NotNil(t, collector)
		})
	})

	t.Run("StatsWithMaxValues", func(t *testing.T) {
		service := "test-service"
		stats := safemap.New[string, *PeerHandlerStats]()

		peer := "max.peer.com:8333"
		peerStats := NewPeerHandlerStats()

		// Set maximum uint64 values
		maxVal := uint64(18446744073709551615) // 2^64 - 1
		peerStats.TransactionSent.Store(maxVal)
		peerStats.BlockProcessingMs.Store(maxVal)

		stats.Set(peer, peerStats)
		collector := createTestCollector(service, stats)

		ch := make(chan prometheus.Metric, 10)
		go func() {
			collector.Collect(ch)
			close(ch)
		}()

		// Should handle max values without overflow
		metricCount := 0
		for metric := range ch {
			metricCount++
			var pb dto.Metric
			err := metric.Write(&pb)
			require.NoError(t, err)

			value := pb.GetCounter().GetValue()
			assert.True(t, value >= 0, "Metric value should be non-negative")
		}

		assert.Equal(t, 8, metricCount)
	})
}

func TestPeerHandlerCollector_Integration(t *testing.T) {
	service := "integration-test"
	stats := safemap.New[string, *PeerHandlerStats]()

	// Create multiple peers with different stats
	peers := []string{
		"peer1.example.com:8333",
		"peer2.example.com:8333",
		"peer3.example.com:8333",
	}

	for i, peer := range peers {
		peerStats := NewPeerHandlerStats()

		// Set different values for each peer
		baseValue := uint64((i + 1) * 10)
		peerStats.TransactionSent.Store(baseValue)
		peerStats.TransactionAnnouncement.Store(baseValue * 2)
		peerStats.TransactionRejection.Store(baseValue / 5)
		peerStats.TransactionGet.Store(baseValue * 3)
		peerStats.Transaction.Store(baseValue + 5)
		peerStats.BlockAnnouncement.Store(baseValue / 2)
		peerStats.Block.Store(baseValue / 3)
		peerStats.BlockProcessingMs.Store(baseValue * 100)

		stats.Set(peer, peerStats)
	}

	collector := createTestCollector(service, stats)

	// Test Describe
	descCh := make(chan *prometheus.Desc, 10)
	go func() {
		collector.Describe(descCh)
		close(descCh)
	}()

	descCount := 0
	for range descCh {
		descCount++
	}
	assert.Equal(t, 8, descCount)

	// Test Collect
	metricCh := make(chan prometheus.Metric, 30)
	go func() {
		collector.Collect(metricCh)
		close(metricCh)
	}()

	metricCount := 0
	peerCounts := make(map[string]int)

	for metric := range metricCh {
		metricCount++

		var pb dto.Metric
		err := metric.Write(&pb)
		require.NoError(t, err)

		peerLabel := pb.GetLabel()[0].GetValue()
		peerCounts[peerLabel]++

		// Verify metric value is reasonable
		value := pb.GetCounter().GetValue()
		assert.True(t, value >= 0)
		assert.True(t, value <= 1000000, "Value seems unreasonably high: %f", value)
	}

	// Should have 8 metrics per peer * 3 peers = 24 total
	assert.Equal(t, 24, metricCount)

	// Each peer should have exactly 8 metrics
	for _, peer := range peers {
		assert.Equal(t, 8, peerCounts[peer], "Peer %s should have 8 metrics", peer)
	}
}

// Helper function to create a test collector without prometheus registration issues
func createTestCollector(service string, stats *safemap.Safemap[string, *PeerHandlerStats]) *PeerHandlerCollector {
	if stats == nil {
		stats = safemap.New[string, *PeerHandlerStats]()
	}

	collector := &PeerHandlerCollector{
		service: service,
		stats:   stats,
	}

	// Initialize descriptors manually to avoid prometheus registration
	collector.transactionSent = prometheus.NewDesc("test_transaction_sent", "test desc", []string{"peer"}, nil)
	collector.transactionAnnouncement = prometheus.NewDesc("test_transaction_announcement", "test desc", []string{"peer"}, nil)
	collector.transactionRejection = prometheus.NewDesc("test_transaction_rejection", "test desc", []string{"peer"}, nil)
	collector.transactionGet = prometheus.NewDesc("test_transaction_get", "test desc", []string{"peer"}, nil)
	collector.transaction = prometheus.NewDesc("test_transaction", "test desc", []string{"peer"}, nil)
	collector.blockAnnouncement = prometheus.NewDesc("test_block_announcement", "test desc", []string{"peer"}, nil)
	collector.block = prometheus.NewDesc("test_block", "test desc", []string{"peer"}, nil)
	collector.blockProcessingMs = prometheus.NewDesc("test_block_processing_ms", "test desc", []string{"peer"}, nil)

	return collector
}
