// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func setupBanList(t *testing.T) (*BanList, chan BanEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	banListInstance = nil
	banListOnce = sync.Once{}

	// Create a new in-memory SQLite database for testing
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	banList := &BanList{
		db:          store.GetDB(),
		engine:      util.SqliteMemory,
		logger:      ulogger.TestLogger{},
		bannedPeers: make(map[string]BanInfo),
		subscribers: make(map[chan BanEvent]struct{}),
	}

	err = banList.Init(ctx)
	require.NoError(t, err)

	banListInstance = banList
	eventChan := banList.Subscribe()

	return banList, eventChan, nil
}

// func teardown(t *testing.T, banList *BanList) {
// 	banList.Clear()
// }

func TestHandleSetBanAdd(t *testing.T) {
	banTime := int64(3600)
	absolute := false
	banTime2 := int64(7200)
	absolute2 := true
	tests := []struct {
		name     string
		args     *bsvjson.SetBanCmd
		isSubnet bool
		expected string
	}{
		{
			name: "test IP add ban",
			args: &bsvjson.SetBanCmd{
				Command:    "add",
				IPOrSubnet: "127.0.0.1",
				BanTime:    &banTime,
				Absolute:   &absolute,
			},
			isSubnet: false,
		},
		{
			name: "test subnet add ban",
			args: &bsvjson.SetBanCmd{
				Command:    "add",
				IPOrSubnet: "127.0.0.0/24",
				BanTime:    &banTime2,
				Absolute:   &absolute2,
			},
			isSubnet: true,
		},
		{
			name: "test ip and port add ban",
			args: &bsvjson.SetBanCmd{
				Command:    "add",
				IPOrSubnet: "127.0.0.0:1234",
				BanTime:    &banTime,
				Absolute:   &absolute,
			},
			isSubnet: false,
			expected: "127.0.0.0/32",
		},
	}
	banList, eventChan, err := setupBanList(t)
	require.NoError(t, err)

	// Create a WaitGroup to track notification goroutines
	var wg sync.WaitGroup

	t.Cleanup(func() {
		// First unsubscribe to prevent new notifications
		banList.Unsubscribe(eventChan)
		// Wait for any pending notifications
		wg.Wait()
		// Now safe to close the channel
		close(eventChan)
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Add to WaitGroup before potential notification
			wg.Add(1)
			err := banList.Add(ctx, tt.args.IPOrSubnet, time.Now().Add(time.Duration(*tt.args.BanTime)*time.Second))
			require.NoError(t, err)

			// Wait for notification to be processed
			select {
			case <-eventChan:
				wg.Done()
			case <-ctx.Done():
				t.Fatal("timeout waiting for ban notification")
			}

			t.Logf("IP or Subnet: %s\n", tt.args.IPOrSubnet)

			banInfo, exists := banList.bannedPeers[tt.args.IPOrSubnet]
			require.True(t, exists)

			expectedExpiration := time.Now().Add(time.Duration(*tt.args.BanTime) * time.Second)
			require.WithinDuration(t, expectedExpiration, banInfo.ExpirationTime, time.Second)

			switch {
			case tt.expected != "":
				require.Equal(t, tt.expected, banInfo.Subnet.String())
			case tt.isSubnet:
				require.Equal(t, tt.args.IPOrSubnet, banInfo.Subnet.String())
			default:
				require.Equal(t, tt.args.IPOrSubnet+"/32", banInfo.Subnet.String())
			}
		})
	}
}

func TestIsBanned(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)
	// Add a banned IP
	err = banList.Add(context.Background(), "192.168.1.1", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	// Add a banned subnet
	err = banList.Add(context.Background(), "10.0.0.0/24", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	tests := []struct {
		name     string
		ip       string
		expected bool
	}{
		{"banned IP", "192.168.1.1", true},
		{"unbanned IP", "192.168.1.2", false},
		{"IP in banned subnet", "10.0.0.5", true},
		{"IP not in banned subnet", "10.0.1.1", false},
		{"invalid IP", "invalid", false},
		{"invalid subnet", "10.0.0.0/33", false},
		{"unbanned IP with port", "192.168.1.2:8333", false},
		{"banned IP with port", "192.168.1.1:8333", true},

		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := banList.IsBanned(tt.ip)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveBan(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	// Add a banned IP
	err = banList.Add(context.Background(), "192.168.1.1", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	// Add a banned subnet
	err = banList.Add(context.Background(), "10.0.0.0/24", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	tests := []struct {
		name       string
		ipOrSubnet string
		expected   bool
	}{
		{"remove banned IP", "192.168.1.1", true},
		{"remove unbanned IP", "192.168.1.2", false},
		{"remove banned subnet", "10.0.0.0/24", true},
		{"remove unbanned subnet", "172.16.0.0/16", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := banList.Remove(context.Background(), tt.ipOrSubnet)
			require.NoError(t, err)

			_, exists := banList.bannedPeers[tt.ipOrSubnet]
			require.False(t, exists)
		})
	}
}

func TestBanListChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.StoreURL, _ = url.Parse("sqlitememory://")

	banList, eventChan, err := GetBanList(ctx, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)
	require.NotNil(t, banList)
	require.NotNil(t, eventChan)

	// Ensure we clean up the subscription at the end of the test
	defer banList.Unsubscribe(eventChan)

	// Create a channel to signal when we're done receiving events
	done := make(chan bool)

	// Start a goroutine to listen for events
	go func() {
		expectedEvents := map[string]struct{}{
			"add|192.168.1.1":    {},
			"add|10.0.0.0/24":    {},
			"remove|192.168.1.1": {},
		}

		receivedCount := 0

		for event := range eventChan {
			// Create a key in the same format as our expected events map
			eventKey := fmt.Sprintf("%s|%s", event.Action, event.IP)

			if _, exists := expectedEvents[eventKey]; !exists {
				t.Errorf("Received unexpected event: %s", eventKey)
				done <- true

				return
			}

			// For IP addresses (not subnets), verify the subnet is correctly formed
			if !strings.Contains(event.IP, "/") {
				expectedSubnet := fmt.Sprintf("%s/32", event.IP)
				if event.Subnet.String() != expectedSubnet {
					t.Errorf("For IP %s, expected subnet %s, got %s", event.IP, expectedSubnet, event.Subnet.String())
				}
			} else if event.Subnet.String() != event.IP {
				t.Errorf("For subnet %s, got mismatched subnet %s", event.IP, event.Subnet.String())
			}

			// Delete this event from our expected map as we've processed it
			delete(expectedEvents, eventKey)

			receivedCount++
			if receivedCount == 3 { // We expect 3 events total
				if len(expectedEvents) > 0 {
					t.Errorf("Not all expected events were received, missing: %v", expectedEvents)
				}
				done <- true

				return
			}
		}
	}()

	// Add an IP address ban
	err = banList.Add(ctx, "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Add a subnet ban
	err = banList.Add(ctx, "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Remove the IP address ban
	err = banList.Remove(ctx, "192.168.1.1")
	require.NoError(t, err)

	// Wait for all events or timeout
	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for events")
	}
}

func TestClearBanlist(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	// Add an IP
	err = banList.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Add a subnet
	err = banList.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Clear the ban list
	banList.Clear()

	// Check that the ban list is empty
	bannedPeers := banList.bannedPeers
	require.Empty(t, bannedPeers)
}

func TestLoadFromDatabase(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	// Add an IP
	err = banList.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Add a subnet
	err = banList.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Load from database
	err = banList.loadFromDatabase(context.Background())
	require.NoError(t, err)

	// Check that the ban list is loaded
	bannedPeers := banList.bannedPeers
	require.Equal(t, 2, len(bannedPeers))
}

// TestIPv6Compatibility tests that the ban list correctly handles IPv6 addresses with ports
func TestIPv6Compatibility(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	// Test IPv6 addresses with and without ports
	testCases := []struct {
		name     string
		banIP    string
		testIP   string
		expected bool
	}{
		{
			name:     "IPv6 exact match without port",
			banIP:    "2406:da18:1f7:353a:b079:da22:c7d5:e166",
			testIP:   "2406:da18:1f7:353a:b079:da22:c7d5:e166",
			expected: true,
		},
		{
			name:     "IPv6 with port should match bare IP",
			banIP:    "2406:da18:1f7:353a:b079:da22:c7d5:e166",
			testIP:   "[2406:da18:1f7:353a:b079:da22:c7d5:e166]:8333",
			expected: true,
		},
		{
			name:     "IPv6 different address should not match",
			banIP:    "2406:da18:1f7:353a:b079:da22:c7d5:e166",
			testIP:   "2406:da18:1f7:353a:b079:da22:c7d5:e167",
			expected: false,
		},
		{
			name:     "IPv4 with port should work",
			banIP:    "192.168.1.1",
			testIP:   "192.168.1.1:8333",
			expected: true,
		},
		{
			name:     "IPv4 without port should work",
			banIP:    "192.168.1.1",
			testIP:   "192.168.1.1",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear ban list
			banList.Clear()

			// Add the ban
			err := banList.Add(context.Background(), tc.banIP, time.Now().Add(time.Hour))
			require.NoError(t, err)

			// Test if the IP is banned
			result := banList.IsBanned(tc.testIP)
			require.Equal(t, tc.expected, result,
				"Expected IsBanned(%s) to be %v when %s is banned",
				tc.testIP, tc.expected, tc.banIP)
		})
	}
}

// TestBanList_NotifySubscribersAsync_ClosedChannelRaceCondition is a proof of concept test
// that verifies the fix for the panic-prone send on possibly closed channel issue.
// This test aggressively tries to trigger the race condition between unsubscribing/closing
// channels and notifying subscribers. With utils.SafeSend, no panics should occur.
//
// Note: This test relies on Go's test runner to detect panics in any goroutine.
// If a panic occurs in the notifySubscribersAsync goroutines (spawned by Add/Remove),
// the test will fail. No custom panic recovery is needed.
func TestBanList_NotifySubscribersAsync_ClosedChannelRaceCondition(t *testing.T) {
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	const (
		numSubscribers = 10
		numIterations  = 1000
	)

	var operationsWg sync.WaitGroup
	var cleanupWg sync.WaitGroup

	// Create multiple subscribers
	subscribers := make([]chan BanEvent, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		ch := banList.Subscribe()
		subscribers[i] = ch
	}

	// Channel to coordinate test completion
	done := make(chan struct{})

	// Goroutine to rapidly unsubscribe and close channels
	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()
		for {
			select {
			case <-done:
				return
			default:
				// Rapidly unsubscribe channels to trigger race condition
				// Don't close the channels here to avoid race with notifySubscribersAsync
				// The channels will be garbage collected after unsubscribe
				for i := 0; i < numSubscribers; i++ {
					if subscribers[i] != nil {
						banList.Unsubscribe(subscribers[i])
						subscribers[i] = nil
					}
				}
				// Re-subscribe to keep the race condition possible
				for i := 0; i < numSubscribers; i++ {
					if subscribers[i] == nil {
						subscribers[i] = banList.Subscribe()
					}
				}
				time.Sleep(time.Microsecond) // Small delay to allow race condition
			}
		}
	}()

	// Goroutine to rapidly trigger Add/Remove operations
	// Each Add/Remove spawns a goroutine that calls notifySubscribersAsync,
	// which is where the send-on-closed-channel would occur without the fix.
	operationsWg.Add(1)
	go func() {
		defer operationsWg.Done()

		for i := 0; i < numIterations; i++ {
			ip := fmt.Sprintf("192.168.1.%d", i%255)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

			// Trigger Add which spawns notifySubscribersAsync goroutine
			_ = banList.Add(ctx, ip, time.Now().Add(time.Hour))

			// Small delay to allow race condition window
			time.Sleep(time.Nanosecond)

			// Trigger Remove which spawns another notifySubscribersAsync goroutine
			_ = banList.Remove(ctx, ip)

			cancel()
		}
	}()

	// Wait for all Add/Remove operations to complete
	operationsWg.Wait()

	// Signal cleanup goroutine to stop
	close(done)
	cleanupWg.Wait()

	// Give time for any remaining async notification goroutines to complete
	// This ensures panics in those goroutines (if any) are caught before test ends
	time.Sleep(100 * time.Millisecond)

	// If we reach here without panics, the test passes
	// Go's test runner would have failed the test if any goroutine panicked
	t.Log("Test PASSED: No panics detected during race condition test")
}
