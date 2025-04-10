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

	"github.com/bitcoin-sv/teranode/services/rpc/bsvjson"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
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

	tSettings := test.CreateBaseTestSettings()

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

	tSettings := test.CreateBaseTestSettings()
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
