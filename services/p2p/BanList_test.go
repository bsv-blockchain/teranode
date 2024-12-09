package p2p

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/rpc/bsvjson"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/test"
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

	store, err := blockchain.NewStore(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
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
	}
	banList, _, err := setupBanList(t)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := banList.Add(ctx, tt.args.IPOrSubnet, time.Now().Add(time.Duration(*tt.args.BanTime)*time.Second))
			require.NoError(t, err)

			banInfo, exists := banList.bannedPeers[tt.args.IPOrSubnet]
			require.True(t, exists)

			expectedExpiration := time.Now().Add(time.Duration(*tt.args.BanTime) * time.Second)
			require.WithinDuration(t, expectedExpiration, banInfo.ExpirationTime, time.Second)

			if tt.isSubnet {
				require.Equal(t, tt.args.IPOrSubnet, banInfo.Subnet.String())
			} else {
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
		expectedEvents := map[string]BanEvent{
			"add_ip": {
				Action: "add",
				IP:     "192.168.1.1",
				Subnet: &net.IPNet{IP: net.ParseIP("192.168.1.1").To4(), Mask: net.CIDRMask(32, 32)},
			},
			"add_subnet": {
				Action: "add",
				IP:     "10.0.0.0/24",
				Subnet: &net.IPNet{IP: net.ParseIP("10.0.0.0").To4(), Mask: net.CIDRMask(24, 32)},
			},
			"remove_ip": {
				Action: "remove",
				IP:     "192.168.1.1",
				Subnet: &net.IPNet{IP: net.ParseIP("192.168.1.1").To4(), Mask: net.CIDRMask(32, 32)},
			},
		}

		receivedEvents := 0

		for event := range eventChan {
			var expectedEvent BanEvent

			var eventKey string

			switch {
			case event.Action == "add" && event.IP == "192.168.1.1":
				eventKey = "add_ip"
			case event.Action == "add" && event.IP == "10.0.0.0/24":
				eventKey = "add_subnet"
			case event.Action == "remove" && event.IP == "192.168.1.1":
				eventKey = "remove_ip"
			default:
				t.Errorf("Unexpected event received: %+v", event)
				continue
			}

			expectedEvent = expectedEvents[eventKey]
			require.Equal(t, expectedEvent.Action, event.Action)
			require.Equal(t, expectedEvent.IP, event.IP)
			require.Equal(t, expectedEvent.Subnet.String(), event.Subnet.String())

			receivedEvents++
			if receivedEvents == len(expectedEvents) {
				close(done)
				return
			}
		}
	}()

	// Add an IP
	err = banList.Add(ctx, "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Add a subnet
	err = banList.Add(ctx, "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Remove an IP
	err = banList.Remove(ctx, "192.168.1.1")
	require.NoError(t, err)

	// Wait for all events to be processed or timeout
	select {
	case <-done:
		// All expected events were received
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for events")
	}
}
