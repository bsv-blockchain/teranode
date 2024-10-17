package p2p

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/rpc/bsvjson"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func TestHandleSetBanAdd(t *testing.T) {
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
				BanTime:    3600, // 1 hour
				Absolute:   false,
			},
			isSubnet: false,
		},
		{
			name: "test subnet add ban",
			args: &bsvjson.SetBanCmd{
				Command:    "add",
				IPOrSubnet: "127.0.0.0/24",
				BanTime:    7200, // 2 hours
				Absolute:   false,
			},
			isSubnet: true,
		},
	}

	banList := NewBanList(ulogger.TestLogger{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := banList.Add(tt.args.IPOrSubnet, time.Now().Add(time.Duration(tt.args.BanTime)*time.Second))
			require.NoError(t, err)

			banInfo, exists := banList.bannedPeers[tt.args.IPOrSubnet]
			require.True(t, exists)

			expectedExpiration := time.Now().Add(time.Duration(tt.args.BanTime) * time.Second)
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
	banList := NewBanList(ulogger.TestLogger{})

	// Add a banned IP
	err := banList.Add("192.168.1.1", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	// Add a banned subnet
	err = banList.Add("10.0.0.0/24", time.Now().Add(3600*time.Second))
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
	banList := NewBanList(ulogger.TestLogger{})

	// Add a banned IP
	err := banList.Add("192.168.1.1", time.Now().Add(3600*time.Second))
	require.NoError(t, err)

	// Add a banned subnet
	err = banList.Add("10.0.0.0/24", time.Now().Add(3600*time.Second))
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
			err := banList.Remove(tt.ipOrSubnet)
			require.NoError(t, err)

			_, exists := banList.bannedPeers[tt.ipOrSubnet]
			require.False(t, exists)
		})
	}
}
