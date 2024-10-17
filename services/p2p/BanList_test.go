package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/rpc/bsvjson"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func setupBanList(t *testing.T) (*BanList, error) {
	banList, err := NewBanList(ulogger.TestLogger{})
	require.NoError(t, err)

	err = banList.Init(context.Background())
	require.NoError(t, err)

	return banList, err
}

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
	banList, err := setupBanList(t)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := banList.Add(context.Background(), tt.args.IPOrSubnet, time.Now().Add(time.Duration(*tt.args.BanTime)*time.Second))
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
	banList, err := setupBanList(t)
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
	banList, err := setupBanList(t)
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
