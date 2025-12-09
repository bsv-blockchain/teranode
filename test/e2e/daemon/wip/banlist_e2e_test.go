package smoke

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestBanListGRPCE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(settings *settings.Settings) {
				settings.P2P.PrivateKey = "c8a1b91ae120878d91a04c904e0d565aa44b2575c1bb30a729bd3e36e2a1d5e6067216fa92b1a1a7e30d0aaabe288e25f1efc0830f309152638b61d84be6b71d"
			},
		})
		defer daemonNode.Stop(t)

		// Wait for node to be ready
		time.Sleep(5 * time.Second)

		// Prepare settings for canonical client
		tSettings := daemonNode.Settings
		tSettings.GRPCAdminAPIKey = "testkey"         // Use a known test key, must match server config
		tSettings.P2P.GRPCAddress = "localhost:19904" // Use the correct test port

		clientI, err := p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), tSettings)
		require.NoError(t, err)

		client := clientI.(*p2p.Client)

		ctx := context.Background()
		ip := "192.168.100.1"
		until := time.Now().Add(1 * time.Hour).Unix()

		// Ban an IP
		err = client.BanPeer(ctx, ip, until)
		require.NoError(t, err)

		// Check ban status
		isBanned, err := client.IsBanned(ctx, ip)
		require.NoError(t, err)
		require.True(t, isBanned)

		bannedList, err := client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, ip)

		// Restart node to check persistence
		daemonNode.Stop(t)
		daemonNode.ResetServiceManagerContext(t)
		// Start again with same settings and data dir
		daemonNode = daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC:         true,
			EnableP2P:         true,
			SkipRemoveDataDir: true, // keep data dir for persistence
		})
		defer daemonNode.Stop(t)
		time.Sleep(5 * time.Second)

		tSettings = daemonNode.Settings
		tSettings.GRPCAdminAPIKey = "testkey"
		tSettings.P2P.GRPCAddress = "localhost:19904"
		clientI, err = p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), tSettings)
		require.NoError(t, err)

		client = clientI.(*p2p.Client)

		isBanned, err = client.IsBanned(ctx, ip)
		require.NoError(t, err)
		require.True(t, isBanned)

		// Unban the IP
		err = client.UnbanPeer(ctx, ip)
		require.NoError(t, err)

		isBanned, err = client.IsBanned(ctx, ip)
		require.NoError(t, err)
		require.False(t, isBanned)

		// Ban a subnet and check an IP in the subnet
		subnet := "10.0.0.0/24"
		err = client.BanPeer(ctx, subnet, until)
		require.NoError(t, err)
		isBanned, err = client.IsBanned(ctx, "10.0.0.5")
		require.NoError(t, err)
		require.True(t, isBanned)

		// --- IPv6 Ban Test ---
		ipv6 := "2406:da18:1f7:353a:b079:da22:c7d5:e166"
		until = time.Now().Add(1 * time.Hour).Unix()

		// Ban the IPv6 address
		err = client.BanPeer(ctx, ipv6, until)
		require.NoError(t, err)

		// Check ban status for the exact IPv6 address
		isBanned, err = client.IsBanned(ctx, ipv6)
		require.NoError(t, err)
		require.True(t, isBanned)

		// Check ban status for the IPv6 address with port
		ipv6WithPort := "[" + ipv6 + "]:8333"
		isBanned, err = client.IsBanned(ctx, ipv6WithPort)
		require.NoError(t, err)
		require.True(t, isBanned)

		// Unban the IPv6 address
		err = client.UnbanPeer(ctx, ipv6)
		require.NoError(t, err)

		// Check that the IPv6 address is no longer banned
		isBanned, err = client.IsBanned(ctx, ipv6)
		require.NoError(t, err)
		require.False(t, isBanned)

		// --- IPv6 Subnet Ban Test ---
		ipv6Subnet := "2406:da18:1f7:353a::/64"
		err = client.BanPeer(ctx, ipv6Subnet, until)
		require.NoError(t, err)

		// Check an address within the subnet
		isBanned, err = client.IsBanned(ctx, ipv6)
		require.NoError(t, err)
		require.True(t, isBanned)
	})
}
