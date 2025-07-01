package p2p

import (
	"context"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestAdvertiseAddressesOptionalPort checks AdvertiseAddresses entries with and without ports
func TestAdvertiseAddressesOptionalPort(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()
	// prepare config with a default port
	defaultPort := 5005
	addrs := []string{"127.0.0.1", "example.com:6001"}
	config := P2PConfig{
		ProcessName:        "test",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: addrs,
		Port:               defaultPort,
		PrivateKey:         "",
		SharedKey:          "",
		UsePrivateDHT:      false,
		OptimiseRetries:    false,
		Advertise:          true,
		StaticPeers:        []string{},
	}
	node, err := NewP2PNode(context.Background(), logger, tSettings, config, nil)
	require.NoError(t, err)

	exposed := make([]string, 0, len(node.host.Addrs()))
	for _, maddr := range node.host.Addrs() {
		exposed = append(exposed, maddr.String())
	}

	require.Len(t, exposed, 2, "expected exactly two advertised multiaddrs")

	// check default port applied for 127.0.0.1
	expected1 := "/ip4/127.0.0.1/tcp/5005"
	found1 := false

	for _, s := range exposed {
		if strings.HasPrefix(s, expected1) {
			found1 = true
			break
		}
	}

	require.True(t, found1, "did not find expected advertise addr %s", expected1)

	// check overridden port applied for example.com:6001
	expected2 := "/dns4/example.com/tcp/6001"
	found2 := false

	for _, s := range exposed {
		if strings.HasPrefix(s, expected2) {
			found2 = true
			break
		}
	}

	require.True(t, found2, "did not find expected advertise addr %s", expected2)
}

// TestAdvertiseAddressesPrivate
func TestAdvertiseAddressesPrivate(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()
	// prepare config with a private DHT
	defaultPort := 5005
	config := P2PConfig{
		ProcessName:        "test",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{},
		Port:               defaultPort,
		PrivateKey:         generateTestPrivateKey(t),
		SharedKey:          generateTestPrivateKey(t), // Fake key; we are just testing the advertising not real connectivity
		UsePrivateDHT:      true,
		OptimiseRetries:    false,
		Advertise:          true,
		StaticPeers:        []string{},
	}
	node, err := NewP2PNode(context.Background(), logger, tSettings, config, nil)
	require.NoError(t, err)

	exposed := make([]string, 0, len(node.host.Addrs()))
	for _, maddr := range node.host.Addrs() {
		exposed = append(exposed, maddr.String())
	}

	require.Len(t, exposed, 1, "expected exactly one advertised multiaddrs")

	// check default port applied for 127.0.0.1
	expected1 := "/ip4/127.0.0.1/tcp/5005"
	require.Equal(t, expected1, exposed[0])
}

// TestAdvertiseAddressesPublic
func TestAdvertiseAddressesPublic(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()
	// prepare config with a private DHT
	defaultPort := 5005
	config := P2PConfig{
		ProcessName:        "test",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{},
		Port:               defaultPort,
		PrivateKey:         "",
		SharedKey:          "",
		UsePrivateDHT:      false,
		OptimiseRetries:    false,
		Advertise:          true,
		StaticPeers:        []string{},
	}
	node, err := NewP2PNode(context.Background(), logger, tSettings, config, nil)
	require.NoError(t, err)

	expected1 := "/ip4/127.0.0.1/tcp/5005"
	found := false
	exposed := make([]string, 0, len(node.host.Addrs()))
	// verify no private IPs, specifically localhost shouldn't exist
	for _, maddr := range node.host.Addrs() {
		exposed = append(exposed, maddr.String())
	}

	for _, s := range exposed {
		if strings.HasPrefix(s, expected1) {
			found = true
			break
		}
	}

	require.False(t, found, "found localhost address in advertised list")
}
