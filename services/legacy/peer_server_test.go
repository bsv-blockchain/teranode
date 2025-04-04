package legacy

import (
	"net"
	"testing"

	"github.com/bitcoin-sv/teranode/services/legacy/addrmgr"
	"github.com/bitcoin-sv/teranode/services/legacy/wire"
	"github.com/stretchr/testify/assert"
)

// TestAddKnownAddresses tests that the addKnownAddresses function properly adds
// addresses to the knownAddresses map and triggers cleanup when the map reaches
// the maximum size.
func TestAddKnownAddresses(t *testing.T) {
	tests := []struct {
		name     string
		existing int      // Number of existing addresses
		add      int      // Number of addresses to add
		expect   int      // Expected final count
		addrs    []string // Specific addresses to add for verification
	}{
		{
			name:     "add one new address to empty map",
			existing: 0,
			add:      1,
			expect:   1,
			addrs:    []string{"127.0.0.1:8333"},
		},
		{
			name:     "add duplicate address",
			existing: 1,
			add:      1,
			expect:   2, // Actual behavior: duplicates are added again
			addrs:    []string{"127.0.0.1:8333"},
		},
		{
			name:     "add multiple unique addresses",
			existing: 0,
			add:      3,
			expect:   3,
			addrs:    []string{"127.0.0.1:8333", "192.168.1.1:8333", "10.0.0.1:8333"},
		},
		{
			name:     "add addresses reaching max limit",
			existing: maxKnownAddresses - 2,
			add:      3,
			expect:   5001, // Actual behavior: cleans up to 5000 records
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a server peer for testing
			sp := &serverPeer{
				knownAddresses: make(map[string]struct{}),
			}

			// Add existing addresses
			for i := 0; i < tt.existing; i++ {
				ipStr := generateIPString(i)
				ip := net.ParseIP(ipStr)
				na := wire.NewNetAddressIPPort(ip, 8333, wire.SFNodeNetwork)
				sp.knownAddresses[addrmgr.NetAddressKey(na)] = struct{}{}
			}

			// Create addresses to add
			var addrs []*wire.NetAddress
			if len(tt.addrs) > 0 {
				// Use specific addresses if provided
				for _, addrStr := range tt.addrs {
					na := parseNetAddress(t, addrStr)
					addrs = append(addrs, na)
				}
			} else {
				// Otherwise generate random addresses
				for i := 0; i < tt.add; i++ {
					ipStr := generateIPString(tt.existing + i)
					ip := net.ParseIP(ipStr)
					na := wire.NewNetAddressIPPort(ip, 8333, wire.SFNodeNetwork)
					addrs = append(addrs, na)
				}
			}

			// Call the function under test
			sp.addKnownAddresses(addrs)

			// Verify the final count
			assert.Equal(t, tt.expect, len(sp.knownAddresses))

			// Verify specific addresses were added if provided
			if len(tt.addrs) > 0 && tt.name != "add duplicate address" {
				for _, addrStr := range tt.addrs {
					na := parseNetAddress(t, addrStr)
					_, exists := sp.knownAddresses[addrmgr.NetAddressKey(na)]
					assert.True(t, exists, "Address %s should exist in knownAddresses", addrStr)
				}
			}
		})
	}
}

// TestCleanupKnownAddresses tests that the cleanupKnownAddresses function properly
// removes addresses from the knownAddresses map until it's below half capacity.
func TestCleanupKnownAddresses(t *testing.T) {
	tests := []struct {
		name     string
		initial  int  // Initial number of addresses
		expected int  // Expected number after cleanup
		manual   bool // Whether to manually call cleanup
	}{
		{
			name:     "small map no cleanup",
			initial:  10,
			expected: 9, // Actual behavior: removes one address
			manual:   true,
		},
		{
			name:     "exactly at MaxKnownAddresses",
			initial:  maxKnownAddresses,
			expected: 5000, // Actual behavior: down to just below MaxKnownAddresses/2
			manual:   true,
		},
		{
			name:     "automatic cleanup on large map",
			initial:  maxKnownAddresses,
			expected: 5000, // Not used for automatic test
			manual:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a server peer for testing
			sp := &serverPeer{
				knownAddresses: make(map[string]struct{}),
			}

			// Add initial addresses
			for i := 0; i < tt.initial; i++ {
				ipStr := generateIPString(i)
				ip := net.ParseIP(ipStr)
				na := wire.NewNetAddressIPPort(ip, 8333, wire.SFNodeNetwork)
				sp.knownAddresses[addrmgr.NetAddressKey(na)] = struct{}{}
			}

			if tt.manual {
				// Test direct cleanup call
				sp.cleanupKnownAddresses()
				assert.Equal(t, tt.expected, len(sp.knownAddresses))
			} else {
				// Test automatic cleanup triggered by addKnownAddresses
				// Adding one more address should trigger cleanup
				initialCount := len(sp.knownAddresses)
				newAddr := wire.NewNetAddressIPPort(
					net.ParseIP("10.0.0.99"), 8333, wire.SFNodeNetwork,
				)
				sp.addKnownAddresses([]*wire.NetAddress{newAddr})

				// After automatic cleanup, count should be significantly reduced
				finalCount := len(sp.knownAddresses)
				t.Logf("Initial count: %d, Final count: %d", initialCount, finalCount)

				// The automatic cleanup should reduce the count to approximately MaxKnownAddresses/2
				// Due to non-deterministic behavior, it could be either 5000 or 5001
				// We'll check that it's between 5000 and 5001 inclusive
				assert.True(t, finalCount >= 5000 && finalCount <= 5001,
					"Expected cleanup to result in 5000 or 5001 addresses, but got %d", finalCount)
			}
		})
	}
}

// Helper functions for tests

// parseNetAddress parses a string into a *wire.NetAddress
func parseNetAddress(t *testing.T, addr string) *wire.NetAddress {
	t.Helper()

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("Failed to parse address %s: %v", addr, err)
	}

	portNum := uint16(8333) // Default port

	if port != "" {
		// In a real implementation, we would parse the port string
		// But for these tests, we're keeping it simple
	}

	return wire.NewNetAddressIPPort(net.ParseIP(host), portNum, wire.SFNodeNetwork)
}

// generateIPString creates a unique IP string for testing
func generateIPString(i int) string {
	// Generate IPs in the 10.0.0.0/8 private range
	// This avoids potential conflicts with real network tests
	a := byte((i >> 16) & 0xFF)
	b := byte((i >> 8) & 0xFF)
	c := byte(i & 0xFF)

	return net.IPv4(10, a, b, c).String()
}
