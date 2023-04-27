//go:build manual_tests

package ipv6

import (
	"log"
	"net"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/ipv6"
	"golang.org/x/net/nettest"
)

func Test2Multicast(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows", "zos":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if !nettest.SupportsIPv6() {
		t.Skip("ipv6 is not supported")
	}

	for i := 0; i < 3; i++ {
		startListener(t, i, "")
	}

}

func startListener(t *testing.T, instance int, address string) {
	ln, err := net.Listen("tcp6", "[::]:1024")
	require.NoError(t, err)

	defer ln.Close()

	c, err := ln.Accept()
	require.NoError(t, err)

	defer c.Close()

	p := ipv6.NewConn(c)

	// if err := p.SetTrafficClass(iana.DiffServAF11); err != nil {
	// 	log.Fatal(err)
	// }

	if err := p.SetHopLimit(128); err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 1024)
	n, err := c.Read(buf)
	require.NoError(t, err)

	require.Equal(t, "HELLO-R-U-THERE", string(buf[:n]))

	if _, err := c.Write([]byte("HELLO-R-U-THERE-ACK")); err != nil {
		log.Fatal(err)
	}

}
