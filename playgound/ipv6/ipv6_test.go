package ipv6

import (
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/ipv6"
)

func TestHello(t *testing.T) {
	go func() {
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

	}()

	// Now send "hello" to the server
	conn, err := net.Dial("tcp6", "[::]:1024")
	require.NoError(t, err)

	defer conn.Close()

	p := ipv6.NewConn(conn)

	// if err := p.SetTrafficClass(iana.DiffServAF11); err != nil {
	// 	log.Fatal(err)
	// }

	if err := p.SetHopLimit(128); err != nil {
		log.Fatal(err)
	}

	if _, err := conn.Write([]byte("HELLO-R-U-THERE")); err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	require.NoError(t, err)

	require.Equal(t, "HELLO-R-U-THERE-ACK", string(buf[:n]))

}
