package main

import (
	"log"
	"net"
	"runtime"

	"golang.org/x/net/ipv6"
	"golang.org/x/net/nettest"
)

func main() {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		log.Fatalf("not supported on %s", runtime.GOOS)
	}
	if !nettest.SupportsIPv6() {
		log.Fatal("ipv6 is not supported")
	}

	en0, err := net.InterfaceByName("en0")
	if err != nil {
		log.Fatal(err)
	}

	// Define the multicast address and port
	multicastAddr := net.ParseIP("ff12::1234")
	port := 3000

	// Create a new IPv6 packet connection
	conn, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		log.Printf("Error creating packet connection: %s\n", err.Error())
		return
	}

	// Join the multicast group on the connection
	p := ipv6.NewPacketConn(conn)

	defer p.Close()

	if err := p.JoinGroup(en0, &net.UDPAddr{IP: multicastAddr, Port: port}); err != nil {
		log.Printf("Error joining multicast group: %s\n", err.Error())
		return
	}

	// Send a multicast message
	msg := []byte("Hello, multicast world!")

	if _, err := p.WriteTo(msg, nil, &net.UDPAddr{IP: multicastAddr, Port: port}); err != nil {
		log.Printf("Error sending multicast message: %s\n", err.Error())
		return
	}

	// Close the connection
	if err := p.Close(); err != nil {
		log.Printf("Error closing connection: %s\n", err.Error())
		return
	}
}
