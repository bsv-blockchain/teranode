package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"

	"golang.org/x/net/ipv6"
	"golang.org/x/net/nettest"
)

var (
	maxDatagramSize = 1024
	netInterface    *net.Interface
)

func init() {
	var err error

	// Guess the interface name
	interfaceName := "eth0"
	if runtime.GOOS == "darwin" {
		interfaceName = "en0"
	}

	netInterface, err = net.InterfaceByName(interfaceName)
	if err != nil {
		log.Fatalf("error resolving interface: %v", err)
	}
}

func main() {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		log.Fatalf("not supported on %s", runtime.GOOS)
	}
	if !nettest.SupportsIPv6() {
		log.Fatal("ipv6 is not supported")
	}

	// Get 1st command line argument
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s server number (1 or 2)", os.Args[0])
	}

	// Get the server number
	serverNumber, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("error parsing server number: %v", err)
	}

	// Check if the server number is valid
	if serverNumber < 1 || serverNumber > 2 {
		log.Fatalf("server number must be 1 or 2")
	}

	// Define the IPv6 multicast address prefix to use
	multicastPrefix := "ff05::5b5a:"

	// Define the port number to use for the multicast groups
	port := 5000

	max := 32 // * 1024
	var from int
	var to int

	switch serverNumber {
	case 1:
		from = 0
		to = max / 2
	case 2:
		from = max / 2
		to = max
	}

	log.Printf("Starting listening to groups %d to %d", from, to-1)

	for i := from; i < to; i++ {
		// Generate the multicast group address by appending the hex value of the group ID to the prefix
		groupAddress := net.ParseIP(multicastPrefix + strconv.FormatInt(int64(i), 16))

		// Create a UDP listener on the group address and port
		conn, err := net.ListenPacket("udp6", fmt.Sprintf("[%s]:%d", groupAddress.String(), port))
		if err != nil {
			log.Printf("Error creating listener for group %s: %s\n", groupAddress.String(), err.Error())
			continue
		}

		// Join the multicast group on the listener interface
		p := ipv6.NewPacketConn(conn)
		if err := p.JoinGroup(netInterface, &net.IPAddr{IP: groupAddress}); err != nil {
			log.Printf("Error joining group %s: %s\n", groupAddress.String(), err.Error())
			continue
		}

		// Start a separate goroutine to handle incoming packets for the multicast group
		go handlePackets(p, groupAddress)

		//  fmt.Printf("Listening for group %s\n", groupAddress.String())
	}

	log.Printf("Listening for groups %d to %d complete", from, to-1)

	// Sleep indefinitely to keep the program running
	select {}
}

func handlePackets(p *ipv6.PacketConn, groupAddress net.IP) {
	buf := make([]byte, 1024)
	for {
		// Read a packet from the multicast group
		n, _, src, err := p.ReadFrom(buf)
		if err != nil {
			log.Printf("Error reading packet from group %s: %s\n", groupAddress.String(), err.Error())
			continue
		}

		// Print the contents of the packet
		log.Printf("Received packet from group %s (%s): %s\n", groupAddress.String(), src.String(), buf[:n])
	}
}
