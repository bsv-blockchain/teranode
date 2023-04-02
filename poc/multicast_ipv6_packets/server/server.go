package main

import (
	"fmt"
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
	conn, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		log.Printf("Error creating packet connection: %s\n", err.Error())
		return
	}

	defer conn.Close()

	// Join the multicast group on the connection
	p := ipv6.NewPacketConn(conn)

	defer p.Close()

	if err := p.JoinGroup(en0, &net.UDPAddr{IP: multicastAddr, Port: port}); err != nil {
		log.Printf("Error joining multicast group: %s\n", err.Error())
		return
	}

	// Set control message on the connection to receive source address
	if err := p.SetControlMessage(ipv6.FlagSrc, true); err != nil {
		log.Printf("Error setting control message: %s\n", err.Error())
		return
	}

	// Loop to receive multicast messages
	buf := make([]byte, 1024)
	for {
		n, cm, _, err := p.ReadFrom(buf)
		if err != nil {
			log.Printf("Error reading packet: %s\n", err.Error())
			continue
		}

		// Extract the source address from the control message
		srcAddr := cm.Src
		fmt.Printf("Received %d bytes from %s: %s\n", n, srcAddr.String(), string(buf[:n]))
	}
}

/*
package main

import (
	"fmt"
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

	group := net.ParseIP("ff02::1")

	// Create UDP listeners for both multicast addresses
	c, err := net.ListenPacket("udp6", "[::]:1024")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}

	defer c.Close()

	p := ipv6.NewPacketConn(c)

	if err := p.JoinGroup(en0, &net.UDPAddr{IP: group}); err != nil {
		log.Fatal(err)
	}

	// Create buffers to hold incoming data for both listeners
	b := make([]byte, 1024)

	// Loop forever, reading data from both multicast groups
	go func() {
		for {
			n, rcm, _, err := p.ReadFrom(b)
			if err != nil {
				log.Fatal(err)
			}
			if rcm.Dst.IsMulticast() {
				if rcm.Dst.Equal(group) {
					fmt.Println("Received from group 1:", string(b[0:n]))
				} else {
					fmt.Printf("Wrong group")
				}
			} else {
				fmt.Printf(".")
			}

		}
	}()

	ch := make(chan bool)
	<-ch
}
*/
