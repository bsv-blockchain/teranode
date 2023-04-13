package main

import (
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"

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

	// Get the addr number from the command line
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s <addr>", os.Args[0])
	}

	addrNumber, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("error parsing addr number: %v", err)
	}

	max := 32 // * 1024
	// Check if the addr number is valid
	if addrNumber < 0 || addrNumber > max {
		log.Fatalf("addr number must be between 0 and %d", max)
	}

	// Define the IPv6 multicast address prefix to use
	multicastPrefix := "ff05::5b5a:"
	log.Printf("prefix: %s", multicastPrefix)

	// Define the port number to use for the multicast groups
	port := 5000
	log.Printf("port: %d", port)

	ip := net.ParseIP(multicastPrefix + strconv.FormatInt(int64(addrNumber), 16))
	log.Printf("ip: %s", ip)

	addr := &net.UDPAddr{
		IP:   ip,
		Port: port,
		Zone: netInterface.Name,
	}

	log.Printf("Address is %v", addr)

	ping(addr)

}

func ping(addr *net.UDPAddr) {
	log.Printf("Dialing address %v", addr)

	conn, err := net.DialUDP("udp6", nil, addr)
	if err != nil {
		log.Fatalf("error dialing address: %v", err)
	}

	if err := conn.SetReadBuffer(maxDatagramSize); err != nil {
		log.Fatalf("error setting read buffer: %v", err)
	}

	for {
		if _, err := conn.Write([]byte("Amazing world\n")); err != nil {
			log.Fatalf("error writing to socket: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}
