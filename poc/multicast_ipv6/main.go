package main

import (
	"encoding/hex"
	"log"
	"net"
	"runtime"
	"time"

	"golang.org/x/net/nettest"
)

var (
	maxDatagramSize = 1024
	addr            *net.UDPAddr
	en0             *net.Interface
)

func init() {
	var err error

	en0, err = net.InterfaceByName("en0")
	if err != nil {
		log.Fatalf("error resolving interface: %v", err)
	}

	// Set the address to the IPv6 loopback address and port 1234.
	addr = &net.UDPAddr{
		IP:   net.ParseIP("ff02::1234"),
		Port: 9999,
		Zone: en0.Name,
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

	go ping()
	listen()
}

func ping() {
	conn, err := net.DialUDP("udp6", nil, addr)
	if err != nil {
		log.Fatalf("error dialing address: %v", err)
	}

	if err := conn.SetReadBuffer(maxDatagramSize); err != nil {
		log.Fatalf("error setting read buffer: %v", err)
	}

	for {
		if _, err := conn.Write([]byte("hello, world\n")); err != nil {
			log.Fatalf("error writing to socket: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func listen() {
	conn, err := net.ListenMulticastUDP("udp6", en0, addr)
	if err != nil {
		log.Fatalf("error starting listener: %v", err)
	}

	// Loop forever reading from the socket
	for {
		buffer := make([]byte, maxDatagramSize)
		numBytes, src, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Fatalf("ReadFromUDP failed: %v", err)
		}

		log.Println(numBytes, "bytes read from", src)
		log.Println(hex.Dump(buffer[:numBytes]))
	}
}
