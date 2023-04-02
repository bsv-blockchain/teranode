package main

import (
	"encoding/hex"
	"log"
	"net"
	"time"
)

var (
	maxDatagramSize = 1024
	addr            *net.UDPAddr
	en0             *net.Interface
)

func init() {
	var err error

	addr, err = net.ResolveUDPAddr("udp4", "239.0.0.0:9999")
	if err != nil {
		log.Fatalf("error resolving address: %v", err)
	}

	en0, err = net.InterfaceByName("en0")
	if err != nil {
		log.Fatalf("error resolving interface: %v", err)
	}
}

func main() {
	go ping()
	listen()
}

func ping() {
	conn, err := net.DialUDP("udp4", nil, addr)
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
	conn, err := net.ListenMulticastUDP("udp4", en0, addr)
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
