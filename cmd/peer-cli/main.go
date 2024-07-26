package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/urfave/cli/v2"
)

var p *peer.Peer
var conn net.Conn
var bsvPeerConnected chan bool
var verack chan struct{}

func main() {
	bsvPeerConnected = make(chan bool, 1)

	app := &cli.App{
		Name:  "bitcoin-cli",
		Usage: "A CLI tool to interact with Bitcoin peers",
		Commands: []*cli.Command{
			{
				Name:   "connect",
				Usage:  "Connect to a Bitcoin peer",
				Action: connect,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Usage:    "Address of the Bitcoin peer",
						Required: true,
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	// If connection is successful, enter the interactive loop
	interactiveLoop()
}

func peerCfg() *peer.Config {
	return &peer.Config{
		UserAgentName:    "peer",  // User agent name to advertise.
		UserAgentVersion: "1.0.0", // User agent version to advertise.
		ChainParams:      &chaincfg.MainNetParams,
		Services:         0,
		TrickleInterval:  time.Second * 10,
		Listeners: peer.MessageListeners{
			OnVersion:    OnVersion,
			OnMemPool:    OnMemPool,
			OnTx:         OnTx,
			OnBlock:      OnBlock,
			OnInv:        OnInv,
			OnHeaders:    OnHeaders,
			OnGetData:    OnGetData,
			OnGetBlocks:  OnGetBlocks,
			OnGetHeaders: OnGetHeaders,
			OnFeeFilter:  OnFeeFilter,
			OnGetAddr:    OnGetAddr,
			OnAddr:       OnAddr,
			OnRead:       OnRead,
			OnWrite:      OnWrite,
			OnReject:     OnReject,
			OnNotFound:   OnNotFound,
			OnVerAck:     OnVerAck,
			OnPing:       OnPing,
			OnPong:       OnPong,
		},
		TstAllowSelfConnection: true,
	}
}

func connect(c *cli.Context) error {
	verack = make(chan struct{})

	addr := c.String("address")
	var err error
	p, err = peer.NewOutboundPeer(peerCfg(), addr)
	if err != nil {
		return err
	}

	fmt.Printf("trying to dial: %v\n", p.Addr())
	conn, err = net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Printf("net.Dial: error %v\n", err)
		return err
	}
	p.AssociateConnection(conn)
	fmt.Printf("peer connected: %t\n", p.Connected())

	// Wait for the verack message or timeout in case of failure.
	select {
	case <-verack:
		fmt.Println("Verack received. Connection established.")
	case <-time.After(time.Second * 5):
		fmt.Println("connection: verack timeout")
		p.Disconnect()
		return fmt.Errorf("verack timeout")
	}

	return nil
}

func sendMessage(msgType string, args ...string) error {
	// 	fmt.Printf("conn.RemoteAddr(): %v\n", conn.RemoteAddr())
	if p == nil || !p.Connected() {
		err := reconnect(conn.RemoteAddr().String())
		if err != nil {
			return fmt.Errorf("failed to reconnect: %v", err)
		}
	}

	switch msgType {
	case "inv":
		invMsg := wire.NewMsgInv()
		// Customize invMsg as needed based on args
		if len(args) > 0 {
			for _, arg := range args {
				// Add inventory vectors based on args
				hash, err := chainhash.NewHashFromStr(arg)
				if err != nil {
					return err
				}

				invVect := wire.NewInvVect(wire.InvTypeTx, hash)
				err = invMsg.AddInvVect(invVect)
				if err != nil {
					return err
				}
			}
		}
		p.QueueMessage(invMsg, nil)
	case "getdata":
		getDataMsg := wire.NewMsgGetData()
		// Customize getDataMsg as needed based on args
		if len(args) > 0 {
			for _, arg := range args {
				// Add inventory vectors based on args
				hash, err := chainhash.NewHashFromStr(arg)
				if err != nil {
					return err
				}

				invVect := wire.NewInvVect(wire.InvTypeTx, hash)
				err = getDataMsg.AddInvVect(invVect)
				if err != nil {
					return err
				}
			}
		}
		p.QueueMessage(getDataMsg, nil)
	case "getaddr":
		getAddrMsg := wire.NewMsgGetAddr()

		p.QueueMessage(getAddrMsg, nil)
	case "ping":
		nonce := uint64(0)
		if len(args) > 0 {
			parsedNonce, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid nonce value: %s", args[0])
			}
			nonce = parsedNonce
		}
		pingMsg := wire.NewMsgPing(nonce)
		p.QueueMessage(pingMsg, nil)
	case "pong":
		nonce := uint64(0)
		if len(args) > 0 {
			parsedNonce, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid nonce value: %s", args[0])
			}
			nonce = parsedNonce
		}
		pongMsg := wire.NewMsgPong(nonce)
		p.QueueMessage(pongMsg, nil)
	// Add other cases as needed
	default:
		return fmt.Errorf("unknown message type: %s", msgType)
	}

	fmt.Printf("Message of type %s sent to Bitcoin peer\n", msgType)
	return nil
}

func interactiveLoop() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("bitcoin-cli> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		input = strings.TrimSpace(input)
		if input == "exit" {
			fmt.Println("Exiting...")
			if p != nil && p.Connected() {
				p.Disconnect()
			}
			break
		}

		handleCommand(input)
	}
}

func handleCommand(input string) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "send":
		if len(args) < 1 {
			fmt.Println("Usage: send <msgType> [args...]")
			return
		}
		err := sendMessage(args[0], args[1:]...)
		if err != nil {
			fmt.Println("Error sending message:", err)
		}
	default:
		fmt.Println("Unknown command:", cmd)
	}
}

func reconnect(addr string) error {
	fmt.Println("Attempting to reconnect...")
	var err error
	p, err = peer.NewOutboundPeer(peerCfg(), addr)
	if err != nil {
		return err
	}

	conn, err = net.Dial("tcp", p.Addr())
	if err != nil {
		return err
	}
	p.AssociateConnection(conn)
	fmt.Println("Reconnected to peer")
	return nil
}
