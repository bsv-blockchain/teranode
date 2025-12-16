// Package peercli provides a command-line interface for interacting with Bitcoin peers.
// It allows users to connect to peers, send various Bitcoin protocol messages, and
// interact with the peer in an interactive mode.
//
// Usage:
//
//	This package is typically used as a CLI tool to establish connections with Bitcoin
//	peers and send protocol messages such as "inv", "getdata", "ping", etc.
//
// Functions:
//   - connect: Establishes a connection to a Bitcoin peer.
//   - sendMessage: Sends a specific Bitcoin protocol message to the connected peer.
//   - interactiveLoop: Starts an interactive CLI loop for user commands.
//   - handleCommand: Processes user input and executes the corresponding command.
//   - reconnect: Attempts to reconnect to a Bitcoin peer.
//
// Side effects:
//
//	Functions in this package may print to stdout and exit the process if an error occurs.
//	They also interact with the Bitcoin network by sending and receiving protocol messages.
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/urfave/cli/v2"
)

const (
	userAgentName    = "peercli" // userAgentName is the name displayed to the Bitcoin peer when connecting.
	userAgentVersion = "1.0.0"   // userAgentVersion is the version displayed to the Bitcoin peer when connecting.
)

var (
	bsvPeerConnected chan bool                // bsvPeerConnected is a channel used to signal when a Bitcoin SV peer is connected.
	conn             net.Conn                 // conn is the network connection to the Bitcoin peer.
	logger           = ulogger.New("peercli") // logger is the logger instance used for logging messages in the peercli application.
	p                *peer.Peer               // p is the peer instance used to interact with the Bitcoin network.
	verack           chan struct{}            // verack is a channel used to wait for the "verack" message from the peer after connection.
)

// main is the entry point for the peercli application.
// It initializes the CLI application, sets up commands, and starts the interactive loop.
// Side effects:
//   - Prints messages to stdout.
//   - Exits the process on fatal errors.
func main() {
	// Verify 64-bit architecture
	if strconv.IntSize != 64 {
		log.Printf("Error: peercli requires a 64-bit architecture. Current architecture: %s\n", runtime.GOARCH)
		os.Exit(1)
	}

	bsvPeerConnected = make(chan bool, 1)

	app := &cli.App{
		Name:  "peer-cli",
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

// peerCfg returns a peer configuration for connecting to a Bitcoin peer.
// Parameters:
//   - None.
//
// Returns:
//   - A pointer to a peer.Config object containing configuration details.
func peerCfg() *peer.Config {
	return &peer.Config{
		UserAgentName:    userAgentName,    // User agent name to advertise.
		UserAgentVersion: userAgentVersion, // User agent version to advertise.
		ChainParams:      &chaincfg.MainNetParams,
		Services:         0,
		TrickleInterval:  time.Second * 10,
		Listeners: peer.MessageListeners{
			OnAddr:       OnAddr,
			OnBlock:      OnBlock,
			OnFeeFilter:  OnFeeFilter,
			OnGetAddr:    OnGetAddr,
			OnGetBlocks:  OnGetBlocks,
			OnGetData:    OnGetData,
			OnGetHeaders: OnGetHeaders,
			OnHeaders:    OnHeaders,
			OnInv:        OnInv,
			OnMemPool:    OnMemPool,
			OnNotFound:   OnNotFound,
			OnPing:       OnPing,
			OnPong:       OnPong,
			OnRead:       OnRead,
			OnReject:     OnReject,
			OnTx:         OnTx,
			OnVerAck:     OnVerAck,
			OnVersion:    OnVersion,
			OnWrite:      OnWrite,
		},
		TstAllowSelfConnection: true,
	}
}

// connect establishes a connection to a Bitcoin peer using the provided address.
// Parameters:
//   - c: A CLI context containing the address of the peer to connect to.
//
// Returns:
//   - An error if the connection fails, otherwise nil.
//
// Side effects:
//   - Prints connection status to stdout.
//   - Sends and receives messages over the network.
func connect(c *cli.Context) error {
	verack = make(chan struct{})

	addr := c.String("address")

	var err error

	p, err = peer.NewOutboundPeer(logger, settings.NewSettings(), peerCfg(), addr)
	if err != nil {
		return err
	}

	fmt.Printf("Trying to dial: %v\n", p.Addr())

	conn, err = net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Printf("net.Dial: error %v\n", err)
		return err
	}

	p.AssociateConnection(conn)
	fmt.Printf("Peer connected: %t\n", p.Connected())

	// Wait for the verack message or timeout in case of failure.
	select {
	case <-verack:
		fmt.Println("Verack received. Connection established.")
	case <-time.After(time.Second * 5):
		p.DisconnectWithInfo("verack timeout")

		return errors.NewError("verack timeout")
	}

	return nil
}

// sendMessage sends a message of the specified type to the connected Bitcoin peer.
// Parameters:
//   - msgType: The type of message to send (e.g., "inv", "getdata", "ping").
//   - args: Additional arguments required for the message.
//
// Returns:
//   - An error if the message fails to send, otherwise nil.
//
// Side effects:
//   - Sends a message to the peer over the network.
//   - Print the message type to stdout.
func sendMessage(msgType string, args ...string) error {
	// 	fmt.Printf("conn.RemoteAddr(): %v\n", conn.RemoteAddr())
	if p == nil || !p.Connected() {
		err := reconnect(conn.RemoteAddr().String())
		if err != nil {
			return errors.NewError("failed to reconnect", err)
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
				return errors.NewError("invalid nonce value: %s", args[0])
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
				return errors.NewError("invalid nonce value: %s", args[0])
			}

			nonce = parsedNonce
		}

		pongMsg := wire.NewMsgPong(nonce)
		p.QueueMessage(pongMsg, nil)
	// Add other cases as needed
	default:
		return errors.NewError("unknown message type: %s", msgType)
	}

	fmt.Printf("Message of type %s sent to Bitcoin peer\n", msgType)

	return nil
}

// interactiveLoop starts an interactive loop for the user to send commands to the Bitcoin peer.
// Parameters:
//   - None.
//
// Side effects:
//   - Reads user input from stdin.
//   - Prints prompts and messages to stdout.
//   - Executes commands based on user input.
func interactiveLoop() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("peer-cli> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		input = strings.TrimSpace(input)
		if input == "exit" {
			fmt.Println("Exiting...")

			if p != nil && p.Connected() {
				p.DisconnectWithInfo("interactiveLoop")
			}

			break
		}

		handleCommand(input)
	}
}

// handleCommand processes user input and executes the corresponding command.
// Parameters:
//   - input: A string containing the user command and arguments.
//
// Side effects:
//   - Prints error messages to stdout if the command fails.
//   - Executes the specified command.
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

// reconnect attempts to reconnect to the Bitcoin peer at the specified address.
// Parameters:
//   - addr: The address of the peer to reconnect to.
//
// Returns:
//   - An error if the reconnection fails, otherwise nil.
//
// Side effects:
//   - Prints reconnection status to stdout.
//   - Sends and receives messages over the network.
func reconnect(addr string) error {
	fmt.Println("Attempting to reconnect...")

	tSettings := settings.NewSettings()

	var err error

	p, err = peer.NewOutboundPeer(logger, tSettings, peerCfg(), addr)
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
