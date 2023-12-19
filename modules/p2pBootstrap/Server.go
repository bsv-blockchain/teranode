package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bitcoin-sv/ubsv/ulogger"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/ordishs/gocore"
)

const (
	bootstrapPrivKeyFilename = "bootstrap_private_key_"
)

var logger ulogger.Logger

func main() {

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	logger.Infof("Starting P2P Bootstrap")

	dhtProtocolIdStr, ok := gocore.Config().Get("p2p_dht_protocol_id")
	if !ok {
		panic(fmt.Errorf("error getting p2p_dht_protocol_id"))
	}
	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(fmt.Errorf("error getting p2p_shared_key"))
	}
	privkeyHex, ok := gocore.Config().Get("p2p_bootstrap_privkey")
	if !ok {
		panic(fmt.Errorf("error getting p2p_bootstrap_privkey"))
	}

	listenAddr, ok := gocore.Config().Get("p2p_bootstrap_listenAddress")
	if !ok {
		panic(fmt.Errorf("error getting p2p_bootstrap_listenAddress"))
	}

	listenPort, ok := gocore.Config().GetInt("p2p_bootstrap_listenPort")
	if !ok {
		panic(fmt.Errorf("failed to get p2p_bootstrap_listenPort"))
	}
	dhtProtocolID := protocol.ID(dhtProtocolIdStr)

	pkBytes, err := hex.DecodeString(privkeyHex)
	if err != nil {
		panic(err)
	}
	pk, err := crypto.UnmarshalPrivateKey(pkBytes)
	if err != nil {
		panic(err)
	}

	s := ""
	s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
	s += fmt.Sprintln("/base16/")
	s += sharedKey

	psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
	if err != nil {
		panic(err)
	}
	host, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", listenAddr, listenPort)),
		libp2p.Identity(pk),
		libp2p.PrivateNetwork(psk),
	)
	if err != nil {
		panic(err)
	}

	var options []dht.Option
	mode := dht.ModeServer

	options = append(options, dht.Mode(mode))
	options = append(options, dht.ProtocolPrefix(dhtProtocolID))
	// options = append(options, dht.DisableProviders())
	// options = append(options, dht.DisableValues())

	_, err = dht.New(context.Background(), host, options...)
	if err != nil {
		panic(err)
	}

	logger.Infof("Bootstrap node is running on:")
	for _, addr := range host.Addrs() {
		logger.Infof("*  %s/p2p/%s\n", addr, host.ID().Pretty())
	}

	select {}
}
