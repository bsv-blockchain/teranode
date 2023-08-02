package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/olekukonko/tablewriter"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

var logger utils.Logger

const progname = "blockchain-status"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func init() {
	gocore.SetInfo(progname, version, commit)
	logger = gocore.Log("work", gocore.NewLogLevelFromString("debug"))
}

func main() {
	_ = os.Chdir("../../")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	clients := flag.String("miners", "", "blockchain miners to watch")
	refresh := flag.Int("refresh", 5, "refresh rate in seconds")
	flag.Parse()

	if *clients == "" {
		logger.Fatalf("no miners specified")
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	minerList := strings.Split(*clients, ",")

	miners := make(map[string]blockchain.ClientI)
	for _, minerAddress := range minerList {
		client, err := blockchain.NewClientWithAddress(minerAddress)
		if err != nil {
			logger.Fatalf("error connecting to minerAddress %s: %s", minerAddress, err)
		}
		miners[minerAddress] = client
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				table := tablewriter.NewWriter(os.Stdout)
				table.SetBorder(false)
				table.SetHeader([]string{"Miner", "Height", "Last block hash", "Previous block hash"})
				for _, minerAddress := range minerList {
					miner := miners[minerAddress]
					blockHeader, height, err := miner.GetBestBlockHeader(ctx)
					if err != nil {
						logger.Errorf("error getting best block header from miner %s: %s", minerAddress, err)
						continue
					}
					table.Append([]string{
						minerAddress,
						strconv.Itoa(int(height)),
						blockHeader.Hash().String(),
						blockHeader.HashPrevBlock.String(),
					})
				}
				fmt.Print("\033[H\033[2J")
				fmt.Printf("Current time: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
				table.Render()
				time.Sleep(time.Duration(*refresh) * time.Second)
			}
		}
	}()

	select {
	case <-interrupt:
		cancel()
		break
	case <-ctx.Done():
		logger.Errorf("context cancelled: %v", ctx.Err())
		cancel()
		break
	}
}
