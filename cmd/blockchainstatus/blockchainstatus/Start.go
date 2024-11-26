package blockchainstatus

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

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/olekukonko/tablewriter"
	"github.com/ordishs/gocore"
)

var logger ulogger.Logger

const progname = "blockchain-status"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func Init() {
	gocore.SetInfo(progname, version, commit)

	logger = ulogger.TestLogger{}
}

func Start() {
	_ = os.Chdir("../../")

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

	tSettings := settings.NewSettings()

	miners := make(map[string]blockchain.ClientI)

	for _, minerAddress := range minerList {
		client, err := blockchain.NewClientWithAddress(context.Background(), logger, tSettings, minerAddress, "cmd/blockchainstatus")
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
				fmt.Print("\033[H\033[2J")
				fmt.Printf("Current time: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

				for _, minerAddress := range minerList {
					miner := miners[minerAddress]

					blockHeader, meta, err := miner.GetBestBlockHeader(ctx)
					if err != nil {
						table.Append([]string{
							minerAddress,
							"",
							"not available",
							"",
						})
					} else {
						table.Append([]string{
							minerAddress,
							strconv.Itoa(int(meta.Height)),
							blockHeader.Hash().String(),
							blockHeader.HashPrevBlock.String(),
						})
					}
				}

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
