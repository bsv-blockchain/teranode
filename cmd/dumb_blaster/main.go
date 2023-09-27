//go:build native

package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/native"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

var (
	incompleteTx, _ = bt.NewTxFromString("010000000000000000ef0193a35408b6068499e0d5abd799d3e827d9bfe70c9b75ebe209c91d25072326510000000000ffffffff00e1f505000000001976a914c0a3c167a28cabb9fbb495affa0761e6e74ac60d88ac02404b4c00000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888acde94a905000000001976a91404d03f746652cfcb6cb55119ab473a045137d26588ac00000000")
	w               *wif.WIF
	counter         atomic.Int64
	workerCount     int
	broadcast       bool
	client          propagation_api.PropagationAPIClient
)

func init() {
	unlocker.InjectExternalSignerFn(native.SignMessage)

	propagationServerAddr, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	conn, err := util.GetGRPCClient(context.Background(), propagationServerAddr[0], &util.ConnectionOptions{
		Prometheus: gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}
	client = propagation_api.NewPropagationAPIClient(conn)
}

func main() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.BoolVar(&broadcast, "broadcast", false, "Broadcast to propagation server")
	flag.Parse()

	var err error

	w, err = wif.DecodeWIF("cNGwGSc7KRrTmdLUZ54fiSXWbhLNDc2Eg5zNucgQxyQCzuQ5YRDq")
	if err != nil {
		panic(err)
	}

	go func() {
		start := time.Now()

		for range time.NewTicker(5 * time.Second).C {
			elapsed := time.Since(start)
			fmt.Printf("TPS: %s\n", FormatFloat(float64(counter.Swap(0))/float64(elapsed.Milliseconds())*1000))

			start = time.Now()
		}
	}()

	for i := 0; i < workerCount; i++ {
		go worker()
	}

	<-make(chan struct{})
}

func worker() {
	for {
		// Create a new transaction
		tx := incompleteTx.Clone()

		ul := unlocker.Getter{PrivateKey: w.PrivKey}
		if err := tx.FillAllInputs(context.Background(), &ul); err != nil {
			panic(err)
		}

		if broadcast {
			if err := sendToPropagationServer(context.Background(), tx.ExtendedBytes()); err != nil {
				panic(err)
			}
		}

		counter.Add(1)
	}
}

func sendToPropagationServer(ctx context.Context, txExtendedBytes []byte) error {
	_, err := client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: txExtendedBytes,
	})

	return err
}

func FormatFloat(f float64) string {
	intPart := int(f)
	decimalPart := int((f - float64(intPart)) * 100)

	var sb strings.Builder
	count := 0
	for intPart > 0 {
		if count > 0 && count%3 == 0 {
			sb.WriteString(",")
		}
		digit := intPart % 10
		sb.WriteString(fmt.Sprintf("%d", digit))
		intPart /= 10
		count++
	}

	reversedIntPart := []rune(sb.String())
	for i, j := 0, len(reversedIntPart)-1; i < j; i, j = i+1, j-1 {
		reversedIntPart[i], reversedIntPart[j] = reversedIntPart[j], reversedIntPart[i]
	}

	return fmt.Sprintf("%s.%02d", string(reversedIntPart), decimalPart)
}
