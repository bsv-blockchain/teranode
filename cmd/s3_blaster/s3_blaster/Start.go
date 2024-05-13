package s3_blaster

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	version     string
	commit      string
	counter     atomic.Int64
	workerCount int
	usePrefix   bool
	txStoreUrl  *url.URL
)
var separator = []byte("/")

func Init() {

	httpAddr, ok := gocore.Config().Get("profilerAddr")
	if !ok {
		log.Printf("Profiler address not set, defaulting to localhost:6060")
		httpAddr = "localhost:6060"
	}

	// start dummy health check...
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      nil,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Profiler available at http://%s/debug/pprof", httpAddr)
	go func() {
		log.Printf("%v", server.ListenAndServe())
	}()

}

func Start() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.BoolVar(&usePrefix, "usePrefix", false, "Use a prefix for the S3 key")
	flag.Parse()

	logger := ulogger.New("s3_blaster")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	url, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStoreUrl = url

	go func() {
		start := time.Now()

		for range time.NewTicker(5 * time.Second).C {
			elapsed := time.Since(start)
			log.Printf("TPS: %s\n", FormatFloat(float64(counter.Swap(0))/float64(elapsed.Milliseconds())*1000))

			start = time.Now()
		}
	}()

	log.Printf("Using %s as the tx store", txStoreUrl)
	log.Print("Prefixing: ", usePrefix)
	log.Printf("Starting %d broadcasting worker(s)", workerCount)

	for i := 0; i < workerCount; i++ {
		go worker(logger)
	}

	<-make(chan struct{})
}

func worker(logger ulogger.Logger) {
	txStore, err := blob.NewStore(logger, txStoreUrl)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	payload := []byte("value")

	for {
		txid := generateRandomBytes()

		if usePrefix {
			prefix := []byte(calculatePrefix(txid))
			txid = append(prefix, append(separator, txid...)...)
		}

		if err := txStore.Set(ctx, txid, payload); err != nil {
			logger.Errorf("Failed to broadcast tx: %v", err)
		}

		counter.Add(1)
	}
}

func generateRandomBytes() []byte {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
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

const (
	numPrefixes = 65535 // The number of prefixes to use
)

func calculatePrefix(s3Key []byte) []byte {
	// Calculate the prefix index based on the first two bytes of the key
	prefixIndex := (int(s3Key[0]) << 8) + int(s3Key[1])
	prefixIndex = prefixIndex % numPrefixes

	// Convert the prefix index to a byte slice
	prefixBytes := []byte(fmt.Sprintf("prefix%d", prefixIndex))

	// Combine the prefix and the key with a "/"
	return append(prefixBytes, append(separator, s3Key...)...)
}
