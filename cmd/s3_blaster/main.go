package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"crypto/rand"
	"net/http"
	_ "net/http/pprof"
	"net/url"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/gcs"
	"github.com/bitcoin-sv/ubsv/stores/blob/kinesiss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/minio"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfs"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/sql"
	"github.com/ordishs/go-utils"
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

func init() {

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

	log.Printf("Profiler available at http://%s/debug/pprof", httpAddr)
	go func() {
		log.Printf("%v", http.ListenAndServe(httpAddr, nil))
	}()

}

func main() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.BoolVar(&usePrefix, "usePrefix", false, "Use a prefix for the S3 key (in theory improves performance)")
	flag.Parse()

	logger := gocore.Log("s3_blaster")

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

func worker(logger utils.Logger) {
	txStore, err := NewStore(txStoreUrl)
	if err != nil {
		panic(err)
	}

	payload := []byte("value")

	for {
		// Create a dummy txid
		txid := generateRandomBytes()
		ctx := context.Background()

		if usePrefix {
			prefix := []byte(calculatePrefix(txid))
			txid = append(prefix, append(separator, txid...)...)
		}

		if err := txStore.Set(ctx, txid, payload); err != nil {
			logger.Fatalf("Failed to broadcast tx: %v", err)
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

func NewStore(storeUrl *url.URL) (store blob.Store, err error) {
	switch storeUrl.Scheme {
	case "null":
		store, err = null.New()
		if err != nil {
			return nil, fmt.Errorf("error creating null blob store: %v", err)
		}
	case "memory":
		store = memory.New()
	case "file":
		store, err = file.New("." + storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating file blob store: %v", err)
		}
	case "postgres", "sqlite", "sqlitememory":
		store, err = sql.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating sql blob store: %v", err)
		}
	case "gcs":
		store, err = gcs.New(strings.Replace(storeUrl.Path, "/", "", 1))
		if err != nil {
			return nil, fmt.Errorf("error creating gcs blob store: %v", err)
		}
	case "minio":
		fallthrough
	case "minios":
		store, err = minio.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating minio blob store: %v", err)
		}
	case "s3":
		store, err = s3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating s3 blob store: %v", err)
		}
	case "kinesiss3":
		store, err = kinesiss3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating kinesiss3 blob store: %v", err)
		}
	case "seaweedfs":
		store, err = seaweedfs.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfs blob store: %v", err)
		}
	case "seaweedfss3":
		store, err = seaweedfss3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfss3 blob store: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	return
}
