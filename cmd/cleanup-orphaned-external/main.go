// Package main provides a tool to clean up Aerospike records that reference
// external store files that no longer exist.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

func main() {
	var (
		host        = flag.String("host", "localhost", "Aerospike host")
		port        = flag.Int("port", 3000, "Aerospike port")
		namespace   = flag.String("namespace", "utxo-store", "Aerospike namespace")
		set         = flag.String("set", "utxo", "Aerospike set")
		externalDir = flag.String("external-dir", "./data/external", "External store directory")
		dryRun      = flag.Bool("dry-run", true, "Dry run (don't delete)")
		workers     = flag.Int("workers", 10, "Number of concurrent workers")
	)
	flag.Parse()

	// Connect to Aerospike
	client, err := aero.NewClient(*host, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to Aerospike: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("Connected to Aerospike at %s:%d\n", *host, *port)
	fmt.Printf("Scanning namespace=%s, set=%s\n", *namespace, *set)
	fmt.Printf("External store directory: %s\n", *externalDir)
	fmt.Printf("Dry run: %v\n", *dryRun)

	// Create scan policy
	policy := aero.NewScanPolicy()
	policy.IncludeBinData = true

	// Track statistics
	var scanned, orphaned, deleted, errors int64
	startTime := time.Now()

	// Create work channel
	workCh := make(chan *aero.Key, 1000)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range workCh {
				if !*dryRun {
					_, err := client.Delete(nil, key)
					if err != nil {
						atomic.AddInt64(&errors, 1)
						fmt.Fprintf(os.Stderr, "Error deleting %v: %v\n", key, err)
					} else {
						atomic.AddInt64(&deleted, 1)
					}
				}
			}
		}()
	}

	// Scan records
	recordset, err := client.ScanAll(policy, *namespace, *set)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start scan: %v\n", err)
		os.Exit(1)
	}

	for rec := range recordset.Results() {
		if rec.Err != nil {
			fmt.Fprintf(os.Stderr, "Scan error: %v\n", rec.Err)
			atomic.AddInt64(&errors, 1)
			continue
		}

		atomic.AddInt64(&scanned, 1)

		// Check if this record has external data
		externalBin, hasExternal := rec.Record.Bins["external"]
		if !hasExternal || externalBin == nil {
			continue
		}

		// The external bin should be non-nil/non-zero if data is external
		isExternal := false
		switch v := externalBin.(type) {
		case int, int64:
			isExternal = v != 0
		case bool:
			isExternal = v
		case []byte:
			isExternal = len(v) > 0
		default:
			isExternal = v != nil
		}

		if !isExternal {
			continue
		}

		// Get the transaction ID from the key
		var txIDHex string
		if rec.Record.Key.Value() != nil {
			txID := rec.Record.Key.Value().GetObject()
			switch v := txID.(type) {
			case []byte:
				txIDHex = hex.EncodeToString(v)
			case string:
				txIDHex = v
			default:
				// Try to get from bins
			}
		}

		// If we couldn't get txID from key, try from bins
		if txIDHex == "" {
			if txIDBin, ok := rec.Record.Bins["txID"]; ok {
				switch v := txIDBin.(type) {
				case []byte:
					txIDHex = hex.EncodeToString(v)
				case string:
					txIDHex = v
				}
			}
		}

		if txIDHex == "" {
			continue
		}

		// Check if the external file exists
		txFile := filepath.Join(*externalDir, txIDHex+".tx")
		if _, statErr := os.Stat(txFile); statErr != nil {
			if os.IsNotExist(statErr) {
				atomic.AddInt64(&orphaned, 1)
				fmt.Printf("ORPHANED: %s (no file at %s)\n", txIDHex, txFile)

				if !*dryRun {
					workCh <- rec.Record.Key
				}
			} else {
				atomic.AddInt64(&errors, 1)
				fmt.Fprintf(os.Stderr, "Error checking file %s: %v\n", txFile, statErr)
			}
		}

		// Progress update every 100k records
		if s := atomic.LoadInt64(&scanned); s%100000 == 0 {
			fmt.Printf("Progress: scanned=%d, orphaned=%d, elapsed=%v\n",
				s, atomic.LoadInt64(&orphaned), time.Since(startTime))
		}
	}

	close(workCh)
	// Wait for workers to finish
	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Scanned:  %d records\n", scanned)
	fmt.Printf("Orphaned: %d records\n", orphaned)
	fmt.Printf("Deleted:  %d records\n", deleted)
	fmt.Printf("Errors:   %d\n", errors)
	fmt.Printf("Elapsed:  %v\n", elapsed)

	if *dryRun && orphaned > 0 {
		fmt.Printf("\nThis was a dry run. Run with -dry-run=false to actually delete orphaned records.\n")
	}
}
