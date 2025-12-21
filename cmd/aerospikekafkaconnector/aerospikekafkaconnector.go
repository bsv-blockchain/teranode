// Package aerospikekafkaconnector provides a command to read Aerospike CDC data from Kafka
// and filter/log operations on specific transaction IDs.
package aerospikekafkaconnector

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

// AerospikeKafkaMessage represents a message from the Aerospike Kafka connector in flat-JSON format.
type AerospikeKafkaMessage struct {
	Metadata struct {
		Msg       string `json:"msg"`       // "write" or "delete"
		Namespace string `json:"namespace"` // Aerospike namespace
		Set       string `json:"set"`       // Aerospike set
		Gen       int    `json:"gen"`       // Generation number
		Lut       int64  `json:"lut"`       // Last update time (milliseconds since epoch)
		Digest    string `json:"digest"`    // Record digest
		Exp       int    `json:"exp,omitempty"`
		Durable   bool   `json:"durable,omitempty"` // Delete durability flag
	} `json:"metadata"`

	// Bins are unmarshaled separately from the top-level JSON
	RawBins map[string]interface{}
}

// Stats tracks statistics for the reader
type Stats struct {
	totalMessages   atomic.Uint64
	matchedMessages atomic.Uint64
	writeOps        atomic.Uint64
	deleteOps       atomic.Uint64
	lastLogTime     time.Time
}

// ReadAerospikeKafka reads Aerospike CDC data from Kafka and filters by transaction ID.
// It consumes messages from the beginning and logs all operations that match the filters.
func ReadAerospikeKafka(
	logger ulogger.Logger,
	tSettings *settings.Settings,
	kafkaURLStr string,
	txIDFilter string,
	namespaceFilter string,
	setFilter string,
	statsIntervalSecs int,
) error {
	logger.Infof("Starting Aerospike Kafka connector reader")
	logger.Infof("  Kafka URL: %s", kafkaURLStr)
	if txIDFilter != "" {
		logger.Infof("  TxID Filter: %s", txIDFilter)
	} else {
		logger.Infof("  TxID Filter: (none - showing all)")
	}
	if namespaceFilter != "" {
		logger.Infof("  Namespace Filter: %s", namespaceFilter)
	}
	if setFilter != "" {
		logger.Infof("  Set Filter: %s", setFilter)
	}

	// Parse Kafka URL
	kafkaURL, err := url.Parse(kafkaURLStr)
	if err != nil {
		return errors.NewProcessingError("failed to parse Kafka URL", err)
	}

	// Convert hex filter to bytes for comparison
	var filterBytes []byte
	if txIDFilter != "" {
		filterBytes, err = hex.DecodeString(txIDFilter)
		if err != nil || len(filterBytes) != 32 {
			return errors.NewProcessingError("Invalid txID filter: must be 64 hex characters representing 32 bytes")
		}
	}

	stats := &Stats{lastLogTime: time.Now()}

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Infof("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// Start stats logger
	go logStats(ctx, logger, stats, time.Duration(statsIntervalSecs)*time.Second)

	// Consumer function
	consumerFn := func(msg *kafka.KafkaMessage) error {
		stats.totalMessages.Add(1)

		// Parse message
		var akMsg AerospikeKafkaMessage
		if err := json.Unmarshal(msg.Value, &akMsg); err != nil {
			logger.Errorf("Failed to parse Kafka message: %v", err)
			return nil // Skip invalid messages
		}

		// Unmarshal bins separately (everything except metadata)
		var rawMsg map[string]interface{}
		if err := json.Unmarshal(msg.Value, &rawMsg); err != nil {
			return nil
		}
		delete(rawMsg, "metadata")
		akMsg.RawBins = rawMsg

		// Filter by namespace
		if namespaceFilter != "" && akMsg.Metadata.Namespace != namespaceFilter {
			return nil
		}

		// Filter by set
		if setFilter != "" && akMsg.Metadata.Set != setFilter {
			return nil
		}

		// Extract txID from bins (only for write operations)
		var txIDHex string
		if akMsg.Metadata.Msg == "write" {
			txIDBytes, err := extractTxIDFromBins(akMsg.RawBins)
			if err != nil {
				// No txID bin or invalid format, skip
				return nil
			}
			txIDHex = hex.EncodeToString(txIDBytes)

			// Filter by txID
			if txIDFilter != "" && txIDHex != txIDFilter {
				return nil
			}

			stats.writeOps.Add(1)
		} else {
			// Delete operation - no bins, can't filter by txID
			if txIDFilter != "" {
				// Skip deletes when filtering by txID (they have no bins)
				return nil
			}
			stats.deleteOps.Add(1)
		}

		stats.matchedMessages.Add(1)

		// Log the operation
		logOperation(logger, &akMsg, txIDHex)

		return nil
	}

	// Start Kafka consumer (replay from beginning)
	// Consumer group ID is unique to avoid conflicts
	groupID := fmt.Sprintf("aerospike-kafka-reader-%d", time.Now().Unix())

	// Start the Kafka listener in a goroutine
	go kafka.StartKafkaListener(ctx, logger, kafkaURL, groupID, false, consumerFn, &tSettings.Kafka)

	// Block until context is cancelled (Ctrl+C or other signal)
	<-ctx.Done()

	logger.Infof("Shutting down Aerospike Kafka connector reader...")

	return nil
}

// extractTxIDFromBins extracts the 32-byte txID from the bins map.
// The txID bin is stored by Aerospike and can be in various formats depending on
// the Kafka connector configuration (base64, hex, or byte array).
func extractTxIDFromBins(bins map[string]interface{}) ([]byte, error) {
	txIDRaw, ok := bins["txID"]
	if !ok {
		return nil, errors.NewProcessingError("txID bin not found")
	}

	// Handle different possible formats
	switch v := txIDRaw.(type) {
	case string:
		// Could be base64 or hex encoded
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil && len(decoded) == 32 {
			return decoded, nil
		}
		if decoded, err := hex.DecodeString(v); err == nil && len(decoded) == 32 {
			return decoded, nil
		}
		return nil, errors.NewProcessingError("string txID has invalid length after decoding")

	case []interface{}:
		// Array of bytes (JSON array of numbers)
		bytes := make([]byte, len(v))
		for i, b := range v {
			if num, ok := b.(float64); ok {
				bytes[i] = byte(num)
			} else {
				return nil, errors.NewProcessingError("invalid byte in txID array")
			}
		}
		if len(bytes) == 32 {
			return bytes, nil
		}
		return nil, errors.NewProcessingError("txID byte array has invalid length: %d", len(bytes))

	case []byte:
		// Direct byte array
		if len(v) == 32 {
			return v, nil
		}
		return nil, errors.NewProcessingError("txID byte slice has invalid length: %d", len(v))
	}

	return nil, errors.NewProcessingError("unsupported txID format: %T", txIDRaw)
}

// logOperation logs a single Aerospike operation
func logOperation(logger ulogger.Logger, msg *AerospikeKafkaMessage, txIDHex string) {
	timestamp := time.UnixMilli(msg.Metadata.Lut).Format("2006-01-02 15:04:05.000")

	if msg.Metadata.Msg == "write" {
		// Format bins for display
		bins := formatBins(msg.RawBins)

		// Truncate txID for display
		txIDDisplay := txIDHex
		if len(txIDDisplay) > 16 {
			txIDDisplay = txIDDisplay[:16] + "..."
		}

		logger.Infof("[%s] [WRITE] TxID: %s | Gen: %d | Set: %s",
			timestamp,
			txIDDisplay,
			msg.Metadata.Gen,
			msg.Metadata.Set)
		logger.Infof("  Bins: %s", bins)
	} else {
		// Delete operation has no bins
		logger.Infof("[%s] [DELETE] TxID: (no bins) | Gen: %d | Set: %s | Durable: %v",
			timestamp,
			msg.Metadata.Gen,
			msg.Metadata.Set,
			msg.Metadata.Durable)
	}
}

// formatBins creates a human-readable summary of bin contents
func formatBins(bins map[string]interface{}) string {
	if len(bins) == 0 {
		return "(no bins)"
	}

	parts := make([]string, 0, 32)
	for k, v := range bins {
		if k == "txID" {
			continue // Already shown in header
		}

		var valueStr string
		switch val := v.(type) {
		case []interface{}:
			valueStr = fmt.Sprintf("[%d items]", len(val))
		case string:
			if len(val) > 30 {
				valueStr = val[:30] + "..."
			} else {
				valueStr = val
			}
		case float64:
			valueStr = fmt.Sprintf("%.0f", val)
		case bool:
			valueStr = fmt.Sprintf("%v", val)
		default:
			valueStr = fmt.Sprintf("%v", val)
		}
		parts = append(parts, fmt.Sprintf("%s=%s", k, valueStr))
	}

	if len(parts) == 0 {
		return "(only txID bin)"
	}

	return strings.Join(parts, ", ")
}

// logStats periodically logs statistics
func logStats(ctx context.Context, logger ulogger.Logger, stats *Stats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final stats on exit
			total := stats.totalMessages.Load()
			matched := stats.matchedMessages.Load()
			writes := stats.writeOps.Load()
			deletes := stats.deleteOps.Load()

			logger.Infof("\n=== Final Statistics ===")
			logger.Infof("Total Messages: %d", total)
			logger.Infof("Matched Operations: %d", matched)
			logger.Infof("  - Writes: %d", writes)
			logger.Infof("  - Deletes: %d", deletes)
			return

		case <-ticker.C:
			total := stats.totalMessages.Load()
			matched := stats.matchedMessages.Load()
			writes := stats.writeOps.Load()
			deletes := stats.deleteOps.Load()

			logger.Infof("\n=== Statistics (%ds interval) ===", int(interval.Seconds()))
			logger.Infof("Total Messages: %d", total)
			logger.Infof("Matched Operations: %d", matched)
			logger.Infof("  - Writes: %d", writes)
			logger.Infof("  - Deletes: %d", deletes)
		}
	}
}
