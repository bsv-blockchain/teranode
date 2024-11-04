// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// calculateSpeed takes the duration of the transfer and the size of the data transferred (in bytes)
// and returns the speed in kilobytes per second.
func calculateSpeed(duration time.Duration, sizeInKB float64) float64 {
	// Convert duration to seconds
	seconds := duration.Seconds()

	// Calculate speed in KB/s
	speed := sizeInKB / seconds

	return speed
}

// GetSubtree creates an HTTP handler for retrieving subtree data in multiple formats.
// Includes performance monitoring and response signing.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Subtree hash (hex string)
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body:
//     {
//     "height": <int>,
//     "fees": <uint64>,
//     "size_in_bytes": <uint64>,
//     "fee_hash": "<string>",
//     "nodes": [
//     // Array of subtree nodes
//     ],
//     "conflicting_nodes": [
//     // Array of conflicting node hashes
//     ]
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw subtree node data (32 bytes per node)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal encoding of node data
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Subtree not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid subtree hash format
//
//   - Subtree deserialization errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetSubtree_http" statistic
//   - Prometheus metric "asset_http_get_subtree" tracks responses
//   - Performance logging including transfer speed (KB/sec)
//   - Response size logging in KB
//
// Security:
//   - Response includes cryptographic signature if private key is configured
//
// Notes:
//   - JSON mode requires full subtree deserialization
//   - Binary/Hex modes use more efficient streaming approach
//   - Includes performance metrics in logs
func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		var b []byte

		start := gocore.CurrentTime()
		stat := AssetStat.NewStat("GetSubtree_http")

		defer func() {
			stat.AddTime(start)
			duration := time.Since(start)
			sizeInKB := float64(len(b)) / 1024

			h.logger.Infof("[Asset_http] GetSubtree in %s for %s: %s (%.2f kB): %s DONE in %s (%.2f kB/sec)", mode, c.Request().RemoteAddr, c.Param("hash"), sizeInKB, duration, calculateSpeed(duration, sizeInKB))
		}()

		h.logger.Infof("[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		// sign the response, if the private key is set, ignore error
		// do this before any output is sent to the client, this adds a signature to the response header
		_ = h.Sign(c.Response(), hash.CloneBytes())

		// At this point, the subtree contains all the fees and sizes for the transactions in the subtree.

		if mode == JSON {
			start2 := gocore.CurrentTime()
			// get subtree is much less efficient than get subtree reader and then only deserializing the nodes
			// this is only needed for the json response
			subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
			_ = stat.NewStat("Get Subtree from repository").AddTime(start2)

			h.logger.Infof("[GetSubtree][%s] sending to client in json (%d nodes)", hash.String(), subtree.Length())
			return c.JSONPretty(200, subtree, "  ")
		}

		// get subtree reader is much more efficient than get subtree
		subtreeReader, err := h.repository.GetSubtreeReader(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// Deserialize the nodes from the reader will return a byte slice of the nodes directly
		b, err = util.DeserializeNodesFromReader(subtreeReader)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			h.logger.Infof("[GetSubtree][%s] sending to client in binary (%d bytes)", hash.String(), len(b))
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			h.logger.Infof("[GetSubtree][%s] sending to client in hex (%d bytes)", hash.String(), len(b))
			return c.String(200, hex.EncodeToString(b))

		default:
			err = errors.NewUnknownError("bad read mode")
			return sendError(c, http.StatusInternalServerError, 52, err)
		}
	}
}

type SubtreeNodesReader struct {
	reader    *bufio.Reader
	itemCount int
	itemsRead int
	extraBuf  []byte
}

func NewSubtreeNodesReader(subtreeReader io.Reader) (*SubtreeNodesReader, error) {
	// Read the root hash and skip
	if _, err := io.ReadFull(subtreeReader, make([]byte, 32)); err != nil {
		return nil, err
	}

	b := make([]byte, 8)
	if _, err := io.ReadFull(subtreeReader, b); err != nil { // fee
		return nil, err
	}
	if _, err := io.ReadFull(subtreeReader, b); err != nil { // sizeInBytes
		return nil, err
	}
	if _, err := io.ReadFull(subtreeReader, b); err != nil { // numberOfLeaves
		return nil, err
	}
	itemCount := binary.LittleEndian.Uint64(b)

	return &SubtreeNodesReader{
		reader:    bufio.NewReaderSize(subtreeReader, 1024*1024*4), // 4MB buffer
		itemCount: int(itemCount),
		extraBuf:  make([]byte, 16),
	}, nil
}

func (r *SubtreeNodesReader) Read(p []byte) (int, error) {
	if r.itemsRead >= r.itemCount {
		return 0, io.EOF // No more data
	}

	totalRead := 0
	for len(p) >= 32 { // Check if there's space for at least one more 32-byte item
		if r.itemsRead >= r.itemCount {
			break
		}

		// Read the 32-byte item
		n, err := readFull(r.reader, p[:32])
		if err != nil {
			return totalRead + n, err
		}
		totalRead += n
		p = p[32:]

		// Skip the next 16 bytes
		_, err = readFull(r.reader, r.extraBuf[:])
		if err != nil {
			return totalRead, err
		}

		r.itemsRead++
	}

	return totalRead, nil
}

// readFull is similar to io.ReadFull but more tailored to this specific use case
func readFull(reader io.Reader, buf []byte) (int, error) {
	bytesRead := 0
	for bytesRead < len(buf) {
		n, err := reader.Read(buf[bytesRead:])
		if err != nil {
			return bytesRead, err
		}
		bytesRead += n
	}
	return bytesRead, nil
}

// GetSubtreeAsReader creates an HTTP handler for streaming subtree node data
// efficiently using a custom reader implementation. This endpoint provides better
// memory efficiency for large subtrees.
//
// URL Parameters:
//   - hash: Subtree hash (hex string)
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Streamed subtree node data:
//	  - Skips root hash (32 bytes)
//	  - Skips fees (8 bytes)
//	  - Skips size (8 bytes)
//	  - Reads number of leaves (8 bytes)
//	  - Streams node hashes (32 bytes each)
//
// Performance:
//   - Uses 4MB buffer for reading
//   - Skips non-essential data during streaming
//   - Efficient memory usage for large subtrees
//   - Performance metrics logged on completion
//
// Error handling and monitoring are identical to GetSubtree endpoint
func (h *HTTP) GetSubtreeAsReader(c echo.Context) error {
	start := gocore.CurrentTime()
	stat := AssetStat.NewStat("GetSubtreeAsReader_http")
	defer func() {
		stat.AddTime(start)
		h.logger.Infof("[Asset_http] GetSubtree using reader for %s: %s DONE in %s", c.Request().RemoteAddr, c.Param("hash"), time.Since(start))
	}()

	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return err
	}

	h.logger.Infof("[Asset_http] GetSubtree using reader for %s: %s", c.Request().RemoteAddr, c.Param("hash"))

	start2 := gocore.CurrentTime()
	subtreeReader, err := h.repository.GetSubtreeReader(c.Request().Context(), hash)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	stat.NewStat("Get Subtree from repository").AddTime(start2)

	prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

	r, err := NewSubtreeNodesReader(subtreeReader)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.Stream(200, echo.MIMEOctetStream, r)
}
