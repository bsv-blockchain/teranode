package http_impl

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

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

func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		var b []byte

		start := gocore.CurrentTime()
		stat := AssetStat.NewStat("GetSubtree_http")

		defer func() {
			stat.AddTime(start)
			duration := time.Since(start)
			sizeInKB := float64(len(b)) / 1024

			h.logger.Infof("[Asset_http] GetSubtree in %s for %s (%.2f kB): %s DONE in %s (%.2f kB/sec)", mode, c.Request().RemoteAddr, c.Param("hash"), sizeInKB, duration, calculateSpeed(duration, sizeInKB))
		}()

		h.logger.Infof("[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		start2 := gocore.CurrentTime()
		subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		start2 = stat.NewStat("Get Subtree from repository").AddTime(start2)

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		// At this point, the subtree contains all the fees and sizes for the transactions in the subtree.

		if mode == JSON {
			/*
					subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
				if err != nil {
					if strings.HasSuffix(err.Error(), " not found") {
						return echo.NewHTTPError(http.StatusNotFound, err.Error())
					} else {
						return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
					}
				}
			*/
			h.logger.Infof("[GetSubtree][%s] sending to client in json (%d nodes)", hash.String(), subtree.Length())
			return c.JSONPretty(200, subtree, "  ")
		}

		// If we did not serve JSON, we need to serialize the nodes into a byte slice.
		// We use SerializeNodes() for this which does NOT include the fees and sizes.
		b, err = subtree.SerializeNodes()
		if err != nil {
			return err
		}

		// DeserializeTransactionIDsOf

		stat.NewStat("Serialize Subtree").AddTime(start2)

		switch mode {
		case BINARY_STREAM:
			h.logger.Infof("[GetSubtree][%s] sending to client in binary (%d bytes)", hash.String(), len(b))
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			h.logger.Infof("[GetSubtree][%s] sending to client in hex (%d bytes)", hash.String(), len(b))
			return c.String(200, hex.EncodeToString(b))

		default:
			err = errors.New("bad read mode")
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
	if _, err := subtreeReader.Read(make([]byte, 32)); err != nil {
		return nil, err
	}

	b := make([]byte, 8)
	if _, err := subtreeReader.Read(b); err != nil { // fee
		return nil, err
	}
	if _, err := subtreeReader.Read(b); err != nil { // sizeInBytes
		return nil, err
	}
	if _, err := subtreeReader.Read(b); err != nil { // numberOfLeaves
		return nil, err
	}
	itemCount := binary.LittleEndian.Uint64(b)

	return &SubtreeNodesReader{
		reader:    bufio.NewReaderSize(subtreeReader, 1024*1024*4),
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
		if strings.HasSuffix(err.Error(), " not found") {
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
