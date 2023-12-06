package http_impl

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		stat := AssetStat.NewStat("GetSubtree_http")
		defer func() {
			stat.AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
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
		stat.NewStat("Get Subtree from repository").AddTime(start2)

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		// At this point, the subtree contains all the fees and sizes for the transactions in the subtree.

		if mode == JSON {
			return c.JSONPretty(200, subtree, "  ")
		}

		// If we did not serve JSON, we need to serialize the nodes into a byte slice.  We use SerializeNodes() for this which does NOT include the fees and sizes.

		b, err := subtree.SerializeNodes()
		if err != nil {
			return err
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			return c.String(200, hex.EncodeToString(b))

		default:
			err = errors.New("bad read mode")
			return sendError(c, http.StatusInternalServerError, 52, err)
		}
	}
}

type SubtreeNodesReader struct {
	reader    io.Reader
	itemCount int
	itemsRead int
	hashBuf   []byte
	extraBuf  []byte
}

func NewSubtreeNodesReader(subtreeReader io.Reader) (*SubtreeNodesReader, error) {
	hashBuf := make([]byte, 32)
	extraBuf := make([]byte, 16)

	// Read the root hash and skip
	if _, err := subtreeReader.Read(hashBuf); err != nil {
		return nil, err
	}

	n, err := wire.ReadVarInt(subtreeReader, 0)
	if err != nil { // fees
		return nil, err
	}
	_ = n

	if _, err := wire.ReadVarInt(subtreeReader, 0); err != nil { // sizeInBytes
		return nil, err
	}

	itemCount, err := wire.ReadVarInt(subtreeReader, 0) // numberOfLeaves
	if err != nil {
		return nil, err
	}

	return &SubtreeNodesReader{
		reader:    subtreeReader,
		itemCount: int(itemCount),
		hashBuf:   hashBuf,
		extraBuf:  extraBuf,
	}, nil
}

func (r *SubtreeNodesReader) Read(p []byte) (int, error) {
	if r.itemsRead >= r.itemCount {
		return 0, io.EOF // No more data
	}

	if len(p) < 32 {
		return 0, errors.New("buffer too small")
	}

	if n, err := r.reader.Read(r.hashBuf); err != nil {
		return n, err
	}

	// Copy data to p
	n := copy(p, r.hashBuf)
	r.itemsRead++

	return n, nil
}

func (h *HTTP) GetSubtreeAsReader(c echo.Context) error {
	start := gocore.CurrentTime()
	stat := AssetStat.NewStat("GetSubtree_http")
	defer func() {
		stat.AddTime(start)
	}()

	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return err
	}

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

func (h *HTTP) GetSubtreeStream() func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetSubtreeStream_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetSubtreeStream for %s: %s", c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		subtreeBytes, err := h.repository.GetSubtreeBytes(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		subtree := &util.Subtree{}
		nodeChan, errChan, err := subtree.DeserializeChan(subtreeBytes)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// Set response type
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)
		c.Response().WriteHeader(http.StatusOK)

		for {
			select {
			case node, ok := <-nodeChan:
				if !ok {
					return nil // Channel closed, end of stream
				}
				if _, err := c.Response().Write(node.Hash[:]); err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError, "Failed to write to response")
				}
				c.Response().Flush()

			case err := <-errChan:
				if err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
		}
	}
}
