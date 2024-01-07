package handlers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/poc/binserve/foldermanager"
	"github.com/labstack/echo/v4"
)

func Upload(c echo.Context) error {
	filePath, err := foldermanager.GetFilePath(c.Param("filename"))
	if err != nil {
		return sendError(c, http.StatusBadRequest, err)
	}

	ttlString := c.QueryParam("ttl")

	if ttlString != "" {
		ttl, err := time.ParseDuration(ttlString)
		if err != nil {
			return sendError(c, http.StatusInternalServerError, `Invalid ttl value. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`)
		}

		if ttl == 0 {
			// Make sure ttl file does not exist
			if err := os.Remove(filePath + ".ttl"); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not remove file %q: %w", filePath+".ttl", err))
				}
			}

		} else {

			f, err := os.Create(filePath + ".ttl")
			if err != nil {
				return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not create TTL file %q: %w", filePath+".ttl", err))
			}

			b := make([]byte, 8)
			s := binary.PutVarint(b, time.Now().Add(ttl).UnixMilli())

			if _, err := f.Write(b[:s]); err != nil {
				return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not write TTL to %q: %w", filePath+".ttl", err))
			}

			if err := f.Close(); err != nil {
				return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not close TTL file %q: %w", filePath+".ttl", err))
			}
		}
	}

	if c.Request().ContentLength == 0 && ttlString == "" {
		return sendError(c, http.StatusBadRequest, "No ttl or request body provided: "+filePath)
	}

	if c.Request().ContentLength > 0 {
		dst, err := os.Create(filePath + ".tmp")
		if err != nil {
			return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not create file %q: %w", filePath+".tmp", err))
		}

		// Stream the file directly from the request body to the destination
		if _, err = io.Copy(dst, c.Request().Body); err != nil {
			dst.Close()
			return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not copy to %q: %w", filePath+".tmp", err))
		}

		if err := dst.Close(); err != nil {
			return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not close file %q: %w", filePath+".tmp", err))
		}

		// Rename the file to remove the .tmp extension
		if err := os.Rename(filePath+".tmp", filePath); err != nil {
			return sendError(c, http.StatusInternalServerError, fmt.Errorf("Could not rename file from %q to %q: %w", filePath+".tmp", filePath, err))

		}
	}

	if ttlString != "" {
		return c.String(http.StatusOK, "File and ttl uploaded successfully: "+filePath)
	}

	return c.String(http.StatusOK, "File uploaded successfully: "+filePath)
}
