package handlers

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/poc/binserve/foldermanager"
	"github.com/labstack/echo/v4"
)

func Delete(c echo.Context) error {
	filePath, err := foldermanager.GetFilePath(c.Param("filename"))
	if err != nil {
		return sendError(c, http.StatusBadRequest, err)
	}

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return sendError(c, http.StatusNotFound, "File not found")
	}

	// We create a TTL file to mark as deleted

	f, err := os.Create(filePath + ".ttl")
	if err != nil {
		return sendError(c, http.StatusInternalServerError, fmt.Errorf("could not create TTL file %q: %w", filePath+".ttl", err))
	}

	b := make([]byte, 8)
	s := binary.PutVarint(b, time.Now().UnixMilli())

	if _, err := f.Write(b[:s]); err != nil {
		return sendError(c, http.StatusInternalServerError, fmt.Errorf("could not write TTL to %q: %w", filePath+".ttl", err))
	}

	if err := f.Close(); err != nil {
		return sendError(c, http.StatusInternalServerError, fmt.Errorf("could not close TTL file %q: %w", filePath+".ttl", err))
	}

	return c.String(http.StatusOK, fmt.Sprintf("%s queued for deletion", filePath))
}
