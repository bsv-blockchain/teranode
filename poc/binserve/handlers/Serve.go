package handlers

import (
	"net/http"
	"os"

	"github.com/bitcoin-sv/ubsv/poc/binserve/foldermanager"
	"github.com/labstack/echo/v4"
)

func Serve(c echo.Context) error {
	filePath, err := foldermanager.GetFilePath(c.Param("filename"))
	if err != nil {
		return sendError(c, http.StatusBadRequest, err)
	}

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return sendError(c, http.StatusNotFound, "File not found")
	}

	// Serve the file
	return c.File(filePath)
}
