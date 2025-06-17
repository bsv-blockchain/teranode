// Package settings provide utilities for managing and displaying application settings.
// It is intended for debugging and inspection of application configuration and runtime behavior.
//
// Usage:
//
// This package is typically used to print application settings in JSON format,
// along-with-version and commit information, for debugging and documentation purposes.
//
// Functions:
//   - PrintSettings: Prints application settings, version, and commit information in a structured format.
//
// Side effects:
//
// Functions in this package may print to stdout and log errors if they occur.
package settings

import (
	"encoding/json"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
)

// PrintSettings prints the application settings, version, and commit information in a structured format.
//
// This function is used to display the current application configuration and runtime details
// for debugging and documentation purposes. It outputs the settings in JSON format along with
// version and commit metadata.
//
// Parameters:
//   - logger: A logger instance for logging errors.
//   - settings: The application settings to be displayed.
//   - version: The version of the application.
//   - commit: The commit hash of the application.
//
// Side effects:
//   - Prints the settings, version, and commit information to stdout.
//   - Logs errors if the settings cannot be marshaled to JSON.
func PrintSettings(logger ulogger.Logger, settings *settings.Settings, version, commit string) {
	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	settingsJSON, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		logger.Errorf("Failed to marshal settings: %v", err)
	} else {
		logger.Infof("SETTINGS JSON\n-------------\n%s\n\n", string(settingsJSON))
	}
}
