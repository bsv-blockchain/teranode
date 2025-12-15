// Package logs provides an interactive TUI log viewer for teranode.
// It supports real-time log tailing, filtering by service and log level,
// text search, and transaction ID tracking across services.
package logs

import (
	"os"

	"github.com/bsv-blockchain/teranode/errors"
	tea "github.com/charmbracelet/bubbletea"
)

// Run starts the interactive log viewer
func Run(logFile string, bufferSize int) error {
	// Check if log file exists
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		return errors.NewError("log file not found: " + logFile + "\n\nMake sure teranode is running with logrotate:\n  ./scripts/run-teranode-with-logrotate.sh")
	}

	// Create the model
	model, err := NewModel(logFile, bufferSize)
	if err != nil {
		return errors.NewError("failed to initialize log viewer", err)
	}

	// Run the TUI
	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),       // Use alternate screen buffer
		tea.WithMouseCellMotion(), // Enable mouse support for scrolling
	)

	if _, err := p.Run(); err != nil {
		return errors.NewError("error running log viewer", err)
	}

	return nil
}
