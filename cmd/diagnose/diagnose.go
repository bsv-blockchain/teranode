// Package diagnose provides diagnostic tools for Teranode nodes.
// It checks service health and validates configuration for common mistakes.
package diagnose

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// CheckStatus represents the result of a health check.
type CheckStatus string

const (
	StatusOK   CheckStatus = "OK"
	StatusFAIL CheckStatus = "FAIL"
	StatusSKIP CheckStatus = "SKIP"
)

// Severity represents the severity of a config check result.
type Severity int

const (
	SeverityOK Severity = iota
	SeverityINFO
	SeverityWARN
	SeverityERROR
)

func (s Severity) String() string {
	switch s {
	case SeverityOK:
		return "OK"
	case SeverityINFO:
		return "INFO"
	case SeverityWARN:
		return "WARN"
	case SeverityERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func (s Severity) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// HealthResult holds the result of a single health check.
type HealthResult struct {
	Service string      `json:"service"`
	Address string      `json:"address"`
	Status  CheckStatus `json:"status"`
	Latency string      `json:"latency,omitempty"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// ConfigResult holds the result of a single configuration check.
type ConfigResult struct {
	Severity    Severity `json:"severity"`
	Check       string   `json:"check"`
	Value       string   `json:"current_value"`
	Recommended string   `json:"recommended"`
}

// DiagnoseResult holds all diagnostic results.
type DiagnoseResult struct {
	Health []HealthResult `json:"health,omitempty"`
	Config []ConfigResult `json:"config,omitempty"`
}

// Run executes the diagnose command and returns an exit code.
// Exit codes: 0 = all pass, 1 = any errors/failures, 2 = warnings only.
func Run(logger ulogger.Logger, s *settings.Settings, checkMode, configMode, jsonOutput bool) int {
	// Default to check mode if neither specified
	if !checkMode && !configMode {
		checkMode = true
	}

	// Use a quiet logger to suppress client initialization noise
	quietLogger := logger.Duplicate(ulogger.WithLevel("ERROR"))

	result := DiagnoseResult{}

	if checkMode {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result.Health = runHealthChecks(ctx, quietLogger, s)
	}

	if configMode {
		result.Config = runConfigChecks(s)
	}

	if jsonOutput {
		renderJSON(result)
	} else {
		renderTable(result)
	}

	return exitCode(result)
}

func renderJSON(result DiagnoseResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
}

func renderTable(result DiagnoseResult) {
	if len(result.Health) > 0 {
		fmt.Println("Service Health Checks")
		fmt.Println("=====================")

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "SERVICE\tADDRESS\tSTATUS\tLATENCY\tMESSAGE\n")

		for _, h := range result.Health {
			latency := h.Latency
			if latency == "" {
				latency = "-"
			}

			msg := h.Message
			if h.Error != "" {
				msg = h.Error
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", h.Service, h.Address, h.Status, latency, msg)
		}

		w.Flush()

		ok, fail, skip := 0, 0, 0
		for _, h := range result.Health {
			switch h.Status {
			case StatusOK:
				ok++
			case StatusFAIL:
				fail++
			case StatusSKIP:
				skip++
			}
		}

		fmt.Printf("\n  %d OK, %d FAIL, %d SKIP\n\n", ok, fail, skip)
	}

	if len(result.Config) > 0 {
		fmt.Println("Configuration Checks")
		fmt.Println("====================")

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "SEVERITY\tCHECK\tVALUE\tRECOMMENDED\n")

		for _, c := range result.Config {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.Severity, c.Check, c.Value, c.Recommended)
		}

		w.Flush()

		okC, info, warn, errC := 0, 0, 0, 0
		for _, c := range result.Config {
			switch c.Severity {
			case SeverityOK:
				okC++
			case SeverityINFO:
				info++
			case SeverityWARN:
				warn++
			case SeverityERROR:
				errC++
			}
		}

		fmt.Printf("\n  %d OK, %d INFO, %d WARN, %d ERROR\n\n", okC, info, warn, errC)
	}
}

func exitCode(result DiagnoseResult) int {
	hasError := false
	hasWarn := false

	for _, h := range result.Health {
		if h.Status == StatusFAIL {
			hasError = true
		}
	}

	for _, c := range result.Config {
		switch c.Severity {
		case SeverityERROR:
			hasError = true
		case SeverityWARN:
			hasWarn = true
		}
	}

	if hasError {
		return 1
	}

	if hasWarn {
		return 2
	}

	return 0
}

func formatLatency(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}

	return fmt.Sprintf("%dms", d.Milliseconds())
}

func isHealthy(statusCode int) bool {
	return statusCode == http.StatusOK
}
