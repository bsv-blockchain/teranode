package ulogger_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
)

func captureStdout(f func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()

	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	r.Close()

	return buf.String()
}

func TestLogLevels(t *testing.T) {
	tests := []struct {
		level           string
		expectedOutputs map[string]bool
	}{
		{
			level: "DEBUG",
			expectedOutputs: map[string]bool{
				"DEBUG": true,
				"INFO":  true,
				"WARN":  true,
				"ERROR": true,
				"FATAL": true,
			},
		},
		{
			level: "INFO",
			expectedOutputs: map[string]bool{
				"DEBUG": false,
				"INFO":  true,
				"WARN":  true,
				"ERROR": true,
				"FATAL": true,
			},
		},
		{
			level: "WARN",
			expectedOutputs: map[string]bool{
				"DEBUG": false,
				"INFO":  false,
				"WARN":  true,
				"ERROR": true,
				"FATAL": true,
			},
		},
		{
			level: "ERROR",
			expectedOutputs: map[string]bool{
				"DEBUG": false,
				"INFO":  false,
				"WARN":  false,
				"ERROR": true,
				"FATAL": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			// Capture the output of the logger
			output := captureStdout(func() {
				logger := ulogger.New("test-service", ulogger.WithLevel(tt.level))

				logger.Debugf("DEBUG message")
				logger.Infof("INFO message")
				logger.Warnf("WARN message")
				logger.Errorf("ERROR message")
			})

			fmt.Println(output)

			// Check if the expected outputs are present in the captured output
			if got := strings.Contains(output, "DEBUG message"); got != tt.expectedOutputs["DEBUG"] {
				t.Errorf("expected DEBUG output: %v, got: %v", tt.expectedOutputs["DEBUG"], got)
			}

			if got := strings.Contains(output, "INFO message"); got != tt.expectedOutputs["INFO"] {
				t.Errorf("expected INFO output: %v, got: %v", tt.expectedOutputs["INFO"], got)
			}

			if got := strings.Contains(output, "WARN message"); got != tt.expectedOutputs["WARN"] {
				t.Errorf("expected WARN output: %v, got: %v", tt.expectedOutputs["WARN"], got)
			}

			if got := strings.Contains(output, "ERROR message"); got != tt.expectedOutputs["ERROR"] {
				t.Errorf("expected ERROR output: %v, got: %v", tt.expectedOutputs["ERROR"], got)
			}
		})
	}
}
