package ulogger_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
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

func captureStderr(f func()) string {
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	f()

	w.Close()

	os.Stderr = oldStderr

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	r.Close()

	return buf.String()
}

func captureBoth(f func()) (stdout, stderr string) {
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()

	os.Stdout = wOut
	os.Stderr = wErr

	f()

	wOut.Close()
	wErr.Close()

	os.Stdout = oldStdout
	os.Stderr = oldStderr

	var bufOut, bufErr bytes.Buffer
	_, _ = io.Copy(&bufOut, rOut)
	_, _ = io.Copy(&bufErr, rErr)

	rOut.Close()
	rErr.Close()

	return bufOut.String(), bufErr.String()
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

func TestJSONLogging(t *testing.T) {
	// Save current config and restore after test
	originalJsonLogging := gocore.Config().GetBool("jsonLogging", false)
	defer func() {
		if originalJsonLogging {
			gocore.Config().Set("jsonLogging", "true")
		} else {
			gocore.Config().Unset("jsonLogging")
		}
	}()

	// Test JSON logging enabled
	gocore.Config().Set("jsonLogging", "true")

	t.Run("JSONLoggingEnabled", func(t *testing.T) {
		stdout, stderr := captureBoth(func() {
			logger := ulogger.New("blockvalidation", ulogger.WithLevel("DEBUG"))

			// Simulate a complex block validation scenario
			logger.Infof("Starting block validation for height %d, hash %s", 850123, "00000000000000000007316856900e76b4f7a9139cfbfba89842c8d196cd5f91")
			logger.Debugf("Validating block header: timestamp=%d, merkleRoot=%s", 1642723200, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
			logger.Warnf("Block validation time exceeded threshold: %dms > %dms for block %d", 2500, 2000, 850123)
			logger.Errorf("Block validation failed: invalid merkle root for block %d, expected %s", 850123, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

			// Test logging with JSON-like transaction data and complex structures
			logger.Infof("Processing transaction validation: {\"txid\":\"tx_%s\",\"inputs\":%d,\"outputs\":%d}", "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069", 3, 2)
			logger.Debugf("UTXO verification result: {\"status\":\"verified\",\"spent_count\":%d,\"missing_utxos\":[]}", 15)
		})

		// fmt.Println("stdout:", stdout)
		// fmt.Println("stderr:", stderr)

		// Verify both text and JSON output goes to stdout with expected content
		expectedTextMessages := []string{
			"Starting block validation for height 850123",
			"blockvalidation",
			"Validating block header: timestamp=1642723200",
			"Block validation time exceeded threshold",
			"Block validation failed: invalid merkle root",
			"Processing transaction validation:",
			"UTXO verification result:",
		}

		for _, msg := range expectedTextMessages {
			if !strings.Contains(stdout, msg) {
				t.Errorf("Expected text message '%s' in stdout", msg)
			}
		}

		// Verify JSON output also goes to stdout and is properly structured
		if !strings.Contains(stdout, `"level":"info"`) {
			t.Error("Expected JSON output in stdout")
		}

		// Extract JSON lines from stdout (they should be mixed with pretty text)
		lines := strings.Split(strings.TrimSpace(stdout), "\n")
		jsonLogCount := 0
		levelCounts := map[string]int{
			"debug": 0,
			"info":  0,
			"warn":  0,
			"error": 0,
		}

		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}

			// Check if this line is JSON (starts with { and ends with })
			if strings.HasPrefix(strings.TrimSpace(line), "{") && strings.HasSuffix(strings.TrimSpace(line), "}") {
				var jsonLog map[string]interface{}
				if err := json.Unmarshal([]byte(line), &jsonLog); err != nil {
					// Not a valid JSON log line, skip
					continue
				}
				jsonLogCount++

				// Verify required JSON structure
				requiredFields := []string{"time", "level", "message"}
				for _, field := range requiredFields {
					if _, ok := jsonLog[field]; !ok {
						t.Errorf("JSON log missing required field '%s': %v", field, jsonLog)
					}
				}

				// Count log levels to verify all were captured
				if level, ok := jsonLog["level"].(string); ok {
					if _, exists := levelCounts[level]; exists {
						levelCounts[level]++
					}
				}

				// Verify complex content is properly handled
				message := fmt.Sprintf("%v", jsonLog["message"])

				// Check that JSON content within log messages is properly escaped
				if strings.Contains(message, "tx_7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069") {
					if !strings.Contains(line, `\"txid\":\"tx_7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069\"`) && !strings.Contains(line, `"txid":"tx_7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"`) {
						t.Error("JSON content not properly handled in log message")
					}
				}

				// Verify numeric formatting is preserved
				if strings.Contains(message, "height 850123") {
					if !strings.Contains(message, "850123") {
						t.Error("Block height formatting not preserved in log message")
					}
				}
			}
		}

		// Verify we captured all expected log levels and counts
		expectedCounts := map[string]int{
			"debug": 2, // 2 debug messages
			"info":  2, // 2 info messages
			"warn":  1, // 1 warn message
			"error": 1, // 1 error message
		}

		for level, expectedCount := range expectedCounts {
			if levelCounts[level] != expectedCount {
				t.Errorf("Expected %d %s logs, but got %d", expectedCount, level, levelCounts[level])
			}
		}

		// Verify total log count
		expectedTotalLogs := 6
		if jsonLogCount < expectedTotalLogs {
			t.Errorf("Expected at least %d JSON log entries, but got %d", expectedTotalLogs, jsonLogCount)
		}

		// Verify stderr is empty or only contains debug initialization message
		// (the initialization message might go to stderr from the pretty logger setup)
		if stderr != "" && !strings.Contains(stderr, "Zerolog logger initialized") {
			t.Errorf("Expected stderr to be empty or only contain init message, but got: %s", stderr)
		}
	})

	// Test JSON logging disabled
	t.Run("JSONLoggingDisabled", func(t *testing.T) {
		gocore.Config().Set("jsonLogging", "false")

		stdout, stderr := captureBoth(func() {
			logger := ulogger.New("test-service", ulogger.WithLevel("INFO"))
			logger.Infof("Test non-JSON message")
		})

		// Text should go to stdout
		if !strings.Contains(stdout, "Test non-JSON message") {
			t.Errorf("Expected text message in stdout, but got: %s", stdout)
		}

		// No JSON output should be present
		if strings.Contains(stdout, `"level":"info"`) {
			t.Errorf("Unexpected JSON output in stdout: %s", stdout)
		}

		// Verify stderr is empty or only contains debug initialization message
		if stderr != "" && !strings.Contains(stderr, "Zerolog logger initialized") {
			t.Errorf("Expected stderr to be empty or only contain init message, but got: %s", stderr)
		}
	})
}

func TestJSONLoggingLevels(t *testing.T) {
	// Save current config and restore after test
	originalJsonLogging := gocore.Config().GetBool("jsonLogging", false)
	defer func() {
		if originalJsonLogging {
			gocore.Config().Set("jsonLogging", "true")
		} else {
			gocore.Config().Unset("jsonLogging")
		}
	}()

	// Enable JSON logging for this test
	gocore.Config().Set("jsonLogging", "true")

	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for _, level := range levels {
		t.Run(fmt.Sprintf("JSONLevel_%s", level), func(t *testing.T) {
			stdout, _ := captureBoth(func() {
				logger := ulogger.New("test-service", ulogger.WithLevel("DEBUG"))

				switch level {
				case "DEBUG":
					logger.Debugf("Test %s message", level)
				case "INFO":
					logger.Infof("Test %s message", level)
				case "WARN":
					logger.Warnf("Test %s message", level)
				case "ERROR":
					logger.Errorf("Test %s message", level)
				}
			})

			// Verify JSON contains the correct level in stdout
			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			found := false

			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}

				// Check if this line is JSON
				if strings.HasPrefix(strings.TrimSpace(line), "{") && strings.HasSuffix(strings.TrimSpace(line), "}") {
					var jsonLog map[string]interface{}
					if err := json.Unmarshal([]byte(line), &jsonLog); err != nil {
						continue
					}

					if strings.Contains(fmt.Sprintf("%v", jsonLog["message"]), fmt.Sprintf("Test %s message", level)) {
						expectedLevel := strings.ToLower(level)
						if level == "WARN" {
							expectedLevel = "warn"
						}

						if jsonLog["level"] != expectedLevel {
							t.Errorf("Expected level %s, got %v", expectedLevel, jsonLog["level"])
						}
						found = true
						break
					}
				}
			}

			if !found {
				t.Errorf("Expected to find JSON log entry for level %s in stdout", level)
			}
		})
	}
}
