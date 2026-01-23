package logs

import (
	"regexp"
	"strings"
	"time"
)

// LogLevel represents the severity of a log entry
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelUnknown
)

// String returns the string representation of a LogLevel
func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Short returns a 5-char padded string for display
func (l LogLevel) Short() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO "
	case LevelWarn:
		return "WARN "
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "???? "
	}
}

// ParseLogLevel converts a string to a LogLevel
func ParseLogLevel(s string) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARN", "WARNING":
		return LevelWarn
	case "ERROR":
		return LevelError
	case "FATAL", "PANIC":
		return LevelFatal
	default:
		return LevelUnknown
	}
}

// LogEntry represents a parsed log line
type LogEntry struct {
	Timestamp  time.Time
	Level      LogLevel
	Caller     string
	Service    string
	Message    string
	RawLine    string
	LineNumber int
}

// logPattern matches the teranode log format:
// 2025-12-08T10:20:45Z | INFO  | app/p2p/dht.go:70                | p2p   | message
var logPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s*\|\s*(\w+)\s*\|\s*([^|]+)\|\s*(\w+)\s*\|\s*(.*)$`)

// ParseLogLine parses a single log line into a LogEntry
func ParseLogLine(line string, lineNumber int) LogEntry {
	entry := LogEntry{
		RawLine:    line,
		LineNumber: lineNumber,
		Level:      LevelUnknown,
	}

	matches := logPattern.FindStringSubmatch(line)
	if matches == nil {
		// Line doesn't match expected format - treat as raw message
		entry.Message = line
		return entry
	}

	// Parse timestamp
	if ts, err := time.Parse(time.RFC3339, matches[1]); err == nil {
		entry.Timestamp = ts
	}

	// Parse level
	entry.Level = ParseLogLevel(matches[2])

	// Parse caller (trim whitespace)
	entry.Caller = strings.TrimSpace(matches[3])

	// Parse service (trim whitespace)
	entry.Service = strings.TrimSpace(matches[4])

	// Parse message
	entry.Message = matches[5]

	return entry
}

// ContainsTxID checks if the log message contains what looks like a transaction ID
// TxIDs are 64-character hex strings
func (e *LogEntry) ContainsTxID(txid string) bool {
	if len(txid) != 64 {
		return false
	}
	return strings.Contains(strings.ToLower(e.Message), strings.ToLower(txid))
}

// TimeString returns a short time string for display (HH:MM:SS)
func (e *LogEntry) TimeString() string {
	if e.Timestamp.IsZero() {
		return "        "
	}
	return e.Timestamp.Format("15:04:05")
}

// MatchesSearch checks if the entry matches a search string (case-insensitive)
func (e *LogEntry) MatchesSearch(search string) bool {
	if search == "" {
		return true
	}
	lower := strings.ToLower(search)
	return strings.Contains(strings.ToLower(e.Message), lower) ||
		strings.Contains(strings.ToLower(e.Service), lower) ||
		strings.Contains(strings.ToLower(e.Caller), lower)
}
