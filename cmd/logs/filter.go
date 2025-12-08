package logs

import (
	"strings"
)

// Filter represents the current filter configuration
type Filter struct {
	Services   map[string]bool // Allowed services (empty = all)
	MinLevel   LogLevel        // Minimum log level to show
	SearchText string          // Text search in message
	TxID       string          // Transaction ID search (64-char hex)
}

// NewFilter creates a new filter with default settings
func NewFilter() *Filter {
	return &Filter{
		Services: make(map[string]bool),
		MinLevel: LevelDebug, // Show all levels by default
	}
}

// Match returns true if the entry matches all filter criteria
func (f *Filter) Match(entry LogEntry) bool {
	// Level filter
	if entry.Level != LevelUnknown && entry.Level < f.MinLevel {
		return false
	}

	// Service filter
	if len(f.Services) > 0 {
		if entry.Service == "" {
			return false
		}
		if !f.Services[strings.ToLower(entry.Service)] {
			return false
		}
	}

	// Text search
	if f.SearchText != "" && !entry.MatchesSearch(f.SearchText) {
		return false
	}

	// TxID search
	if f.TxID != "" && !entry.ContainsTxID(f.TxID) {
		return false
	}

	return true
}

// SetServices sets the service filter from a comma-separated string
func (f *Filter) SetServices(services string) {
	f.Services = make(map[string]bool)
	if services == "" {
		return
	}
	for _, s := range strings.Split(services, ",") {
		s = strings.TrimSpace(strings.ToLower(s))
		if s != "" {
			f.Services[s] = true
		}
	}
}

// GetServicesString returns the current service filter as a comma-separated string
func (f *Filter) GetServicesString() string {
	if len(f.Services) == 0 {
		return ""
	}
	services := make([]string, 0, len(f.Services))
	for s := range f.Services {
		services = append(services, s)
	}
	return strings.Join(services, ",")
}

// ToggleService toggles a service in the filter
func (f *Filter) ToggleService(service string) {
	service = strings.ToLower(strings.TrimSpace(service))
	if service == "" {
		return
	}
	if f.Services[service] {
		delete(f.Services, service)
	} else {
		f.Services[service] = true
	}
}

// SetMinLevel sets the minimum log level
func (f *Filter) SetMinLevel(level LogLevel) {
	f.MinLevel = level
}

// IncreaseLevel increases the minimum log level (shows fewer logs)
func (f *Filter) IncreaseLevel() {
	if f.MinLevel < LevelFatal {
		f.MinLevel++
	}
}

// DecreaseLevel decreases the minimum log level (shows more logs)
func (f *Filter) DecreaseLevel() {
	if f.MinLevel > LevelDebug {
		f.MinLevel--
	}
}

// SetSearch sets the text search filter
func (f *Filter) SetSearch(text string) {
	f.SearchText = text
}

// SetTxID sets the transaction ID filter
func (f *Filter) SetTxID(txid string) {
	// Validate txid format (should be 64-char hex)
	txid = strings.TrimSpace(txid)
	if len(txid) == 64 {
		f.TxID = txid
	} else {
		f.TxID = ""
	}
}

// Clear resets all filters to default
func (f *Filter) Clear() {
	f.Services = make(map[string]bool)
	f.MinLevel = LevelDebug
	f.SearchText = ""
	f.TxID = ""
}

// HasActiveFilters returns true if any filter is active
func (f *Filter) HasActiveFilters() bool {
	return len(f.Services) > 0 || f.MinLevel > LevelDebug || f.SearchText != "" || f.TxID != ""
}

// FilterEntries returns a new slice containing only entries that match the filter
func (f *Filter) FilterEntries(entries []LogEntry) []LogEntry {
	if !f.HasActiveFilters() {
		return entries
	}

	result := make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		if f.Match(entry) {
			result = append(result, entry)
		}
	}
	return result
}

// Summary returns a human-readable summary of active filters
func (f *Filter) Summary() string {
	var parts []string

	if f.MinLevel > LevelDebug {
		parts = append(parts, f.MinLevel.String()+"+")
	}

	if len(f.Services) > 0 {
		parts = append(parts, f.GetServicesString())
	}

	if f.SearchText != "" {
		parts = append(parts, "\""+f.SearchText+"\"")
	}

	if f.TxID != "" {
		parts = append(parts, "tx:"+f.TxID[:8]+"...")
	}

	if len(parts) == 0 {
		return "all"
	}

	return strings.Join(parts, " | ")
}
