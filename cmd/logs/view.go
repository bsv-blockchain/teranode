package logs

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// renderView renders the complete view
func (m *Model) renderView() string {
	if m.mode == ModeHelp {
		return m.renderHelpView()
	}

	var b strings.Builder

	// Header
	b.WriteString(m.renderHeader())
	b.WriteString("\n")

	// Main content (viewport or input)
	switch m.mode {
	case ModeSearch:
		b.WriteString(m.viewport.View())
		b.WriteString("\n")
		b.WriteString(m.searchInput.View())
	case ModeFilter:
		b.WriteString(m.viewport.View())
		b.WriteString("\n")
		b.WriteString(m.filterInput.View())
		b.WriteString("\n")
		b.WriteString(m.renderServiceHint())
	case ModeTxSearch:
		b.WriteString(m.viewport.View())
		b.WriteString("\n")
		b.WriteString(m.txInput.View())
	default:
		b.WriteString(m.viewport.View())
		b.WriteString("\n")
		// Error panel (if enabled)
		if m.showErrorPanel {
			b.WriteString(m.renderErrorPanel())
			b.WriteString("\n")
		}
		b.WriteString(m.renderStatusBar())
	}

	// Help line
	b.WriteString("\n")
	b.WriteString(m.renderHelpLine())

	return b.String()
}

// renderHeader renders the header line
func (m *Model) renderHeader() string {
	// Title
	title := TitleStyle.Render("TERANODE LOGS")

	// Rate graph (if enabled)
	rateInfo := ""
	if m.showRateGraph {
		sparkline := m.rateTracker.Sparkline()
		currentRate := m.rateTracker.CurrentRate()
		rateInfo = " " + SparklineStyle.Render(sparkline) + " " + RateStyle.Render(fmt.Sprintf("%d/s", currentRate))
	}

	// Status indicators
	var status []string

	if m.paused {
		status = append(status, PausedStyle.Render("PAUSED"))
	} else {
		status = append(status, ActiveStyle.Render("LIVE"))
	}

	if !m.mouseEnabled {
		status = append(status, HelpStyle.Render("SELECT"))
	}

	status = append(status, fmt.Sprintf("%d lines", len(m.filteredLogs)))

	if m.buffer.Size() != len(m.filteredLogs) {
		status = append(status, fmt.Sprintf("(%d total)", m.buffer.Size()))
	}

	statusStr := strings.Join(status, " | ")

	// Calculate spacing
	titleLen := lipgloss.Width(title)
	rateLen := lipgloss.Width(rateInfo)
	statusLen := lipgloss.Width(statusStr)
	padding := m.width - titleLen - rateLen - statusLen - 2
	if padding < 1 {
		padding = 1
	}

	return title + rateInfo + strings.Repeat(" ", padding) + statusStr
}

// renderStatusBar renders the status bar showing current filters
func (m *Model) renderStatusBar() string {
	filterSummary := "Filter: " + m.filter.Summary()

	return StatusBarStyle.Width(m.width).Render(filterSummary)
}

// renderHelpLine renders the bottom help line
func (m *Model) renderHelpLine() string {
	return HelpStyle.Render(m.keys.ShortHelp())
}

// renderLogs renders all log entries for the viewport
func (m *Model) renderLogs() string {
	if len(m.filteredLogs) == 0 {
		return HelpStyle.Render("No log entries" + m.getNoLogsHint())
	}

	var b strings.Builder
	for i, entry := range m.filteredLogs {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(m.renderLogEntry(entry))
	}
	return b.String()
}

// getNoLogsHint returns a hint about why there might be no logs
func (m *Model) getNoLogsHint() string {
	if m.filter.HasActiveFilters() {
		return " (try clearing filters with 'c')"
	}
	if m.buffer.Size() == 0 {
		return fmt.Sprintf(" (waiting for logs from %s)", m.logFile)
	}
	return ""
}

// renderLogEntry renders a single log entry
func (m *Model) renderLogEntry(entry LogEntry) string {
	// If unparseable, show raw line
	if entry.Level == LevelUnknown && entry.Service == "" {
		return HelpStyle.Render(entry.RawLine)
	}

	var b strings.Builder

	// Timestamp
	b.WriteString(TimestampStyle.Render(entry.TimeString()))
	b.WriteString(" ")

	// Level with color
	levelStyle := GetLevelStyle(entry.Level)
	b.WriteString(levelStyle.Render(entry.Level.Short()))
	b.WriteString(" ")

	// Service with color
	if entry.Service != "" {
		serviceStyle := GetServiceStyle(entry.Service)
		// Pad service name to 8 chars for alignment
		service := entry.Service
		if len(service) > 8 {
			service = service[:8]
		}
		b.WriteString(serviceStyle.Render(fmt.Sprintf("%-8s", service)))
		b.WriteString(" ")
	}

	// Message (potentially highlighted)
	message := entry.Message
	if m.filter.SearchText != "" {
		message = m.highlightText(message, m.filter.SearchText)
	}
	if m.filter.TxID != "" {
		message = m.highlightText(message, m.filter.TxID)
	}
	b.WriteString(message)

	return b.String()
}

// highlightText highlights occurrences of a search term in text
func (m *Model) highlightText(text, search string) string {
	if search == "" {
		return text
	}

	lower := strings.ToLower(text)
	lowerSearch := strings.ToLower(search)

	var result strings.Builder
	lastIdx := 0

	for {
		idx := strings.Index(lower[lastIdx:], lowerSearch)
		if idx == -1 {
			result.WriteString(text[lastIdx:])
			break
		}

		idx += lastIdx
		result.WriteString(text[lastIdx:idx])
		result.WriteString(HighlightStyle.Render(text[idx : idx+len(search)]))
		lastIdx = idx + len(search)
	}

	return result.String()
}

// renderServiceHint renders the available services hint when in filter mode
func (m *Model) renderServiceHint() string {
	services := m.serviceTracker.Services()
	if len(services) == 0 {
		return HelpStyle.Render("No services discovered yet. Press Tab to autocomplete.")
	}

	// Show available services with Tab hint
	hint := "Available: " + m.serviceTracker.FormatServiceList() + "  (Tab to autocomplete)"
	return HelpStyle.Render(hint)
}

// renderErrorPanel renders the error summary panel
func (m *Model) renderErrorPanel() string {
	summary := m.errorStats.Summary()
	totalErrors := m.errorStats.TotalErrors()
	totalWarnings := m.errorStats.TotalWarnings()

	if totalErrors == 0 && totalWarnings == 0 {
		return HelpStyle.Render("ERRORS (5m): none")
	}

	// Show top services with errors/warnings (limit to fit width)
	maxServices := 4
	parts := make([]string, 0, maxServices+1)
	for i, sc := range summary {
		if i >= maxServices {
			break
		}
		part := fmt.Sprintf("%s: %s %s",
			sc.Service,
			ErrorCountStyle.Render(fmt.Sprintf("%dE", sc.Errors)),
			WarnCountStyle.Render(fmt.Sprintf("%dW", sc.Warnings)))
		parts = append(parts, part)
	}

	// Add total
	total := fmt.Sprintf("Total: %s %s",
		ErrorCountStyle.Render(fmt.Sprintf("%dE", totalErrors)),
		WarnCountStyle.Render(fmt.Sprintf("%dW", totalWarnings)))
	parts = append(parts, total)

	content := ErrorPanelTitleStyle.Render("ERRORS (5m): ") + strings.Join(parts, " | ")
	return content
}

// renderHelpView renders the full help screen
func (m *Model) renderHelpView() string {
	var b strings.Builder

	b.WriteString(TitleStyle.Render("TERANODE LOG VIEWER HELP"))
	b.WriteString("\n\n")
	b.WriteString(m.keys.FullHelp())
	b.WriteString("\n\n")
	b.WriteString(HelpStyle.Render("Press ? or Esc to close help"))

	return b.String()
}
