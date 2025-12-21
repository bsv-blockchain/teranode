package logs

import (
	"github.com/charmbracelet/lipgloss"
)

// Color palette
var (
	// Log level colors (matching zerologger.go patterns)
	colorDebug = lipgloss.Color("241") // Gray
	colorInfo  = lipgloss.Color("39")  // Blue
	colorWarn  = lipgloss.Color("214") // Yellow/Orange
	colorError = lipgloss.Color("196") // Red
	colorFatal = lipgloss.Color("201") // Magenta

	// UI colors
	colorTitle      = lipgloss.Color("205") // Pink
	colorBorder     = lipgloss.Color("62")  // Purple
	colorSubtle     = lipgloss.Color("241") // Gray
	colorHighlight  = lipgloss.Color("227") // Yellow (for search matches)
	colorStatusBar  = lipgloss.Color("236") // Dark gray
	colorStatusText = lipgloss.Color("252") // Light gray
)

// Log level styles
var (
	DebugStyle = lipgloss.NewStyle().Foreground(colorDebug)
	InfoStyle  = lipgloss.NewStyle().Foreground(colorInfo)
	WarnStyle  = lipgloss.NewStyle().Foreground(colorWarn)
	ErrorStyle = lipgloss.NewStyle().Foreground(colorError)
	FatalStyle = lipgloss.NewStyle().Foreground(colorFatal)
)

// UI element styles
var (
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorTitle)

	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorTitle).
			Background(lipgloss.Color("235")).
			Padding(0, 1)

	BorderStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder)

	StatusBarStyle = lipgloss.NewStyle().
			Background(colorStatusBar).
			Foreground(colorStatusText).
			Padding(0, 1)

	HelpStyle = lipgloss.NewStyle().
			Foreground(colorSubtle)

	HelpKeyStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205")).
			Bold(true)

	HelpDescStyle = lipgloss.NewStyle().
			Foreground(colorSubtle)

	HighlightStyle = lipgloss.NewStyle().
			Background(colorHighlight).
			Foreground(lipgloss.Color("0"))

	PausedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorWarn)

	ActiveStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("42")) // Green

	InputStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205"))

	InputPromptStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("205"))

	TimestampStyle = lipgloss.NewStyle().
			Foreground(colorSubtle)

	ServiceStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("99")) // Purple

	CallerStyle = lipgloss.NewStyle().
			Foreground(colorSubtle)

	// Error panel styles
	ErrorPanelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorError).
			Padding(0, 1)

	ErrorPanelTitleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorError)

	ErrorCountStyle = lipgloss.NewStyle().
			Foreground(colorError).
			Bold(true)

	WarnCountStyle = lipgloss.NewStyle().
			Foreground(colorWarn).
			Bold(true)

	// Rate graph styles
	SparklineStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")) // Blue

	RateStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42")) // Green
)

// GetLevelStyle returns the appropriate style for a log level
func GetLevelStyle(level LogLevel) lipgloss.Style {
	switch level {
	case LevelDebug:
		return DebugStyle
	case LevelInfo:
		return InfoStyle
	case LevelWarn:
		return WarnStyle
	case LevelError:
		return ErrorStyle
	case LevelFatal:
		return FatalStyle
	default:
		return lipgloss.NewStyle()
	}
}

// Service-specific colors for visual distinction
var serviceColors = map[string]lipgloss.Color{
	"p2p":       lipgloss.Color("42"),  // Green
	"bchn":      lipgloss.Color("99"),  // Purple
	"validator": lipgloss.Color("39"),  // Blue
	"valid":     lipgloss.Color("39"),  // Blue (short form)
	"prop":      lipgloss.Color("214"), // Orange
	"ba":        lipgloss.Color("205"), // Pink
	"rpc":       lipgloss.Color("117"), // Cyan
	"asset":     lipgloss.Color("220"), // Gold
	"alert":     lipgloss.Color("196"), // Red
	"pruner":    lipgloss.Color("141"), // Light purple
	"legacy":    lipgloss.Color("245"), // Gray
}

// GetServiceStyle returns a style for a specific service
func GetServiceStyle(service string) lipgloss.Style {
	if color, ok := serviceColors[service]; ok {
		return lipgloss.NewStyle().Foreground(color)
	}
	return ServiceStyle
}
