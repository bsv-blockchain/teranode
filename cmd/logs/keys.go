package logs

import (
	"github.com/charmbracelet/bubbles/key"
)

// KeyMap defines all keyboard bindings for the log viewer
type KeyMap struct {
	Quit         key.Binding
	Up           key.Binding
	Down         key.Binding
	PageUp       key.Binding
	PageDown     key.Binding
	HalfPageUp   key.Binding
	HalfPageDown key.Binding
	Top          key.Binding
	Bottom       key.Binding
	Search       key.Binding
	Filter       key.Binding
	TxSearch     key.Binding
	ClearFilter  key.Binding
	Pause        key.Binding
	Help         key.Binding
	LevelUp      key.Binding
	LevelDown    key.Binding
	Escape       key.Binding
	Enter        key.Binding
	Tab          key.Binding
	ToggleMouse  key.Binding
	ErrorPanel   key.Binding
	RateGraph    key.Binding
}

// DefaultKeyMap returns the default key bindings
var DefaultKeyMap = KeyMap{
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	),
	Up: key.NewBinding(
		key.WithKeys("up", "k"),
		key.WithHelp("k/up", "up"),
	),
	Down: key.NewBinding(
		key.WithKeys("down", "j"),
		key.WithHelp("j/down", "down"),
	),
	PageUp: key.NewBinding(
		key.WithKeys("pgup", "b"),
		key.WithHelp("pgup", "page up"),
	),
	PageDown: key.NewBinding(
		key.WithKeys("pgdown", "f"),
		key.WithHelp("pgdn", "page down"),
	),
	HalfPageUp: key.NewBinding(
		key.WithKeys("ctrl+u"),
		key.WithHelp("ctrl+u", "half page up"),
	),
	HalfPageDown: key.NewBinding(
		key.WithKeys("ctrl+d"),
		key.WithHelp("ctrl+d", "half page down"),
	),
	Top: key.NewBinding(
		key.WithKeys("home", "g"),
		key.WithHelp("g", "top"),
	),
	Bottom: key.NewBinding(
		key.WithKeys("end", "G"),
		key.WithHelp("G", "bottom"),
	),
	Search: key.NewBinding(
		key.WithKeys("/"),
		key.WithHelp("/", "search"),
	),
	Filter: key.NewBinding(
		key.WithKeys("s"),
		key.WithHelp("s", "services"),
	),
	TxSearch: key.NewBinding(
		key.WithKeys("t"),
		key.WithHelp("t", "txid"),
	),
	ClearFilter: key.NewBinding(
		key.WithKeys("c"),
		key.WithHelp("c", "clear"),
	),
	Pause: key.NewBinding(
		key.WithKeys("p", " "),
		key.WithHelp("p", "pause"),
	),
	Help: key.NewBinding(
		key.WithKeys("?"),
		key.WithHelp("?", "help"),
	),
	LevelUp: key.NewBinding(
		key.WithKeys("+", "="),
		key.WithHelp("+", "level+"),
	),
	LevelDown: key.NewBinding(
		key.WithKeys("-"),
		key.WithHelp("-", "level-"),
	),
	Escape: key.NewBinding(
		key.WithKeys("esc"),
		key.WithHelp("esc", "cancel"),
	),
	Enter: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("enter", "confirm"),
	),
	Tab: key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "autocomplete"),
	),
	ToggleMouse: key.NewBinding(
		key.WithKeys("m"),
		key.WithHelp("m", "mouse/select"),
	),
	ErrorPanel: key.NewBinding(
		key.WithKeys("e"),
		key.WithHelp("e", "errors"),
	),
	RateGraph: key.NewBinding(
		key.WithKeys("r"),
		key.WithHelp("r", "rate"),
	),
}

// ShortHelp returns a condensed help string for the status bar
func (k KeyMap) ShortHelp() string {
	return HelpKeyStyle.Render("q") + HelpDescStyle.Render(":quit ") +
		HelpKeyStyle.Render("/") + HelpDescStyle.Render(":search ") +
		HelpKeyStyle.Render("s") + HelpDescStyle.Render(":services ") +
		HelpKeyStyle.Render("t") + HelpDescStyle.Render(":txid ") +
		HelpKeyStyle.Render("e") + HelpDescStyle.Render(":errors ") +
		HelpKeyStyle.Render("r") + HelpDescStyle.Render(":rate ") +
		HelpKeyStyle.Render("p") + HelpDescStyle.Render(":pause ") +
		HelpKeyStyle.Render("?") + HelpDescStyle.Render(":help")
}

// FullHelp returns a full help screen
func (k KeyMap) FullHelp() string {
	return `
  Navigation
    j/k, up/down     Scroll up/down
    g/G, home/end    Go to top/bottom
    pgup/pgdn        Page up/down
    ctrl+u/ctrl+d    Half page up/down

  Filtering
    /                Search in logs
    s                Filter by services
    t                Search by transaction ID
    +/-              Increase/decrease min log level
    c                Clear all filters

  Stats & Monitoring
    e                Toggle error summary panel (errors/warnings by service)
    r                Toggle rate graph (logs/second sparkline)

  Controls
    p, space         Pause/resume auto-scroll
    m                Toggle mouse mode (off = text selection for copy)
    ?                Toggle this help
    q, ctrl+c        Quit

  Tips
    - When paused, new logs still accumulate but won't auto-scroll
    - Service filter accepts comma-separated names (e.g., "p2p,bchn")
    - Press Tab in service filter for autocomplete (cycles through matches)
    - TxID search highlights all logs containing the transaction ID
    - Level filter: DEBUG < INFO < WARN < ERROR < FATAL
    - Error panel shows errors/warnings from the last 5 minutes
    - Rate graph shows log frequency over the last 30 seconds
`
}
