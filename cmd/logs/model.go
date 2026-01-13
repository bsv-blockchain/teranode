package logs

import (
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

// ViewMode represents the current UI mode
type ViewMode int

const (
	ModeNormal ViewMode = iota
	ModeSearch
	ModeFilter
	ModeTxSearch
	ModeHelp
)

// Model represents the TUI state
type Model struct {
	// Data
	buffer         *RingBuffer
	watcher        *FileWatcher
	filter         *Filter
	filteredLogs   []LogEntry
	serviceTracker *ServiceTracker
	errorStats     *ErrorStats
	rateTracker    *RateTracker

	// UI Components
	viewport    viewport.Model
	searchInput textinput.Model
	filterInput textinput.Model
	txInput     textinput.Model

	// State
	mode            ViewMode
	paused          bool
	autoScroll      bool
	ready           bool
	quitting        bool
	mouseEnabled    bool     // When false, allows text selection for copy
	showErrorPanel  bool     // Toggle error summary panel
	showRateGraph   bool     // Toggle rate sparkline in header
	suggestionIndex int      // Current suggestion index for tab completion
	suggestions     []string // Current suggestions for tab completion

	// Dimensions
	width  int
	height int

	// Messages
	lastError string
	logFile   string

	// Key bindings
	keys KeyMap
}

// Message types
type newEntriesMsg []LogEntry
type tickMsg time.Time
type errMsg error

// NewModel creates a new log viewer model
func NewModel(logFile string, bufferSize int) (*Model, error) {
	// Create file watcher
	watcher, err := NewFileWatcher(logFile)
	if err != nil {
		return nil, errors.NewError("failed to create file watcher", err)
	}

	// Create text inputs
	searchInput := textinput.New()
	searchInput.Placeholder = "Search..."
	searchInput.Prompt = "/ "
	searchInput.PromptStyle = InputPromptStyle
	searchInput.TextStyle = InputStyle

	filterInput := textinput.New()
	filterInput.Placeholder = "p2p,bchn,validator..."
	filterInput.Prompt = "Services: "
	filterInput.PromptStyle = InputPromptStyle
	filterInput.TextStyle = InputStyle

	txInput := textinput.New()
	txInput.Placeholder = "64-character transaction ID"
	txInput.Prompt = "TxID: "
	txInput.PromptStyle = InputPromptStyle
	txInput.TextStyle = InputStyle
	txInput.CharLimit = 64

	return &Model{
		buffer:         NewRingBuffer(bufferSize),
		watcher:        watcher,
		filter:         NewFilter(),
		filteredLogs:   []LogEntry{},
		serviceTracker: NewServiceTracker(),
		errorStats:     NewErrorStats(5 * time.Minute), // 5-minute window
		rateTracker:    NewRateTracker(30),             // 30-second history
		searchInput:    searchInput,
		filterInput:    filterInput,
		txInput:        txInput,
		mode:           ModeNormal,
		paused:         false,
		autoScroll:     true,
		mouseEnabled:   true,
		showErrorPanel: false,
		showRateGraph:  true, // Show rate graph by default
		keys:           DefaultKeyMap,
		logFile:        logFile,
	}, nil
}

// Init implements tea.Model
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.startWatcher(),
		m.listenForEntries(),
		tickCmd(),
	)
}

// startWatcher starts the file watcher
func (m *Model) startWatcher() tea.Cmd {
	return func() tea.Msg {
		// Start watching with last 1000 lines
		if err := m.watcher.Start(1000); err != nil {
			return errMsg(err)
		}
		return nil
	}
}

// listenForEntries listens for new log entries from the file watcher
func (m *Model) listenForEntries() tea.Cmd {
	return func() tea.Msg {
		entries := <-m.watcher.Entries()
		return newEntriesMsg(entries)
	}
}

// tickCmd returns a command that ticks periodically
func tickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// Update implements tea.Model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Handle key presses based on current mode
		cmd := m.handleKeyPress(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateViewport()
		m.ready = true

	case newEntriesMsg:
		// Add new entries to buffer and track services/stats
		entries := []LogEntry(msg)
		m.buffer.PushMany(entries)
		m.serviceTracker.TrackMany(entries)
		m.errorStats.TrackMany(entries)
		m.rateTracker.Add(len(entries))
		m.updateFilteredLogs()

		// Auto-scroll to bottom if not paused
		if m.autoScroll && !m.paused {
			m.viewport.GotoBottom()
		}

		// Continue listening for entries
		cmds = append(cmds, m.listenForEntries())

	case tickMsg:
		// Periodic refresh and rate tracking
		m.rateTracker.Tick()
		cmds = append(cmds, tickCmd())

	case errMsg:
		m.lastError = msg.Error()
	}

	// Update viewport
	if m.mode == ModeNormal {
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

// handleKeyPress handles keyboard input based on current mode
func (m *Model) handleKeyPress(msg tea.KeyMsg) tea.Cmd {
	switch m.mode {
	case ModeSearch:
		return m.handleSearchMode(msg)
	case ModeFilter:
		return m.handleFilterMode(msg)
	case ModeTxSearch:
		return m.handleTxSearchMode(msg)
	case ModeHelp:
		return m.handleHelpMode(msg)
	default:
		return m.handleNormalMode(msg)
	}
}

// handleNormalMode handles key presses in normal mode
func (m *Model) handleNormalMode(msg tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.Quit):
		m.quitting = true
		m.watcher.Stop()
		return tea.Quit

	case key.Matches(msg, m.keys.Search):
		m.mode = ModeSearch
		m.searchInput.Focus()
		return textinput.Blink

	case key.Matches(msg, m.keys.Filter):
		m.mode = ModeFilter
		m.filterInput.SetValue(m.filter.GetServicesString())
		m.filterInput.Focus()
		return textinput.Blink

	case key.Matches(msg, m.keys.TxSearch):
		m.mode = ModeTxSearch
		m.txInput.Focus()
		return textinput.Blink

	case key.Matches(msg, m.keys.Help):
		m.mode = ModeHelp

	case key.Matches(msg, m.keys.Pause):
		m.paused = !m.paused
		if !m.paused {
			m.autoScroll = true
			m.viewport.GotoBottom()
		}

	case key.Matches(msg, m.keys.ToggleMouse):
		m.mouseEnabled = !m.mouseEnabled
		if m.mouseEnabled {
			return tea.EnableMouseCellMotion
		}
		return tea.DisableMouse

	case key.Matches(msg, m.keys.ErrorPanel):
		m.showErrorPanel = !m.showErrorPanel
		m.updateViewport()

	case key.Matches(msg, m.keys.RateGraph):
		m.showRateGraph = !m.showRateGraph

	case key.Matches(msg, m.keys.ClearFilter):
		m.filter.Clear()
		m.searchInput.SetValue("")
		m.filterInput.SetValue("")
		m.txInput.SetValue("")
		m.updateFilteredLogs()

	case key.Matches(msg, m.keys.LevelUp):
		m.filter.IncreaseLevel()
		m.updateFilteredLogs()

	case key.Matches(msg, m.keys.LevelDown):
		m.filter.DecreaseLevel()
		m.updateFilteredLogs()

	case key.Matches(msg, m.keys.Top):
		m.viewport.GotoTop()
		m.autoScroll = false

	case key.Matches(msg, m.keys.Bottom):
		m.viewport.GotoBottom()
		m.autoScroll = true

	case key.Matches(msg, m.keys.Up):
		m.viewport.LineUp(1)
		m.autoScroll = false

	case key.Matches(msg, m.keys.Down):
		m.viewport.LineDown(1)

	case key.Matches(msg, m.keys.PageUp):
		m.viewport.ViewUp()
		m.autoScroll = false

	case key.Matches(msg, m.keys.PageDown):
		m.viewport.ViewDown()

	case key.Matches(msg, m.keys.HalfPageUp):
		m.viewport.HalfViewUp()
		m.autoScroll = false

	case key.Matches(msg, m.keys.HalfPageDown):
		m.viewport.HalfViewDown()
	}

	return nil
}

// handleSearchMode handles key presses in search mode
func (m *Model) handleSearchMode(msg tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.Escape):
		m.mode = ModeNormal
		m.searchInput.Blur()
		return nil

	case key.Matches(msg, m.keys.Enter):
		m.filter.SetSearch(m.searchInput.Value())
		m.updateFilteredLogs()
		m.mode = ModeNormal
		m.searchInput.Blur()
		return nil
	}

	var cmd tea.Cmd
	m.searchInput, cmd = m.searchInput.Update(msg)
	return cmd
}

// handleFilterMode handles key presses in filter mode
func (m *Model) handleFilterMode(msg tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.Escape):
		m.mode = ModeNormal
		m.filterInput.Blur()
		m.suggestions = nil
		m.suggestionIndex = 0
		return nil

	case key.Matches(msg, m.keys.Enter):
		m.filter.SetServices(m.filterInput.Value())
		m.updateFilteredLogs()
		m.mode = ModeNormal
		m.filterInput.Blur()
		m.suggestions = nil
		m.suggestionIndex = 0
		return nil

	case key.Matches(msg, m.keys.Tab):
		// Tab completion for services
		m.handleServiceTabComplete()
		return nil
	}

	// Reset suggestions when typing (not tab)
	m.suggestions = nil
	m.suggestionIndex = 0

	var cmd tea.Cmd
	m.filterInput, cmd = m.filterInput.Update(msg)
	return cmd
}

// handleServiceTabComplete handles tab completion for service names
func (m *Model) handleServiceTabComplete() {
	value := m.filterInput.Value()

	// Get the current word being typed (after last comma)
	lastComma := -1
	for i := len(value) - 1; i >= 0; i-- {
		if value[i] == ',' {
			lastComma = i
			break
		}
	}

	prefix := ""
	baseValue := ""
	if lastComma >= 0 {
		baseValue = value[:lastComma+1]
		prefix = value[lastComma+1:]
	} else {
		prefix = value
	}

	// Get suggestions if we don't have any, or cycle through existing ones
	if m.suggestions == nil || len(m.suggestions) == 0 {
		m.suggestions = m.serviceTracker.Suggest(prefix)
		m.suggestionIndex = 0
	} else {
		// Cycle to next suggestion
		m.suggestionIndex = (m.suggestionIndex + 1) % len(m.suggestions)
	}

	// Apply the suggestion
	if len(m.suggestions) > 0 {
		newValue := baseValue + m.suggestions[m.suggestionIndex]
		m.filterInput.SetValue(newValue)
		m.filterInput.SetCursor(len(newValue))
	}
}

// handleTxSearchMode handles key presses in transaction search mode
func (m *Model) handleTxSearchMode(msg tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.Escape):
		m.mode = ModeNormal
		m.txInput.Blur()
		return nil

	case key.Matches(msg, m.keys.Enter):
		m.filter.SetTxID(m.txInput.Value())
		m.updateFilteredLogs()
		m.mode = ModeNormal
		m.txInput.Blur()
		return nil
	}

	var cmd tea.Cmd
	m.txInput, cmd = m.txInput.Update(msg)
	return cmd
}

// handleHelpMode handles key presses in help mode
func (m *Model) handleHelpMode(msg tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.Escape), key.Matches(msg, m.keys.Help), key.Matches(msg, m.keys.Quit):
		m.mode = ModeNormal
	}
	return nil
}

// updateViewport recalculates viewport dimensions
func (m *Model) updateViewport() {
	headerHeight := 1
	statusHeight := 2

	// Account for error panel if visible
	errorPanelHeight := 0
	if m.showErrorPanel {
		errorPanelHeight = 3 // border + content + border
	}

	viewportHeight := m.height - headerHeight - statusHeight - errorPanelHeight
	if viewportHeight < 1 {
		viewportHeight = 1
	}

	if !m.ready {
		m.viewport = viewport.New(m.width, viewportHeight)
		m.viewport.YPosition = headerHeight
	} else {
		m.viewport.Width = m.width
		m.viewport.Height = viewportHeight
	}

	m.updateFilteredLogs()
}

// updateFilteredLogs applies the current filter and updates the viewport content
func (m *Model) updateFilteredLogs() {
	entries := m.buffer.Entries()
	m.filteredLogs = m.filter.FilterEntries(entries)
	m.viewport.SetContent(m.renderLogs())
}

// View implements tea.Model
func (m Model) View() string {
	if m.quitting {
		return ""
	}

	if !m.ready {
		return "Loading..."
	}

	return m.renderView()
}
