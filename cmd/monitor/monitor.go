// Package monitor provides a TUI (Text User Interface) dashboard for monitoring Teranode node status.
// It displays real-time information about blockchain state, connected peers, FSM status, and service health.
package monitor

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ViewMode represents the current view
type ViewMode int

const (
	ViewDashboard ViewMode = iota
	ViewSettings
	ViewHealth
	ViewAerospike
)

// Styles for the TUI
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			MarginBottom(1)

	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1)

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255"))

	goodStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42"))

	warnStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("214"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("99")).
			MarginBottom(1)

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241")).
			MarginTop(1)

	settingKeyStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")).
			Bold(true)

	settingValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("255"))

	settingSectionStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("205")).
				MarginTop(1)
)

// ServiceHealth holds health status for a single service
type ServiceHealth struct {
	Name       string
	Configured bool
	Healthy    bool
	StatusCode int
	Message    string
	Latency    time.Duration
}

// AerospikeStats holds collected Aerospike metrics
type AerospikeStats struct {
	Connected   bool
	ClusterName string
	Nodes       []nodeInfo
	Namespaces  map[string]map[string]string // namespace -> stats map
	Latencies   map[string]string            // latency histograms
}

// nodeInfo holds per-node Aerospike statistics
type nodeInfo struct {
	Name       string
	Host       string
	Stats      map[string]string
	Namespaces map[string]map[string]string
}

// NodeData holds the fetched node data
type NodeData struct {
	BlockStats     *model.BlockStats
	FSMState       *blockchain.FSMStateType
	Peers          []*p2p.PeerInfo
	BlockchainOK   bool
	P2POK          bool
	ServiceHealth  map[string]*ServiceHealth
	AerospikeStats *AerospikeStats
	LastUpdated    time.Time
	Error          string
}

// Model represents the TUI state
type Model struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	p2pClient        p2p.ClientI
	validatorClient  validator.Interface
	blockValClient   blockvalidation.Interface
	blockAsmClient   blockassembly.ClientI
	subtreeClient    subtreevalidation.Interface
	aerospikeClient  *aerospike.Client
	spinner          spinner.Model
	data             NodeData
	width            int
	height           int
	refreshInterval  time.Duration
	quitting         bool
	viewMode         ViewMode
	settingsScroll   int // scroll offset for settings view
	aerospikeScroll  int // scroll offset for aerospike view
}

// Messages
type tickMsg time.Time
type dataMsg NodeData

// NewModel creates a new TUI model
func NewModel(logger ulogger.Logger, s *settings.Settings) (*Model, error) {
	ctx := context.Background()

	// Create blockchain client
	blockchainClient, err := blockchain.NewClient(ctx, logger, s, "TUI Monitor")
	if err != nil {
		return nil, errors.NewProcessingError("failed to create blockchain client", err)
	}

	// Create P2P client
	p2pClient, err := p2p.NewClient(ctx, logger, s)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create p2p client", err)
	}

	// Create optional service clients (don't fail if not configured)
	var validatorClient validator.Interface
	if s.Validator.GRPCAddress != "" {
		validatorClient, _ = validator.NewClient(ctx, logger, s)
	}

	var blockValClient blockvalidation.Interface
	if s.BlockValidation.GRPCAddress != "" {
		blockValClient, _ = blockvalidation.NewClient(ctx, logger, s, "TUI Monitor")
	}

	var blockAsmClient blockassembly.ClientI
	if s.BlockAssembly.GRPCAddress != "" {
		blockAsmClient, _ = blockassembly.NewClient(ctx, logger, s)
	}

	var subtreeClient subtreevalidation.Interface
	if s.SubtreeValidation.GRPCAddress != "" {
		subtreeClient, _ = subtreevalidation.NewClient(ctx, logger, s, "TUI Monitor")
	}

	// Create optional Aerospike client (don't fail if not configured)
	var aerospikeClient *aerospike.Client
	if s.Aerospike.Host != "" && s.Aerospike.Port > 0 {
		aerospikeClient, _ = aerospike.NewClient(s.Aerospike.Host, s.Aerospike.Port)
	}

	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return &Model{
		logger:           logger,
		settings:         s,
		blockchainClient: blockchainClient,
		p2pClient:        p2pClient,
		validatorClient:  validatorClient,
		blockValClient:   blockValClient,
		blockAsmClient:   blockAsmClient,
		subtreeClient:    subtreeClient,
		aerospikeClient:  aerospikeClient,
		spinner:          sp,
		refreshInterval:  2 * time.Second,
		width:            80,
		height:           24,
	}, nil
}

// Init initializes the TUI
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		m.fetchData,
		m.tick(),
	)
}

// tick returns a command that sends a tick message after the refresh interval
func (m Model) tick() tea.Cmd {
	return tea.Tick(m.refreshInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// fetchData fetches data from the node services
func (m Model) fetchData() tea.Msg {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := NodeData{
		LastUpdated:   time.Now(),
		ServiceHealth: make(map[string]*ServiceHealth),
	}

	// Fetch blockchain stats
	stats, err := m.blockchainClient.GetBlockStats(ctx)
	if err != nil {
		data.Error = fmt.Sprintf("blockchain: %v", err)
	} else {
		data.BlockStats = stats
		data.BlockchainOK = true
	}

	// Fetch FSM state
	fsmState, err := m.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		if data.Error != "" {
			data.Error += "; "
		}
		data.Error += fmt.Sprintf("fsm: %v", err)
	} else {
		data.FSMState = fsmState
	}

	// Fetch peers
	peers, err := m.p2pClient.GetPeers(ctx)
	if err != nil {
		if data.Error != "" {
			data.Error += "; "
		}
		data.Error += fmt.Sprintf("p2p: %v", err)
	} else {
		data.Peers = peers
		data.P2POK = true
	}

	// Collect service health
	m.collectServiceHealth(ctx, &data)

	// Collect Aerospike stats
	data.AerospikeStats = m.collectAerospikeStats()

	return dataMsg(data)
}

// collectServiceHealth checks health of all configured services
func (m Model) collectServiceHealth(ctx context.Context, data *NodeData) {
	// Blockchain health (use existing check)
	data.ServiceHealth["blockchain"] = &ServiceHealth{
		Name:       "Blockchain",
		Configured: true,
		Healthy:    data.BlockchainOK,
		StatusCode: 200,
		Message:    "OK",
	}
	if !data.BlockchainOK {
		data.ServiceHealth["blockchain"].StatusCode = 503
		data.ServiceHealth["blockchain"].Message = "Connection failed"
	}

	// P2P health (use existing check)
	data.ServiceHealth["p2p"] = &ServiceHealth{
		Name:       "P2P",
		Configured: true,
		Healthy:    data.P2POK,
		StatusCode: 200,
		Message:    fmt.Sprintf("%d peers connected", len(data.Peers)),
	}
	if !data.P2POK {
		data.ServiceHealth["p2p"].StatusCode = 503
		data.ServiceHealth["p2p"].Message = "Connection failed"
	}

	// Validator health
	if m.validatorClient != nil {
		health := m.checkServiceHealth(ctx, "Validator", func(ctx context.Context) (int, string, error) {
			return m.validatorClient.Health(ctx, true)
		})
		data.ServiceHealth["validator"] = health
	} else {
		data.ServiceHealth["validator"] = &ServiceHealth{
			Name:       "Validator",
			Configured: false,
			Message:    "Not configured",
		}
	}

	// Block Validation health
	if m.blockValClient != nil {
		health := m.checkServiceHealth(ctx, "Block Validation", func(ctx context.Context) (int, string, error) {
			return m.blockValClient.Health(ctx, true)
		})
		data.ServiceHealth["blockvalidation"] = health
	} else {
		data.ServiceHealth["blockvalidation"] = &ServiceHealth{
			Name:       "Block Validation",
			Configured: false,
			Message:    "Not configured",
		}
	}

	// Block Assembly health
	if m.blockAsmClient != nil {
		health := m.checkServiceHealth(ctx, "Block Assembly", func(ctx context.Context) (int, string, error) {
			return m.blockAsmClient.Health(ctx, true)
		})
		data.ServiceHealth["blockassembly"] = health
	} else {
		data.ServiceHealth["blockassembly"] = &ServiceHealth{
			Name:       "Block Assembly",
			Configured: false,
			Message:    "Not configured",
		}
	}

	// Subtree Validation health
	if m.subtreeClient != nil {
		health := m.checkServiceHealth(ctx, "Subtree Validation", func(ctx context.Context) (int, string, error) {
			return m.subtreeClient.Health(ctx, true)
		})
		data.ServiceHealth["subtreevalidation"] = health
	} else {
		data.ServiceHealth["subtreevalidation"] = &ServiceHealth{
			Name:       "Subtree Validation",
			Configured: false,
			Message:    "Not configured",
		}
	}
}

// checkServiceHealth checks health of a single service with timing
func (m Model) checkServiceHealth(ctx context.Context, name string, healthFn func(context.Context) (int, string, error)) *ServiceHealth {
	start := time.Now()
	statusCode, message, err := healthFn(ctx)
	latency := time.Since(start)

	health := &ServiceHealth{
		Name:       name,
		Configured: true,
		Latency:    latency,
	}

	if err != nil {
		health.Healthy = false
		health.StatusCode = 503
		health.Message = err.Error()
	} else {
		health.StatusCode = statusCode
		health.Message = message
		health.Healthy = statusCode == 200
	}

	return health
}

// collectAerospikeStats collects statistics from Aerospike cluster
func (m Model) collectAerospikeStats() *AerospikeStats {
	stats := &AerospikeStats{
		Namespaces: make(map[string]map[string]string),
		Latencies:  make(map[string]string),
	}

	if m.aerospikeClient == nil {
		return stats
	}

	if !m.aerospikeClient.IsConnected() {
		return stats
	}

	stats.Connected = true

	// Get cluster name
	nodes := m.aerospikeClient.GetNodes()
	if len(nodes) == 0 {
		return stats
	}

	// Collect info from each node
	policy := aerospike.NewInfoPolicy()
	for _, node := range nodes {
		ni := nodeInfo{
			Name:       node.GetName(),
			Host:       node.GetHost().String(),
			Stats:      make(map[string]string),
			Namespaces: make(map[string]map[string]string),
		}

		// Get node statistics
		infoMap, err := node.RequestInfo(policy, "statistics")
		if err == nil {
			if statsStr, ok := infoMap["statistics"]; ok {
				ni.Stats = parseAerospikeInfo(statsStr)
			}
		}

		// Get cluster name from first node
		if stats.ClusterName == "" {
			if clusterInfo, err := node.RequestInfo(policy, "cluster-name"); err == nil {
				if cn, ok := clusterInfo["cluster-name"]; ok {
					stats.ClusterName = cn
				}
			}
		}

		// Get namespace information
		if nsInfo, err := node.RequestInfo(policy, "namespaces"); err == nil {
			if nsStr, ok := nsInfo["namespaces"]; ok {
				namespaces := strings.Split(nsStr, ";")
				for _, ns := range namespaces {
					if ns == "" {
						continue
					}
					// Get namespace-specific stats
					nsStatsInfo, err := node.RequestInfo(policy, "namespace/"+ns)
					if err == nil {
						if nsStatsStr, ok := nsStatsInfo["namespace/"+ns]; ok {
							nsStats := parseAerospikeInfo(nsStatsStr)
							ni.Namespaces[ns] = nsStats
							// Aggregate namespace stats
							if stats.Namespaces[ns] == nil {
								stats.Namespaces[ns] = nsStats
							}
						}
					}
				}
			}
		}

		// Get latency information - try different info commands
		latencyCommands := []string{"latencies:", "latency:"}
		for _, cmd := range latencyCommands {
			if latInfo, err := node.RequestInfo(policy, cmd); err == nil {
				for k, v := range latInfo {
					if v != "" && v != "error" {
						stats.Latencies[k] = v
					}
				}
				if len(stats.Latencies) > 0 {
					break
				}
			}
		}

		stats.Nodes = append(stats.Nodes, ni)
	}

	return stats
}

// parseAerospikeInfo parses Aerospike info string into a map
func parseAerospikeInfo(info string) map[string]string {
	result := make(map[string]string)
	pairs := strings.Split(info, ";")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			result[kv[0]] = kv[1]
		}
	}
	return result
}

// Update handles messages and updates the model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		case "esc":
			if m.viewMode == ViewSettings || m.viewMode == ViewHealth || m.viewMode == ViewAerospike {
				m.viewMode = ViewDashboard
				return m, nil
			}
			m.quitting = true
			return m, tea.Quit
		case "r":
			return m, m.fetchData
		case "s":
			if m.viewMode == ViewSettings {
				m.viewMode = ViewDashboard
			} else {
				m.viewMode = ViewSettings
				m.settingsScroll = 0
			}
			return m, nil
		case "h":
			if m.viewMode == ViewHealth {
				m.viewMode = ViewDashboard
			} else {
				m.viewMode = ViewHealth
			}
			return m, nil
		case "a":
			if m.viewMode == ViewAerospike {
				m.viewMode = ViewDashboard
			} else {
				m.viewMode = ViewAerospike
				m.aerospikeScroll = 0
			}
			return m, nil
		case "j", "down":
			if m.viewMode == ViewSettings {
				m.settingsScroll++
			} else if m.viewMode == ViewAerospike {
				m.aerospikeScroll++
			}
		case "k", "up":
			if m.viewMode == ViewSettings && m.settingsScroll > 0 {
				m.settingsScroll--
			} else if m.viewMode == ViewAerospike && m.aerospikeScroll > 0 {
				m.aerospikeScroll--
			}
		case "g", "home":
			if m.viewMode == ViewSettings {
				m.settingsScroll = 0
			} else if m.viewMode == ViewAerospike {
				m.aerospikeScroll = 0
			}
		case "G", "end":
			if m.viewMode == ViewSettings {
				m.settingsScroll = 1000 // will be capped in render
			} else if m.viewMode == ViewAerospike {
				m.aerospikeScroll = 1000 // will be capped in render
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		return m, tea.Batch(m.fetchData, m.tick())

	case dataMsg:
		m.data = NodeData(msg)
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

// View renders the TUI
func (m Model) View() string {
	if m.quitting {
		return "Goodbye!\n"
	}

	if m.viewMode == ViewSettings {
		return m.renderSettingsView()
	}

	if m.viewMode == ViewHealth {
		return m.renderHealthView()
	}

	if m.viewMode == ViewAerospike {
		return m.renderAerospikeView()
	}

	var b strings.Builder

	// Title
	title := titleStyle.Render("TERANODE MONITOR")
	b.WriteString(title)
	b.WriteString("\n\n")

	// Create panels
	blockchainPanel := m.renderBlockchainPanel()
	fsmPanel := m.renderFSMPanel()
	peersPanel := m.renderPeersPanel()

	// Arrange panels side by side if there's enough width
	if m.width >= 100 {
		leftColumn := lipgloss.JoinVertical(lipgloss.Left, blockchainPanel, fsmPanel)
		rightColumn := peersPanel
		b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, leftColumn, "  ", rightColumn))
	} else {
		b.WriteString(blockchainPanel)
		b.WriteString("\n")
		b.WriteString(fsmPanel)
		b.WriteString("\n")
		b.WriteString(peersPanel)
	}

	// Service health summary row
	b.WriteString("\n")
	b.WriteString(m.renderHealthSummary())

	// Error display
	if m.data.Error != "" {
		b.WriteString("\n")
		b.WriteString(errorStyle.Render("Errors: " + m.data.Error))
	}

	// Status bar
	b.WriteString("\n")
	statusLine := fmt.Sprintf("Last updated: %s", m.data.LastUpdated.Format("15:04:05"))
	b.WriteString(labelStyle.Render(statusLine))

	// Help
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("q: quit | r: refresh | s: settings | h: health | a: aerospike"))

	return b.String()
}

// renderBlockchainPanel renders the blockchain statistics panel
func (m Model) renderBlockchainPanel() string {
	var content strings.Builder

	content.WriteString(headerStyle.Render("BLOCKCHAIN"))
	content.WriteString("\n")

	if m.data.BlockStats == nil {
		content.WriteString(labelStyle.Render("Loading..."))
		return boxStyle.Width(40).Render(content.String())
	}

	stats := m.data.BlockStats

	content.WriteString(m.renderRow("Height", fmt.Sprintf("%d", stats.MaxHeight)))
	content.WriteString(m.renderRow("Blocks", fmt.Sprintf("%d", stats.BlockCount)))
	content.WriteString(m.renderRow("Transactions", formatNumber(stats.TxCount)))
	content.WriteString(m.renderRow("Avg Block Size", formatBytes(uint64(stats.AvgBlockSize))))
	content.WriteString(m.renderRow("Avg Tx/Block", fmt.Sprintf("%.1f", stats.AvgTxCountPerBlock)))

	if stats.LastBlockTime > 0 {
		lastBlockAge := time.Since(time.Unix(int64(stats.LastBlockTime), 0))
		ageStr := formatDuration(lastBlockAge)
		content.WriteString(m.renderRow("Last Block", ageStr+" ago"))
	}

	return boxStyle.Width(40).Render(content.String())
}

// renderFSMPanel renders the FSM state panel
func (m Model) renderFSMPanel() string {
	var content strings.Builder

	content.WriteString(headerStyle.Render("FSM STATE"))
	content.WriteString("\n")

	if m.data.FSMState == nil {
		content.WriteString(labelStyle.Render("Loading..."))
		return boxStyle.Width(40).Render(content.String())
	}

	state := m.data.FSMState.String()

	// Color-code the state
	var stateStyled string
	switch {
	case strings.Contains(strings.ToLower(state), "running"):
		stateStyled = goodStyle.Render(state)
	case strings.Contains(strings.ToLower(state), "idle"):
		stateStyled = warnStyle.Render(state)
	case strings.Contains(strings.ToLower(state), "catching"), strings.Contains(strings.ToLower(state), "sync"):
		stateStyled = warnStyle.Render(state + " " + m.spinner.View())
	default:
		stateStyled = valueStyle.Render(state)
	}

	content.WriteString(m.renderRow("State", stateStyled))

	// Service health indicators
	content.WriteString("\n")
	content.WriteString(labelStyle.Render("Services:"))
	content.WriteString("\n")

	if m.data.BlockchainOK {
		content.WriteString("  " + goodStyle.Render("* Blockchain"))
	} else {
		content.WriteString("  " + errorStyle.Render("* Blockchain"))
	}
	content.WriteString("\n")

	if m.data.P2POK {
		content.WriteString("  " + goodStyle.Render("* P2P"))
	} else {
		content.WriteString("  " + errorStyle.Render("* P2P"))
	}

	return boxStyle.Width(40).Render(content.String())
}

// renderPeersPanel renders the peers panel
func (m Model) renderPeersPanel() string {
	var content strings.Builder

	content.WriteString(headerStyle.Render("PEERS"))
	content.WriteString("\n")

	if m.data.Peers == nil {
		content.WriteString(labelStyle.Render("Loading..."))
		return boxStyle.Width(50).Render(content.String())
	}

	// Count connected peers
	connectedCount := 0
	for _, peer := range m.data.Peers {
		if peer.IsConnected {
			connectedCount++
		}
	}

	content.WriteString(m.renderRow("Connected", fmt.Sprintf("%d", connectedCount)))
	content.WriteString(m.renderRow("Total Known", fmt.Sprintf("%d", len(m.data.Peers))))
	content.WriteString("\n")

	// Show top peers by height
	if len(m.data.Peers) > 0 {
		content.WriteString(labelStyle.Render("Top Peers by Height:"))
		content.WriteString("\n")

		// Sort and show top 5
		shown := 0
		for _, peer := range m.data.Peers {
			if !peer.IsConnected {
				continue
			}
			if shown >= 5 {
				break
			}

			peerID := peer.ID.String()
			if len(peerID) > 12 {
				peerID = peerID[:12] + "..."
			}

			repScore := fmt.Sprintf("%.0f", peer.ReputationScore)
			var repStyled string
			if peer.ReputationScore >= 80 {
				repStyled = goodStyle.Render(repScore)
			} else if peer.ReputationScore >= 50 {
				repStyled = warnStyle.Render(repScore)
			} else {
				repStyled = errorStyle.Render(repScore)
			}

			line := fmt.Sprintf("  %s h:%d rep:%s", peerID, peer.Height, repStyled)
			content.WriteString(line)
			content.WriteString("\n")
			shown++
		}

		if connectedCount > 5 {
			content.WriteString(labelStyle.Render(fmt.Sprintf("  ... and %d more", connectedCount-5)))
		}
	}

	return boxStyle.Width(50).Render(content.String())
}

// renderRow renders a label-value row
func (m Model) renderRow(label, value string) string {
	return fmt.Sprintf("%s %s\n",
		labelStyle.Render(label+":"),
		valueStyle.Render(value))
}

// formatNumber formats a large number with commas
func formatNumber(n uint64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	remainder := len(str) % 3
	if remainder > 0 {
		result.WriteString(str[:remainder])
		if len(str) > remainder {
			result.WriteString(",")
		}
	}

	for i := remainder; i < len(str); i += 3 {
		result.WriteString(str[i : i+3])
		if i+3 < len(str) {
			result.WriteString(",")
		}
	}

	return result.String()
}

// formatBytes formats bytes to human readable format
func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats a duration to a human readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// renderSettingsView renders the settings panel
func (m Model) renderSettingsView() string {
	var b strings.Builder

	// Title
	title := titleStyle.Render("TERANODE SETTINGS")
	b.WriteString(title)
	b.WriteString("\n")
	b.WriteString(labelStyle.Render(fmt.Sprintf("Context: %s", m.settings.Context)))
	b.WriteString("\n\n")

	// Collect all settings lines
	var lines []string

	// General settings
	lines = append(lines, settingSectionStyle.Render("GENERAL"))
	lines = append(lines, m.renderSettingRow("Version", m.settings.Version))
	lines = append(lines, m.renderSettingRow("Commit", m.settings.Commit))
	lines = append(lines, m.renderSettingRow("Network", m.settings.ChainCfgParams.Name))
	lines = append(lines, m.renderSettingRow("Data Folder", m.settings.DataFolder))
	lines = append(lines, m.renderSettingRow("Log Level", m.settings.LogLevel))
	lines = append(lines, m.renderSettingRow("Pretty Logs", fmt.Sprintf("%v", m.settings.PrettyLogs)))

	// Blockchain settings
	lines = append(lines, settingSectionStyle.Render("BLOCKCHAIN"))
	lines = append(lines, m.renderSettingRow("gRPC Address", m.settings.BlockChain.GRPCAddress))
	lines = append(lines, m.renderSettingRow("gRPC Listen", m.settings.BlockChain.GRPCListenAddress))
	lines = append(lines, m.renderSettingRow("HTTP Listen", m.settings.BlockChain.HTTPListenAddress))
	if m.settings.BlockChain.StoreURL != nil {
		lines = append(lines, m.renderSettingRow("Store", m.settings.BlockChain.StoreURL.String()))
	}

	// P2P settings
	lines = append(lines, settingSectionStyle.Render("P2P"))
	lines = append(lines, m.renderSettingRow("gRPC Address", m.settings.P2P.GRPCAddress))
	lines = append(lines, m.renderSettingRow("gRPC Listen", m.settings.P2P.GRPCListenAddress))
	lines = append(lines, m.renderSettingRow("HTTP Address", m.settings.P2P.HTTPAddress))
	lines = append(lines, m.renderSettingRow("Port", fmt.Sprintf("%d", m.settings.P2P.Port)))
	lines = append(lines, m.renderSettingRow("Listen Mode", m.settings.P2P.ListenMode))
	lines = append(lines, m.renderSettingRow("DHT Mode", m.settings.P2P.DHTMode))
	if len(m.settings.P2P.BootstrapPeers) > 0 {
		lines = append(lines, m.renderSettingRow("Bootstrap Peers", fmt.Sprintf("%d configured", len(m.settings.P2P.BootstrapPeers))))
	}
	if len(m.settings.P2P.StaticPeers) > 0 {
		lines = append(lines, m.renderSettingRow("Static Peers", fmt.Sprintf("%d configured", len(m.settings.P2P.StaticPeers))))
	}

	// Validator settings
	lines = append(lines, settingSectionStyle.Render("VALIDATOR"))
	lines = append(lines, m.renderSettingRow("gRPC Address", m.settings.Validator.GRPCAddress))
	lines = append(lines, m.renderSettingRow("gRPC Listen", m.settings.Validator.GRPCListenAddress))

	// Block Assembly settings
	lines = append(lines, settingSectionStyle.Render("BLOCK ASSEMBLY"))
	lines = append(lines, m.renderSettingRow("Disabled", fmt.Sprintf("%v", m.settings.BlockAssembly.Disabled)))
	lines = append(lines, m.renderSettingRow("gRPC Address", m.settings.BlockAssembly.GRPCAddress))
	lines = append(lines, m.renderSettingRow("gRPC Listen", m.settings.BlockAssembly.GRPCListenAddress))

	// Kafka settings
	lines = append(lines, settingSectionStyle.Render("KAFKA"))
	lines = append(lines, m.renderSettingRow("Hosts", m.settings.Kafka.Hosts))
	lines = append(lines, m.renderSettingRow("Port", fmt.Sprintf("%d", m.settings.Kafka.Port)))
	lines = append(lines, m.renderSettingRow("Partitions", fmt.Sprintf("%d", m.settings.Kafka.Partitions)))

	// Aerospike settings
	lines = append(lines, settingSectionStyle.Render("AEROSPIKE"))
	lines = append(lines, m.renderSettingRow("Host", m.settings.Aerospike.Host))
	lines = append(lines, m.renderSettingRow("Port", fmt.Sprintf("%d", m.settings.Aerospike.Port)))

	// Asset settings
	lines = append(lines, settingSectionStyle.Render("ASSET"))
	lines = append(lines, m.renderSettingRow("HTTP Address", m.settings.Asset.HTTPAddress))
	lines = append(lines, m.renderSettingRow("HTTP Listen", m.settings.Asset.HTTPListenAddress))

	// Apply scroll
	visibleLines := m.height - 6 // account for header, footer, margins
	if visibleLines < 5 {
		visibleLines = 5
	}

	// Cap scroll offset
	maxScroll := len(lines) - visibleLines
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.settingsScroll > maxScroll {
		m.settingsScroll = maxScroll
	}

	// Render visible lines
	endIdx := m.settingsScroll + visibleLines
	if endIdx > len(lines) {
		endIdx = len(lines)
	}

	for i := m.settingsScroll; i < endIdx; i++ {
		b.WriteString(lines[i])
		b.WriteString("\n")
	}

	// Scroll indicator
	if len(lines) > visibleLines {
		scrollInfo := fmt.Sprintf("(%d-%d of %d)", m.settingsScroll+1, endIdx, len(lines))
		b.WriteString("\n")
		b.WriteString(labelStyle.Render(scrollInfo))
	}

	// Help
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("s/esc: back | j/k: scroll | g/G: top/bottom | q: quit"))

	return b.String()
}

// renderSettingRow renders a single setting key-value row
func (m Model) renderSettingRow(key, value string) string {
	if value == "" {
		value = labelStyle.Render("(not set)")
	} else {
		value = settingValueStyle.Render(value)
	}
	return fmt.Sprintf("  %s %s", settingKeyStyle.Render(key+":"), value)
}

// renderHealthSummary renders a compact one-line health summary for the dashboard
func (m Model) renderHealthSummary() string {
	if len(m.data.ServiceHealth) == 0 {
		return labelStyle.Render("Services: loading...")
	}

	// Define service order and short names
	services := []struct {
		key   string
		short string
	}{
		{"blockchain", "BC"},
		{"validator", "VAL"},
		{"blockvalidation", "BV"},
		{"blockassembly", "BA"},
		{"subtreevalidation", "ST"},
		{"p2p", "P2P"},
	}

	parts := make([]string, 0, len(services))
	for _, svc := range services {
		health, ok := m.data.ServiceHealth[svc.key]
		if !ok {
			continue
		}

		var status string
		if !health.Configured {
			status = labelStyle.Render(svc.short + ":○")
		} else if health.Healthy {
			status = goodStyle.Render(svc.short + ":✓")
		} else {
			status = errorStyle.Render(svc.short + ":✗")
		}
		parts = append(parts, status)
	}

	return labelStyle.Render("Services: ") + strings.Join(parts, " ")
}

// renderHealthView renders the detailed health view
func (m Model) renderHealthView() string {
	var b strings.Builder

	// Title
	title := titleStyle.Render("TERANODE SERVICE HEALTH")
	b.WriteString(title)
	b.WriteString("\n\n")

	// Header row
	header := fmt.Sprintf("  %-20s %-10s %-10s %s", "SERVICE", "STATUS", "LATENCY", "MESSAGE")
	b.WriteString(headerStyle.Render(header))
	b.WriteString("\n")
	b.WriteString(labelStyle.Render("  " + strings.Repeat("─", 70)))
	b.WriteString("\n")

	// Define service order
	serviceOrder := []string{
		"blockchain",
		"validator",
		"blockvalidation",
		"blockassembly",
		"subtreevalidation",
		"p2p",
	}

	for _, key := range serviceOrder {
		health, ok := m.data.ServiceHealth[key]
		if !ok {
			continue
		}

		// Status indicator
		var statusStr string
		if !health.Configured {
			statusStr = labelStyle.Render("○ N/A")
		} else if health.Healthy {
			statusStr = goodStyle.Render("✓ OK")
		} else {
			statusStr = errorStyle.Render("✗ DOWN")
		}

		// Latency
		var latencyStr string
		if health.Configured && health.Latency > 0 {
			latencyStr = fmt.Sprintf("%dms", health.Latency.Milliseconds())
		} else {
			latencyStr = "-"
		}

		// Message (truncate if too long)
		message := health.Message
		if len(message) > 35 {
			message = message[:32] + "..."
		}

		line := fmt.Sprintf("  %-20s %-10s %-10s %s",
			health.Name,
			statusStr,
			latencyStr,
			message)
		b.WriteString(line)
		b.WriteString("\n")
	}

	// Last checked timestamp
	b.WriteString("\n")
	b.WriteString(labelStyle.Render(fmt.Sprintf("Last checked: %s", m.data.LastUpdated.Format("15:04:05"))))

	// Help
	b.WriteString("\n\n")
	b.WriteString(helpStyle.Render("h/esc: back | r: refresh | q: quit"))

	return b.String()
}

// renderAerospikeView renders the Aerospike statistics view
func (m Model) renderAerospikeView() string {
	var b strings.Builder

	// Title
	title := titleStyle.Render("AEROSPIKE STATISTICS")
	b.WriteString(title)
	b.WriteString("\n\n")

	stats := m.data.AerospikeStats
	if stats == nil || !stats.Connected {
		if m.aerospikeClient == nil {
			b.WriteString(labelStyle.Render("Aerospike not configured"))
			b.WriteString("\n")
			b.WriteString(labelStyle.Render(fmt.Sprintf("Host: %s, Port: %d", m.settings.Aerospike.Host, m.settings.Aerospike.Port)))
		} else {
			b.WriteString(errorStyle.Render("Aerospike not connected"))
		}
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("a/esc: back | r: refresh | q: quit"))
		return b.String()
	}

	// Collect all lines for scrolling
	var lines []string

	// Cluster info
	lines = append(lines, headerStyle.Render("CLUSTER INFO"))
	lines = append(lines, m.renderSettingRow("Cluster Name", stats.ClusterName))
	lines = append(lines, m.renderSettingRow("Nodes", fmt.Sprintf("%d", len(stats.Nodes))))
	lines = append(lines, "")

	// Node summary
	for i, node := range stats.Nodes {
		lines = append(lines, headerStyle.Render(fmt.Sprintf("NODE %d: %s", i+1, node.Name)))
		lines = append(lines, m.renderSettingRow("Host", node.Host))

		// Key node stats
		keyStats := []string{
			"cluster_size", "uptime", "system_free_mem_pct",
			"client_connections", "batch_index_queue", "scan_queue",
		}
		for _, key := range keyStats {
			if val, ok := node.Stats[key]; ok {
				lines = append(lines, m.renderSettingRow(key, val))
			}
		}
		lines = append(lines, "")
	}

	// Namespace info
	for ns, nsStats := range stats.Namespaces {
		lines = append(lines, headerStyle.Render(fmt.Sprintf("NAMESPACE: %s", ns)))

		// Calculate disk usage percentage
		if deviceUsed, ok := nsStats["device_used_bytes"]; ok {
			if deviceTotal, ok := nsStats["device_total_bytes"]; ok {
				used, _ := strconv.ParseUint(deviceUsed, 10, 64)
				total, _ := strconv.ParseUint(deviceTotal, 10, 64)
				if total > 0 {
					pct := float64(used) / float64(total) * 100
					diskLine := fmt.Sprintf("%s / %s (%.1f%%)", formatBytes(used), formatBytes(total), pct)
					lines = append(lines, m.renderSettingRow("Disk Used/Total", diskLine))
				}
			}
		}

		// Key namespace metrics grouped by category
		criticalStats := []struct {
			key  string
			warn bool // if true, highlight when non-zero
		}{
			{"stop_writes", true},
			{"clock_skew_stop_writes", true},
			{"hwm_breached", true},
		}
		for _, stat := range criticalStats {
			if val, ok := nsStats[stat.key]; ok {
				if stat.warn && val != "0" && val != "false" {
					lines = append(lines, "  "+errorStyle.Render(stat.key+": "+val))
				} else {
					lines = append(lines, m.renderSettingRow(stat.key, val))
				}
			}
		}

		// Storage metrics
		storageStats := []string{
			"data_avail_pct", "memory_used_bytes", "index_used_bytes",
			"sindex_used_bytes", "cache_read_pct",
		}
		for _, key := range storageStats {
			if val, ok := nsStats[key]; ok {
				lines = append(lines, m.renderSettingRow(key, val))
			}
		}

		// Object/record metrics
		objectStats := []string{
			"objects", "tombstones", "evicted_objects", "expired_objects",
			"truncated_records",
		}
		for _, key := range objectStats {
			if val, ok := nsStats[key]; ok {
				lines = append(lines, m.renderSettingRow(key, val))
			}
		}

		// Throughput metrics
		throughputStats := []string{
			"client_read_success", "client_read_error", "client_read_timeout",
			"client_write_success", "client_write_error", "client_write_timeout",
			"batch_sub_read_success", "batch_sub_read_error",
		}
		for _, key := range throughputStats {
			if val, ok := nsStats[key]; ok {
				lines = append(lines, m.renderSettingRow(key, val))
			}
		}

		// Migration metrics
		migrationStats := []string{
			"migrate_rx_partitions_active", "migrate_tx_partitions_active",
			"migrate_rx_partitions_remaining", "migrate_tx_partitions_remaining",
		}
		for _, key := range migrationStats {
			if val, ok := nsStats[key]; ok && val != "0" {
				lines = append(lines, m.renderSettingRow(key, val))
			}
		}

		lines = append(lines, "")
	}

	// Latency info
	if len(stats.Latencies) > 0 {
		lines = append(lines, headerStyle.Render("LATENCIES"))
		// Sort latency keys for consistent display
		var latKeys []string
		for k := range stats.Latencies {
			latKeys = append(latKeys, k)
		}
		sort.Strings(latKeys)
		for _, k := range latKeys {
			v := stats.Latencies[k]
			if len(v) > 60 {
				v = v[:57] + "..."
			}
			lines = append(lines, m.renderSettingRow(k, v))
		}
		lines = append(lines, "")
	}

	// Apply scroll
	visibleLines := m.height - 6
	if visibleLines < 5 {
		visibleLines = 5
	}

	// Cap scroll offset
	maxScroll := len(lines) - visibleLines
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.aerospikeScroll > maxScroll {
		m.aerospikeScroll = maxScroll
	}

	// Render visible lines
	endIdx := m.aerospikeScroll + visibleLines
	if endIdx > len(lines) {
		endIdx = len(lines)
	}

	for i := m.aerospikeScroll; i < endIdx; i++ {
		b.WriteString(lines[i])
		b.WriteString("\n")
	}

	// Scroll indicator
	if len(lines) > visibleLines {
		scrollInfo := fmt.Sprintf("(%d-%d of %d)", m.aerospikeScroll+1, endIdx, len(lines))
		b.WriteString("\n")
		b.WriteString(labelStyle.Render(scrollInfo))
	}

	// Last updated
	b.WriteString("\n")
	b.WriteString(labelStyle.Render(fmt.Sprintf("Last updated: %s", m.data.LastUpdated.Format("15:04:05"))))

	// Help
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("a/esc: back | j/k: scroll | g/G: top/bottom | r: refresh | q: quit"))

	return b.String()
}

// Run starts the TUI monitor
func Run(logger ulogger.Logger, s *settings.Settings) error {
	m, err := NewModel(logger, s)
	if err != nil {
		return err
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
