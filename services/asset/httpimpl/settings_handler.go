package httpimpl

import (
	"net/http"
	"sort"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

// SettingsHandler handles settings-related HTTP requests.
type SettingsHandler struct {
	settings *settings.Settings
	logger   ulogger.Logger
}

// NewSettingsHandler creates a new settings handler instance.
func NewSettingsHandler(s *settings.Settings, logger ulogger.Logger) *SettingsHandler {
	return &SettingsHandler{
		settings: s,
		logger:   logger,
	}
}

// SettingsResponse represents the API response for settings.
type SettingsResponse struct {
	Settings   []settings.SettingMetadata `json:"settings"`
	Categories []string                   `json:"categories"`
	Total      int                        `json:"total"`
	Filtered   int                        `json:"filtered"`
	Version    string                     `json:"version"`
	Commit     string                     `json:"commit"`
}

// GetSettings handles GET /api/v1/settings
// Query parameters:
//   - category: Filter by category (e.g., "P2P", "Kafka")
//   - search: Search keyword in key, name, or description
//   - sort: Sort field (key, name, category) - default: category,key
//   - order: Sort order (asc, desc) - default: asc
func (h *SettingsHandler) GetSettings(c echo.Context) error {
	h.logger.Debugf("[Asset_http] GetSettings request")

	// Get query parameters
	category := c.QueryParam("category")
	search := strings.ToLower(strings.TrimSpace(c.QueryParam("search")))
	sortField := c.QueryParam("sort")
	sortOrder := c.QueryParam("order")

	// Default sort by category then key
	if sortField == "" {
		sortField = "category"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	// Export all settings with metadata
	registry := h.settings.ExportMetadata()

	// Filter settings
	filtered := make([]settings.SettingMetadata, 0, len(registry.Settings))
	for _, setting := range registry.Settings {
		// Filter by category if specified
		if category != "" && !strings.EqualFold(setting.Category, category) {
			continue
		}

		// Filter by search term if specified
		if search != "" {
			keyMatch := strings.Contains(strings.ToLower(setting.Key), search)
			nameMatch := strings.Contains(strings.ToLower(setting.Name), search)
			descMatch := strings.Contains(strings.ToLower(setting.Description), search)
			categoryMatch := strings.Contains(strings.ToLower(setting.Category), search)
			currentValMatch := strings.Contains(strings.ToLower(setting.CurrentValue), search)
			usageHintMatch := strings.Contains(strings.ToLower(setting.UsageHint), search)

			if !keyMatch && !nameMatch && !descMatch && !categoryMatch && !currentValMatch && !usageHintMatch {
				continue
			}
		}

		filtered = append(filtered, setting)
	}

	// Sort settings
	sort.Slice(filtered, func(i, j int) bool {
		var cmp int
		switch sortField {
		case "key":
			cmp = strings.Compare(strings.ToLower(filtered[i].Key), strings.ToLower(filtered[j].Key))
		case "name":
			cmp = strings.Compare(strings.ToLower(filtered[i].Name), strings.ToLower(filtered[j].Name))
		case "category":
			cmp = strings.Compare(strings.ToLower(filtered[i].Category), strings.ToLower(filtered[j].Category))
			if cmp == 0 {
				// Secondary sort by key within category
				cmp = strings.Compare(strings.ToLower(filtered[i].Key), strings.ToLower(filtered[j].Key))
			}
		default:
			// Default to category then key
			cmp = strings.Compare(strings.ToLower(filtered[i].Category), strings.ToLower(filtered[j].Category))
			if cmp == 0 {
				cmp = strings.Compare(strings.ToLower(filtered[i].Key), strings.ToLower(filtered[j].Key))
			}
		}

		if sortOrder == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})

	response := SettingsResponse{
		Settings:   filtered,
		Categories: registry.Categories,
		Total:      len(registry.Settings),
		Filtered:   len(filtered),
		Version:    registry.Version,
		Commit:     registry.Commit,
	}

	return c.JSON(http.StatusOK, response)
}

// GetSettingsCategories handles GET /api/v1/settings/categories
// Returns just the list of available categories.
func (h *SettingsHandler) GetSettingsCategories(c echo.Context) error {
	h.logger.Debugf("[Asset_http] GetSettingsCategories request")

	categories := settings.AllCategories()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"categories": categories,
	})
}
