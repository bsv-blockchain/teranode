package settings

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
)

// metadataEntry holds the static metadata extracted from struct tags.
type metadataEntry struct {
	FieldName       string
	Key             string
	Name            string
	Type            string
	DefaultValue    string
	Description     string
	LongDescription string
	Category        string
	UsageHint       string
	ValuePath       []int // Reflects field index path for nested access
}

// Package-level cache for metadata structure.
var (
	metadataCache     []metadataEntry
	metadataCacheOnce sync.Once
)

// ExportMetadata exports all settings with their metadata for the settings portal.
// It uses reflection to extract struct tags on first call (cached), then combines
// with current runtime values on each subsequent call.
func (s *Settings) ExportMetadata() *SettingsRegistry {
	// Extract metadata structure once (cached after first call)
	metadataCacheOnce.Do(func() {
		metadataCache = extractMetadataStructure()
	})

	// Build settings with current values
	settings := make([]SettingMetadata, 0, len(metadataCache)+1)
	val := reflect.ValueOf(s).Elem()

	for _, entry := range metadataCache {
		// Get current value using cached field path
		currentVal := getValueAtPath(val, entry.ValuePath)

		settings = append(settings, SettingMetadata{
			Key:             entry.Key,
			Name:            entry.Name,
			Type:            entry.Type,
			DefaultValue:    entry.DefaultValue,
			CurrentValue:    formatValue(currentVal),
			Description:     entry.Description,
			LongDescription: entry.LongDescription,
			Category:        entry.Category,
			UsageHint:       entry.UsageHint,
		})
	}

	// Add special "network" setting from ChainCfgParams
	if s.ChainCfgParams != nil {
		settings = append(settings, SettingMetadata{
			Key:             "network",
			Name:            "Network",
			Type:            "string",
			DefaultValue:    "mainnet",
			CurrentValue:    s.ChainCfgParams.Name,
			Description:     "Bitcoin network to connect to (mainnet, testnet, stn, regtest)",
			LongDescription: "Specifies which Bitcoin SV network this node connects to. Each network has different genesis blocks, address prefixes, and peer discovery. 'mainnet' is the production Bitcoin SV network with real economic value - use for mining, exchanges, and production services. 'testnet' is a public test network with worthless coins for development and testing without risking real funds. 'stn' (Scaling Test Network) is BSV's dedicated network for testing high-throughput scenarios and large blocks. 'regtest' (Regression Test) is a local private network for automated testing with instant block generation. Network selection affects: genesis block hash, magic bytes for P2P protocol, default ports, address version bytes (for legacy addresses), and peer discovery seeds. Cannot be changed at runtime - requires node restart with empty data directory to switch networks.",
			Category:        CategoryGlobal,
			UsageHint:       "Use 'mainnet' for production, 'testnet' or 'stn' for testing",
		})
	}

	return &SettingsRegistry{
		Settings:   settings,
		Categories: AllCategories(),
		Version:    s.Version,
		Commit:     s.Commit,
	}
}

// extractMetadataStructure extracts tag metadata once (expensive operation).
func extractMetadataStructure() []metadataEntry {
	var entries []metadataEntry

	// Use reflection to walk the Settings type (not instance)
	typ := reflect.TypeOf(Settings{})

	// Recursively extract all fields with tags
	extractFields(typ, nil, &entries)

	return entries
}

// extractFields recursively extracts fields with struct tags.
func extractFields(typ reflect.Type, path []int, entries *[]metadataEntry) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldPath := append(append([]int{}, path...), i)

		// Check if field has our metadata tags
		key := field.Tag.Get("key")
		if key == "" {
			// Check if it's a nested struct to recurse into
			fieldType := field.Type

			// Handle pointer to struct (e.g., *PolicySettings)
			if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
				extractFields(fieldType.Elem(), fieldPath, entries)
			} else if fieldType.Kind() == reflect.Struct {
				extractFields(fieldType, fieldPath, entries)
			}
			continue
		}

		// Extract all metadata from tags
		entry := metadataEntry{
			FieldName:       field.Name,
			Key:             key,
			Name:            field.Tag.Get("name"),
			Type:            field.Tag.Get("type"),
			DefaultValue:    field.Tag.Get("default"),
			Description:     field.Tag.Get("desc"),
			LongDescription: field.Tag.Get("longdesc"),
			Category:        field.Tag.Get("category"),
			UsageHint:       field.Tag.Get("usage"),
			ValuePath:       fieldPath,
		}

		// If Name is not provided, derive it from the field name
		if entry.Name == "" {
			entry.Name = fieldNameToDisplayName(field.Name)
		}

		*entries = append(*entries, entry)
	}
}

// fieldNameToDisplayName returns the field name as-is for the display name.
func fieldNameToDisplayName(fieldName string) string {
	return fieldName
}

// getValueAtPath retrieves a value from a reflect.Value using a field index path.
func getValueAtPath(val reflect.Value, path []int) reflect.Value {
	for _, idx := range path {
		val = val.Field(idx)
		// Dereference pointers to access nested struct fields
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				// Return invalid value for nil pointers
				return reflect.Value{}
			}
			val = val.Elem()
		}
	}
	return val
}

// formatValue formats a reflect.Value as a string for display.
func formatValue(val reflect.Value) string {
	if !val.IsValid() {
		return ""
	}

	switch val.Kind() {
	case reflect.Bool:
		return formatBool(val.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Check if it's a time.Duration
		if val.Type() == reflect.TypeOf(time.Duration(0)) {
			return formatDuration(time.Duration(val.Int()))
		}
		return formatInt(int(val.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return formatUint32(uint32(val.Uint()))
	case reflect.Float32, reflect.Float64:
		return formatFloat(val.Float())
	case reflect.String:
		return val.String()
	case reflect.Struct:
		// Special handling for url.URL struct
		if val.Type() == reflect.TypeOf(url.URL{}) {
			u := val.Interface().(url.URL)
			return u.String()
		}
		return fmt.Sprintf("%v", val.Interface())
	case reflect.Ptr:
		if val.IsNil() {
			return ""
		}
		// Special handling for *url.URL
		if val.Type() == reflect.TypeOf((*url.URL)(nil)) {
			return formatURL(val.Interface().(*url.URL))
		}
		return formatValue(val.Elem())
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.String {
			slice := make([]string, val.Len())
			for i := 0; i < val.Len(); i++ {
				slice[i] = val.Index(i).String()
			}
			return formatStringSlice(slice)
		}
		return fmt.Sprintf("[%d items]", val.Len())
	default:
		return fmt.Sprintf("%v", val.Interface())
	}
}

// Helper functions for formatting values

func formatBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func formatInt(i int) string {
	return fmt.Sprintf("%d", i)
}

func formatUint32(i uint32) string {
	return fmt.Sprintf("%d", i)
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%v", f)
}

func formatDuration(d time.Duration) string {
	return d.String()
}

func formatURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.String()
}

func formatStringSlice(s []string) string {
	return strings.Join(s, "|")
}
