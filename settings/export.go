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

// sensitiveKeys contains setting keys whose values must be redacted in
// exported metadata. Derived once from struct tags via extractSensitiveKeys
// (see redact.go) — add `redact:"true"` to a field's struct tag to mark it
// sensitive; the tag is the single source of truth.
var sensitiveKeys = extractSensitiveKeys()

const redactedValue = "********"

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

		currentValueStr := formatValue(currentVal)
		if sensitiveKeys[entry.Key] && currentValueStr != "" {
			currentValueStr = redactedValue
		}

		settings = append(settings, SettingMetadata{
			Key:             entry.Key,
			Name:            entry.Name,
			Type:            entry.Type,
			DefaultValue:    entry.DefaultValue,
			CurrentValue:    currentValueStr,
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
			LongDescription: "Specifies which BSV Blockchain network this node connects to. Each network has different genesis blocks, address prefixes, and peer discovery. 'mainnet' is the production BSV Blockchain network with real economic value - use for mining, exchanges, and production services. 'testnet' is a public test network with worthless coins for development and testing without risking real funds. 'stn' (Scaling Test Network) is BSV's dedicated network for testing high-throughput scenarios and large blocks. 'regtest' (Regression Test) is a local private network for automated testing with instant block generation. Network selection affects: genesis block hash, magic bytes for P2P protocol, default ports, address version bytes (for legacy addresses), and peer discovery seeds. Cannot be changed at runtime - requires node restart with empty data directory to switch networks.",
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
			if fieldType.Kind() == reflect.Pointer && fieldType.Elem().Kind() == reflect.Struct {
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
		if val.Kind() == reflect.Pointer {
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
			return redactURL(&u).String()
		}
		return fmt.Sprintf("%v", val.Interface())
	case reflect.Pointer:
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

	return redactURL(u).String()
}

// credentialQueryKeys are query-parameter names whose values are treated as secrets when a URL
// setting is rendered for display. Matched case-insensitively as a substring of the parameter
// name (bitcoin-sv/teranode#4844).
var credentialQueryKeys = []string{
	"password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
	"auth", "auth_key", "credential", "private_key", "access_key", "sas", "signature",
}

// redactedURLValue is the placeholder used INSIDE a redacted URL. It is deliberately not the
// `********` used for ordinary tagged fields: url.URL.String() percent-encodes the userinfo, so
// that placeholder renders as `%2A%2A%2A%2A%2A%2A%2A%2A` and defeats the only thing a placeholder
// is for — telling the operator at a glance that a credential was removed
// (bitcoin-sv/teranode#4844). This value survives URL encoding unchanged in every position it is
// used: userinfo, query value and opaque component.
const redactedURLValue = "REDACTED"

// redactURL returns a display copy of u with embedded credentials removed: the userinfo password
// and any credential-bearing query parameter are replaced by the placeholder. Scheme, user name,
// host, port, path and non-credential query parameters are preserved, because operators need to see
// which backend a node points at (bitcoin-sv/teranode#4844).
//
// The name-based redact tag cannot see these: nothing in the key `blockchain_store` looks like a
// secret, yet its documented production syntax is postgres://user:pass@host/db. Doing it
// structurally here covers every URL setting at once, including ones added later.
//
// The URL is copied by VALUE and the caller's is never mutated: these are the live settings the
// store constructors use, so mutating one would break the node.
func redactURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}

	c := *u

	// An OPAQUE URL is one net/url could not decompose: `scheme:opaque`, with no `//` authority.
	// Everything after the scheme lands in one uninterpreted string, so User is nil, Host is empty,
	// and the structural redaction below has nothing to bite on — yet the opaque part can hold a
	// credential verbatim, as in `postgres:user:password@host/db`. There is no safe way to pick the
	// secret out of a form the standard parser itself declined to interpret, so fail safe and
	// replace the whole component. The scheme is preserved, so an operator can still see which
	// backend type the setting names and that something was removed
	// (bitcoin-sv/teranode#4844).
	if c.Opaque != "" {
		c.Opaque = redactedURLValue
		c.User = nil
		c.RawQuery = redactRawQuery(c.RawQuery)

		return &c
	}

	if c.User != nil {
		if _, hasPassword := c.User.Password(); hasPassword {
			c.User = url.UserPassword(c.User.Username(), redactedURLValue)
		}
	}

	c.RawQuery = redactRawQuery(c.RawQuery)

	return &c
}

// redactRawQuery replaces the values of credential-bearing query parameters, working on the RAW
// query string rather than a parsed map.
//
// Two simpler designs leak. Scanning url.ParseQuery's MAP misses the offending key entirely when
// the value carries a malformed escape (`password=%ZZ`), so the scan finds nothing, concludes the
// query is clean, and preserves it byte-for-byte. Splitting the raw string on `&` alone misses
// `a=1;password=secret`, which is then a single segment whose key parses as `a`.
//
// So url.ParseQuery is used ONLY as a yes/no validity oracle, never as a data source, and anything
// the standard parser rejects has its whole query replaced. That trades display fidelity for
// safety on malformed input, deliberately: an operator who wrote a malformed query string sees
// `?REDACTED` instead of their parameters, a cosmetic annoyance they can diagnose from the config
// file - the alternative is printing their password into the settings portal. Do not "improve"
// this back.
//
// Well-formed queries are rewritten segment by segment rather than through Encode(), which would
// reorder alphabetically and re-percent-encode, churning every kafka and aerospike URL in the
// settings portal for no benefit.
func redactRawQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}

	if _, err := url.ParseQuery(rawQuery); err != nil {
		return redactedURLValue
	}

	segments := strings.Split(rawQuery, "&")
	changed := false

	for i, segment := range segments {
		eq := strings.Index(segment, "=")
		if eq < 0 {
			// A valueless key such as `?password` carries no credential, so rewriting it would
			// invent a secret that is not there.
			continue
		}

		rawKey := segment[:eq]

		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			key = rawKey
		}

		if !isCredentialQueryKey(key) {
			continue
		}

		segments[i] = rawKey + "=" + redactedURLValue
		changed = true
	}

	if !changed {
		return rawQuery
	}

	return strings.Join(segments, "&")
}

// isCredentialQueryKey reports whether a query-parameter name looks like it carries a secret.
func isCredentialQueryKey(key string) bool {
	lower := strings.ToLower(key)

	for _, candidate := range credentialQueryKeys {
		if strings.Contains(lower, candidate) {
			return true
		}
	}

	return false
}

func formatStringSlice(s []string) string {
	return strings.Join(s, "|")
}
