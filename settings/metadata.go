package settings

// SettingMetadata contains metadata about a single configuration setting.
type SettingMetadata struct {
	Key             string `json:"key"`             // Config key used in settings.conf
	Name            string `json:"name"`            // Human-readable display name
	Type            string `json:"type"`            // Data type: string, int, bool, duration, url, []string
	DefaultValue    string `json:"defaultValue"`    // Default value as string
	CurrentValue    string `json:"currentValue"`    // Current runtime value as string
	Description     string `json:"description"`     // What the setting does (short)
	LongDescription string `json:"longDescription"` // Detailed explanation of the setting
	Category        string `json:"category"`        // Service grouping (e.g., "Global", "Kafka", "P2P")
	UsageHint       string `json:"usageHint"`       // Usage guidance or tips
}

// SettingsRegistry contains all settings metadata organized by category.
type SettingsRegistry struct {
	Settings   []SettingMetadata `json:"settings"`
	Categories []string          `json:"categories"`
	Version    string            `json:"version"`
	Commit     string            `json:"commit"`
}

// Setting categories for organizational purposes.
const (
	CategoryGlobal            = "Global"
	CategoryKafka             = "Kafka"
	CategoryAerospike         = "Aerospike"
	CategoryAsset             = "Asset"
	CategoryBlockAssembly     = "BlockAssembly"
	CategoryBlockValidation   = "BlockValidation"
	CategoryBlockChain        = "BlockChain"
	CategoryBlock             = "Block"
	CategoryBlockPersister    = "BlockPersister"
	CategoryP2P               = "P2P"
	CategoryValidator         = "Validator"
	CategorySubtreeValidation = "SubtreeValidation"
	CategoryLegacy            = "Legacy"
	CategoryPolicy            = "Policy"
	CategoryPruner            = "Pruner"
	CategoryRPC               = "RPC"
	CategoryUtxoStore         = "UtxoStore"
	CategoryPropagation       = "Propagation"
	CategoryCoinbase          = "Coinbase"
	CategoryDashboard         = "Dashboard"
	CategoryAlert             = "Alert"
	CategoryPostgres          = "Postgres"
	CategoryDebug             = "Debug"
)

// AllCategories returns all available setting categories in display order.
func AllCategories() []string {
	return []string{
		CategoryGlobal,
		CategoryKafka,
		CategoryAerospike,
		CategoryAsset,
		CategoryBlockAssembly,
		CategoryBlockValidation,
		CategoryBlockChain,
		CategoryBlock,
		CategoryBlockPersister,
		CategoryP2P,
		CategoryValidator,
		CategorySubtreeValidation,
		CategoryLegacy,
		CategoryPolicy,
		CategoryPruner,
		CategoryRPC,
		CategoryUtxoStore,
		CategoryPropagation,
		CategoryCoinbase,
		CategoryDashboard,
		CategoryAlert,
		CategoryPostgres,
		CategoryDebug,
	}
}
