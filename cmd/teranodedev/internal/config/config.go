package config

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"gopkg.in/yaml.v3"
)

const configFile = ".teranode-dev.yaml"

// Config holds the developer's local setup choices.
type Config struct {
	Version          int    `yaml:"version"`
	DevName          string `yaml:"dev_name"`
	UTXOBackend      string `yaml:"utxo_backend"`
	Network          string `yaml:"network"`
	UseKafka         bool   `yaml:"use_kafka"`
	EnableMonitoring bool   `yaml:"enable_monitoring"`
	EnableTracing    bool   `yaml:"enable_tracing"`
	ProjectRoot      string `yaml:"project_root"`
	DataDir          string `yaml:"data_dir"`
	CreatedAt        string `yaml:"created_at,omitempty"`
	UpdatedAt        string `yaml:"updated_at,omitempty"`
}

// FindProjectRoot walks up from CWD looking for go.mod containing the teranode module path.
func FindProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", errors.NewProcessingError("failed to get working directory", err)
	}

	for {
		gomod := filepath.Join(dir, "go.mod")

		data, err := os.ReadFile(gomod)
		if err == nil && strings.Contains(string(data), "github.com/bsv-blockchain/teranode") {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.NewProcessingError("could not find teranode project root (no go.mod with teranode module found)")
		}

		dir = parent
	}
}

// Load reads the config file from the project root.
func Load(projectRoot string) (*Config, error) {
	path := filepath.Join(projectRoot, configFile)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read %s", configFile, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, errors.NewProcessingError("failed to parse %s", configFile, err)
	}

	return &cfg, nil
}

// Save writes the config file to the project root.
func Save(projectRoot string, cfg *Config) error {
	cfg.Version = 1

	now := time.Now().Format(time.RFC3339)
	if cfg.CreatedAt == "" {
		cfg.CreatedAt = now
	}

	cfg.UpdatedAt = now

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return errors.NewProcessingError("failed to marshal config", err)
	}

	path := filepath.Join(projectRoot, configFile)

	return os.WriteFile(path, data, 0644)
}
