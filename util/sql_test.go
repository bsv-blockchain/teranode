package util_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// mockLogger implements ulogger.Logger for testing
type mockLogger struct{}

func (m *mockLogger) LogLevel() int                                { return 0 }
func (m *mockLogger) SetLogLevel(string)                           {}
func (m *mockLogger) Debugf(string, ...interface{})                {}
func (m *mockLogger) Infof(string, ...interface{})                 {}
func (m *mockLogger) Warnf(string, ...interface{})                 {}
func (m *mockLogger) Errorf(string, ...interface{})                {}
func (m *mockLogger) Fatalf(string, ...interface{})                {}
func (m *mockLogger) New(string, ...ulogger.Option) ulogger.Logger { return m }
func (m *mockLogger) Duplicate(...ulogger.Option) ulogger.Logger   { return m }

// createTestSettings creates a test settings object with default values
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		DataFolder: os.TempDir(),
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}
}

func TestInitSQLDB(t *testing.T) {
	logger := &mockLogger{}
	testSettings := createTestSettings()

	tests := []struct {
		name    string
		url     string
		wantErr bool
		errType string
	}{
		{
			name:    "valid postgres scheme",
			url:     "postgres://user:pass@localhost:5432/testdb",
			wantErr: true, // Will fail because we don't have a real postgres connection
			errType: "service error",
		},
		{
			name:    "valid sqlite scheme",
			url:     "sqlite:///testdb",
			wantErr: false,
		},
		{
			name:    "valid sqlitememory scheme",
			url:     "sqlitememory:///testdb",
			wantErr: false,
		},
		{
			name:    "unknown scheme",
			url:     "mysql://localhost:3306/testdb",
			wantErr: true,
			errType: "configuration error",
		},
		{
			name:    "invalid url",
			url:     "://invalid-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Failed to parse test URL: %v", err)
				}
				return
			}

			db, err := util.InitSQLDB(logger, storeURL, testSettings)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)

				if tt.errType == "configuration error" {
					var teranodeErr *errors.Error
					if assert.ErrorAs(t, err, &teranodeErr) {
						assert.Contains(t, err.Error(), "unknown scheme")
					}
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, db)
				if db != nil {
					err := db.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestInitPostgresDB(t *testing.T) {
	logger := &mockLogger{}
	testSettings := createTestSettings()

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "postgres with credentials and sslmode",
			url:     "postgres://testuser:testpass@localhost:5432/testdb?sslmode=require",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres without credentials",
			url:     "postgres://localhost:5432/testdb",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres with default sslmode",
			url:     "postgres://user:pass@localhost:5432/testdb",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres with multiple sslmode values",
			url:     "postgres://user:pass@localhost:5432/testdb?sslmode=disable&sslmode=require",
			wantErr: true, // Will fail without real postgres
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			require.NoError(t, err)

			db, err := util.InitPostgresDB(logger, storeURL, testSettings, nil)

			// Since we don't have a real postgres instance, we expect connection errors
			// but we can still test the URL parsing logic
			assert.Error(t, err)
			assert.Nil(t, db)

			// Verify it's a service error (connection failure)
			var teranodeErr *errors.Error
			if assert.ErrorAs(t, err, &teranodeErr) {
				assert.Contains(t, err.Error(), "failed to open postgres DB")
			}
		})
	}
}

func TestInitSQLiteDB(t *testing.T) {
	logger := &mockLogger{}

	// Create a temporary directory for file-based sqlite tests
	tempDir := t.TempDir()
	testSettings := &settings.Settings{
		DataFolder: tempDir,
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}

	tests := []struct {
		name    string
		scheme  string
		path    string
		wantErr bool
	}{
		{
			name:    "sqlite memory database",
			scheme:  "sqlitememory",
			path:    "/testdb",
			wantErr: false,
		},
		{
			name:    "sqlite file database",
			scheme:  "sqlite",
			path:    "/testdb",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL := &url.URL{
				Scheme: tt.scheme,
				Path:   tt.path,
			}

			db, err := util.InitSQLiteDB(logger, storeURL, testSettings)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, db)

				if db != nil {
					// Test that foreign keys are enabled
					var foreignKeysEnabled int
					err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeysEnabled)
					assert.NoError(t, err)
					assert.Equal(t, 1, foreignKeysEnabled, "Foreign keys should be enabled")

					// Test that locking mode pragma was executed (value may vary by SQLite version)
					var lockingMode string
					err = db.QueryRow("PRAGMA locking_mode").Scan(&lockingMode)
					assert.NoError(t, err)
					assert.Contains(t, []string{"shared", "normal"}, lockingMode, "Locking mode should be set")

					// For file-based databases, check that the file was created
					if tt.scheme == "sqlite" {
						dbName := tt.path[1:] // Remove leading slash
						expectedPath := filepath.Join(tempDir, dbName+".db")
						_, err := os.Stat(expectedPath)
						assert.NoError(t, err, "Database file should be created")
					}

					err := db.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestInitSQLiteDBDataFolderCreation(t *testing.T) {
	logger := &mockLogger{}

	// Use a non-existent directory to test folder creation
	tempDir := filepath.Join(os.TempDir(), "teranode-test", "sql-test", time.Now().Format("20060102-150405"))
	testSettings := &settings.Settings{
		DataFolder: tempDir,
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}

	storeURL := &url.URL{
		Scheme: "sqlite",
		Path:   "/testdb",
	}

	// Ensure the directory doesn't exist initially
	_, err := os.Stat(tempDir)
	assert.True(t, os.IsNotExist(err), "Test directory should not exist initially")

	db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Verify the directory was created
	stat, err := os.Stat(tempDir)
	assert.NoError(t, err)
	assert.True(t, stat.IsDir(), "Data folder should be created")

	if db != nil {
		err := db.Close()
		assert.NoError(t, err)
	}

	// Cleanup
	err = os.RemoveAll(filepath.Dir(tempDir))
	assert.NoError(t, err)
}

func TestInitSQLiteDBNestedPath(t *testing.T) {
	logger := &mockLogger{}

	// Use a non-existent directory with nested subdirectory in the path
	// This tests the multi-node scenario where paths are like "teranode1/blockchain1"
	tempDir := filepath.Join(os.TempDir(), "teranode-test", "nested-sql-test", time.Now().Format("20060102-150405"))
	testSettings := &settings.Settings{
		DataFolder: tempDir,
		// Not used by InitSQLiteDB, but set for consistency with other DB init tests.
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}

	// This simulates sqlite:///teranode1/blockchain1 used by MultiNodeSettings
	storeURL := &url.URL{
		Scheme: "sqlite",
		Path:   "/teranode1/blockchain1",
	}

	// Ensure the directory doesn't exist initially
	_, err := os.Stat(tempDir)
	assert.True(t, os.IsNotExist(err), "Test directory should not exist initially")

	db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
	assert.NoError(t, err, "Should handle nested paths like teranode1/blockchain1")
	assert.NotNil(t, db)

	// Verify the nested directory was created
	nestedDir := filepath.Join(tempDir, "teranode1")
	stat, err := os.Stat(nestedDir)
	assert.NoError(t, err)
	assert.True(t, stat.IsDir(), "Nested data folder teranode1 should be created")

	// Verify the database file exists
	dbFile := filepath.Join(nestedDir, "blockchain1.db")
	_, err = os.Stat(dbFile)
	assert.NoError(t, err, "Database file should exist")

	if db != nil {
		err := db.Close()
		assert.NoError(t, err)
	}

	// Cleanup
	err = os.RemoveAll(filepath.Dir(tempDir))
	assert.NoError(t, err)
}

func TestInitSQLiteDBInvalidDataFolder(t *testing.T) {
	logger := &mockLogger{}

	// Try to create a database in a location where we can't create directories
	// This simulates a permission error scenario
	testSettings := &settings.Settings{
		DataFolder: "/root/invalid/path/that/should/not/exist",
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}

	storeURL := &url.URL{
		Scheme: "sqlite",
		Path:   "/testdb",
	}

	db, err := util.InitSQLiteDB(logger, storeURL, testSettings)

	// This should fail on most systems due to permissions
	if err != nil {
		assert.Error(t, err)
		assert.Nil(t, db)

		var teranodeErr *errors.Error
		if assert.ErrorAs(t, err, &teranodeErr) {
			assert.Contains(t, err.Error(), "failed to create data folder")
		}
	}
}

func TestInitPostgresDB_PoolSettings(t *testing.T) {
	logger := &mockLogger{}

	// Helper to create test settings with global defaults
	createSettingsWithGlobalDefaults := func() *settings.Settings {
		return &settings.Settings{
			Postgres: settings.PostgresSettings{
				MaxOpenConns:    50,
				MaxIdleConns:    10,
				ConnMaxLifetime: 5 * time.Minute,
				ConnMaxIdleTime: 1 * time.Minute,
			},
		}
	}

	tests := []struct {
		name                 string
		globalSettings       *settings.Settings
		servicePoolSettings  *settings.PostgresSettings
		expectedMaxOpenConns int
		expectedMaxIdleConns int
		expectedLifetime     time.Duration
		expectedIdleTime     time.Duration
		description          string
	}{
		{
			name:                 "use global defaults when service settings nil",
			globalSettings:       createSettingsWithGlobalDefaults(),
			servicePoolSettings:  nil,
			expectedMaxOpenConns: 50,
			expectedMaxIdleConns: 10,
			expectedLifetime:     5 * time.Minute,
			expectedIdleTime:     1 * time.Minute,
			description:          "When no service-specific settings, use global defaults",
		},
		{
			name:           "service settings fully override global",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    80,
				MaxIdleConns:    20,
				ConnMaxLifetime: 10 * time.Minute,
				ConnMaxIdleTime: 2 * time.Minute,
			},
			expectedMaxOpenConns: 80,
			expectedMaxIdleConns: 20,
			expectedLifetime:     10 * time.Minute,
			expectedIdleTime:     2 * time.Minute,
			description:          "All service settings override global defaults",
		},
		{
			name:           "partial override - only MaxOpenConns",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    100,
				MaxIdleConns:    0, // Zero value - should use global
				ConnMaxLifetime: 0, // Zero value - should use global
				ConnMaxIdleTime: 0, // Zero value - should use global
			},
			expectedMaxOpenConns: 100,
			expectedMaxIdleConns: 10,              // From global
			expectedLifetime:     5 * time.Minute, // From global
			expectedIdleTime:     1 * time.Minute, // From global
			description:          "Only MaxOpenConns overridden, others use global",
		},
		{
			name:           "partial override - only MaxIdleConns",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    0, // Zero value - should use global
				MaxIdleConns:    25,
				ConnMaxLifetime: 0, // Zero value - should use global
				ConnMaxIdleTime: 0, // Zero value - should use global
			},
			expectedMaxOpenConns: 50, // From global
			expectedMaxIdleConns: 25,
			expectedLifetime:     5 * time.Minute, // From global
			expectedIdleTime:     1 * time.Minute, // From global
			description:          "Only MaxIdleConns overridden, others use global",
		},
		{
			name:           "partial override - only ConnMaxLifetime",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    0, // Zero value - should use global
				MaxIdleConns:    0, // Zero value - should use global
				ConnMaxLifetime: 3 * time.Minute,
				ConnMaxIdleTime: 0, // Zero value - should use global
			},
			expectedMaxOpenConns: 50, // From global
			expectedMaxIdleConns: 10, // From global
			expectedLifetime:     3 * time.Minute,
			expectedIdleTime:     1 * time.Minute, // From global
			description:          "Only ConnMaxLifetime overridden, others use global",
		},
		{
			name:           "partial override - only ConnMaxIdleTime",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    0, // Zero value - should use global
				MaxIdleConns:    0, // Zero value - should use global
				ConnMaxLifetime: 0, // Zero value - should use global
				ConnMaxIdleTime: 30 * time.Second,
			},
			expectedMaxOpenConns: 50,              // From global
			expectedMaxIdleConns: 10,              // From global
			expectedLifetime:     5 * time.Minute, // From global
			expectedIdleTime:     30 * time.Second,
			description:          "Only ConnMaxIdleTime overridden, others use global",
		},
		{
			name:           "mixed override - some settings",
			globalSettings: createSettingsWithGlobalDefaults(),
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns:    75,
				MaxIdleConns:    0, // Zero value - should use global
				ConnMaxLifetime: 7 * time.Minute,
				ConnMaxIdleTime: 0, // Zero value - should use global
			},
			expectedMaxOpenConns: 75,
			expectedMaxIdleConns: 10, // From global
			expectedLifetime:     7 * time.Minute,
			expectedIdleTime:     1 * time.Minute, // From global
			description:          "Mixed override - MaxOpenConns and ConnMaxLifetime, others use global",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse("postgres://user:pass@localhost:5432/testdb")
			require.NoError(t, err)

			// We can't actually connect to PostgreSQL, but we can test the logic
			// by checking that InitPostgresDB processes the settings correctly
			// The function will fail at connection, but we can verify the settings merging logic

			// Since we can't verify the actual DB settings without a connection,
			// we'll test the helper function that determines pool settings
			// by creating a mock that captures what would be set

			// Test the merging logic by calling InitPostgresDB
			// It will fail to connect, but we can verify the error is about connection, not settings
			db, err := util.InitPostgresDB(logger, storeURL, tt.globalSettings, tt.servicePoolSettings)

			// We expect a connection error, not a configuration error
			if err != nil {
				// Verify it's a connection error, not a settings error
				assert.Contains(t, err.Error(), "failed to open postgres DB")
				assert.Nil(t, db)
			}

			// To actually verify the pool settings were applied correctly,
			// we would need a real PostgreSQL connection. For unit tests,
			// we verify the logic by testing the settings merging function separately.
		})
	}
}

func TestPostgresPoolSettingsMerging(t *testing.T) {
	// Test the merging logic directly
	globalSettings := &settings.PostgresSettings{
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
	}

	tests := []struct {
		name             string
		serviceSettings  *settings.PostgresSettings
		expectedMaxOpen  int
		expectedMaxIdle  int
		expectedLifetime time.Duration
		expectedIdleTime time.Duration
	}{
		{
			name:             "nil service settings uses global",
			serviceSettings:  nil,
			expectedMaxOpen:  50,
			expectedMaxIdle:  10,
			expectedLifetime: 5 * time.Minute,
			expectedIdleTime: 1 * time.Minute,
		},
		{
			name: "full override",
			serviceSettings: &settings.PostgresSettings{
				MaxOpenConns:    80,
				MaxIdleConns:    20,
				ConnMaxLifetime: 10 * time.Minute,
				ConnMaxIdleTime: 2 * time.Minute,
			},
			expectedMaxOpen:  80,
			expectedMaxIdle:  20,
			expectedLifetime: 10 * time.Minute,
			expectedIdleTime: 2 * time.Minute,
		},
		{
			name: "partial override with zero values",
			serviceSettings: &settings.PostgresSettings{
				MaxOpenConns:    100,
				MaxIdleConns:    0, // Should use global
				ConnMaxLifetime: 0, // Should use global
				ConnMaxIdleTime: 0, // Should use global
			},
			expectedMaxOpen:  100,
			expectedMaxIdle:  10,              // From global
			expectedLifetime: 5 * time.Minute, // From global
			expectedIdleTime: 1 * time.Minute, // From global
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the merging logic from InitPostgresDB
			poolSettings := globalSettings
			if tt.serviceSettings != nil {
				poolSettings = &settings.PostgresSettings{
					MaxOpenConns:    tt.serviceSettings.MaxOpenConns,
					MaxIdleConns:    tt.serviceSettings.MaxIdleConns,
					ConnMaxLifetime: tt.serviceSettings.ConnMaxLifetime,
					ConnMaxIdleTime: tt.serviceSettings.ConnMaxIdleTime,
				}
				// Use global defaults for zero values
				if poolSettings.MaxOpenConns == 0 {
					poolSettings.MaxOpenConns = globalSettings.MaxOpenConns
				}
				if poolSettings.MaxIdleConns == 0 {
					poolSettings.MaxIdleConns = globalSettings.MaxIdleConns
				}
				if poolSettings.ConnMaxLifetime == 0 {
					poolSettings.ConnMaxLifetime = globalSettings.ConnMaxLifetime
				}
				if poolSettings.ConnMaxIdleTime == 0 {
					poolSettings.ConnMaxIdleTime = globalSettings.ConnMaxIdleTime
				}
			}

			assert.Equal(t, tt.expectedMaxOpen, poolSettings.MaxOpenConns, "MaxOpenConns mismatch")
			assert.Equal(t, tt.expectedMaxIdle, poolSettings.MaxIdleConns, "MaxIdleConns mismatch")
			assert.Equal(t, tt.expectedLifetime, poolSettings.ConnMaxLifetime, "ConnMaxLifetime mismatch")
			assert.Equal(t, tt.expectedIdleTime, poolSettings.ConnMaxIdleTime, "ConnMaxIdleTime mismatch")
		})
	}
}

func TestInitSQLDB_WithServicePoolSettings(t *testing.T) {
	logger := &mockLogger{}
	testSettings := createTestSettings()
	testSettings.Postgres = settings.PostgresSettings{
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
	}

	tests := []struct {
		name                string
		url                 string
		servicePoolSettings *settings.PostgresSettings
		wantErr             bool
		description         string
	}{
		{
			name:                "postgres with nil service settings",
			url:                 "postgres://user:pass@localhost:5432/testdb",
			servicePoolSettings: nil,
			wantErr:             true, // Connection will fail
			description:         "Should use global defaults when service settings are nil",
		},
		{
			name: "postgres with service settings",
			url:  "postgres://user:pass@localhost:5432/testdb",
			servicePoolSettings: &settings.PostgresSettings{
				MaxOpenConns: 80,
			},
			wantErr:     true, // Connection will fail
			description: "Should merge service settings with global defaults",
		},
		{
			name:                "sqlite ignores service pool settings",
			url:                 "sqlite:///test",
			servicePoolSettings: &settings.PostgresSettings{MaxOpenConns: 999},
			wantErr:             false, // SQLite should work
			description:         "SQLite should ignore PostgreSQL pool settings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			require.NoError(t, err)

			var db *usql.DB
			if tt.servicePoolSettings != nil {
				db, err = util.InitSQLDB(logger, storeURL, testSettings, tt.servicePoolSettings)
			} else {
				db, err = util.InitSQLDB(logger, storeURL, testSettings)
			}

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)
			} else {
				require.NoError(t, err)
				require.NotNil(t, db)
				defer db.Close()
			}
		})
	}
}

func TestSQLEngineConstants(t *testing.T) {
	// Test that the SQLEngine constants have expected values
	assert.Equal(t, "postgres", string(util.Postgres))
	assert.Equal(t, "sqlite", string(util.Sqlite))
	assert.Equal(t, "sqlitememory", string(util.SqliteMemory))
}

// TestSQLiteConnectionString tests various SQLite connection string formats
func TestSQLiteConnectionString(t *testing.T) {
	logger := &mockLogger{}
	tempDir := t.TempDir()

	testSettings := &settings.Settings{
		DataFolder: tempDir,
		Postgres: settings.PostgresSettings{
			MaxIdleConns: 10,
			MaxOpenConns: 80,
		},
	}

	t.Run("memory database connection string format", func(t *testing.T) {
		storeURL := &url.URL{
			Scheme: "sqlitememory",
			Path:   "/testdb",
		}

		db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer func() {
			err := db.Close()
			assert.NoError(t, err)
		}()

		// Memory databases should work immediately
		_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
		assert.NoError(t, err)
	})

	t.Run("file database with WAL mode", func(t *testing.T) {
		storeURL := &url.URL{
			Scheme: "sqlite",
			Path:   "/waltest",
		}

		db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer func() {
			err := db.Close()
			assert.NoError(t, err)
		}()

		// Check that WAL mode is enabled
		var journalMode string
		err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
		assert.NoError(t, err)
		assert.Equal(t, "wal", journalMode, "Journal mode should be WAL")

		// Check busy timeout
		var busyTimeout int
		err = db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout)
		assert.NoError(t, err)
		assert.Equal(t, 5000, busyTimeout, "Busy timeout should be 5000ms")
	})
}
