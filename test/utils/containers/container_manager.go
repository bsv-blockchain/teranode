package containers

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aerospike2 "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// UTXOStoreType represents the type of UTXO store backend
type UTXOStoreType string

const (
	// UTXOStoreAerospike uses Aerospike as the UTXO store backend
	UTXOStoreAerospike UTXOStoreType = "aerospike"
	// UTXOStorePostgres uses PostgreSQL as the UTXO store backend
	UTXOStorePostgres UTXOStoreType = "postgres"
	// UTXOStoreSQLite uses SQLite as the UTXO store backend (no container needed)
	UTXOStoreSQLite UTXOStoreType = "sqlite"
)

// ContainerManager manages test container lifecycle for various store backends
type ContainerManager struct {
	storeType         UTXOStoreType
	containerURL      string
	cleanupFunc       func() error
	aerospikeClient   *uaerospike.Client
	postgresContainer *postgres.PostgresContainer
}

// NewContainerManager creates a new container manager for the specified store type
// If storeType is empty, defaults to UTXOStoreAerospike
func NewContainerManager(storeType UTXOStoreType) (*ContainerManager, error) {
	if storeType == "" {
		storeType = UTXOStoreAerospike
	}

	cm := &ContainerManager{
		storeType: storeType,
	}

	return cm, nil
}

// Initialize starts the appropriate container and returns the connection URL
func (cm *ContainerManager) Initialize(ctx context.Context) (*url.URL, error) {
	switch cm.storeType {
	case UTXOStoreAerospike:
		return cm.initializeAerospike(ctx)
	case UTXOStorePostgres:
		return cm.initializePostgres(ctx)
	case UTXOStoreSQLite:
		return cm.initializeSQLite()
	default:
		return nil, errors.NewInvalidArgumentError("unsupported UTXO store type: %s", cm.storeType)
	}
}

// initializeAerospike starts an Aerospike container
func (cm *ContainerManager) initializeAerospike(ctx context.Context) (*url.URL, error) {
	aerospike.InitPrometheusMetrics()

	container, err := aerospike2.RunContainer(ctx, aerospike2.WithTTLSupport("test"))
	if err != nil {
		return nil, errors.NewExternalError("failed to start Aerospike container: %v", err)
	}

	cm.cleanupFunc = func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return container.Terminate(cleanupCtx)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, errors.NewExternalError("failed to get Aerospike host: %v", err)
	}

	port, err := container.ServicePort(ctx)
	if err != nil {
		return nil, errors.NewExternalError("failed to get Aerospike port: %v", err)
	}

	// Create raw client for cleanup operations
	client, aeroErr := uaerospike.NewClient(host, port)
	if aeroErr != nil {
		return nil, errors.NewExternalError("failed to create Aerospike client: %v", aeroErr)
	}
	cm.aerospikeClient = client

	// Build Aerospike connection URL
	cm.containerURL = fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%s&externalStore=file://./data/externalStore",
		host, port, "test", "test", "10m")

	parsedURL, err := url.Parse(cm.containerURL)
	if err != nil {
		return nil, errors.NewExternalError("failed to parse Aerospike URL: %v", err)
	}

	return parsedURL, nil
}

// initializePostgres starts a PostgreSQL container
func (cm *ContainerManager) initializePostgres(ctx context.Context) (*url.URL, error) {
	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	// Implement retry logic with random delays for more reliable container creation
	var (
		postgresC *postgres.PostgresContainer
		err       error
	)

	for attempt := 0; attempt < 3; attempt++ {
		// Add random delay to reduce chance of simultaneous container creation conflicts
		if attempt > 0 {
			// Random delay between 100-600ms
			delay := time.Duration(100+time.Now().Nanosecond()%500) * time.Millisecond
			time.Sleep(delay)
		}

		postgresC, err = postgres.Run(ctx,
			"docker.io/postgres:16-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername(dbUser),
			postgres.WithPassword(dbPassword),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(30*time.Second),
				wait.ForListeningPort("5432/tcp")),
		)

		if err == nil {
			break // Successfully created container
		}
	}

	// If all attempts failed, return the last error
	if err != nil {
		return nil, errors.NewExternalError("failed to start PostgreSQL container after 3 attempts: %v", err)
	}

	cm.postgresContainer = postgresC

	// Set cleanup function immediately after container assignment to ensure cleanup on all error paths
	cm.cleanupFunc = func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return postgresC.Terminate(cleanupCtx)
	}

	connStr, err := postgresC.ConnectionString(ctx)
	if err != nil {
		return nil, errors.NewExternalError("failed to get PostgreSQL connection string: %v", err)
	}

	// Ensure SSL is disabled in the connection string
	if !strings.Contains(connStr, "sslmode=") {
		connStr += "&sslmode=disable"
		// If there's no query parameter yet, use ? instead of &
		if !strings.Contains(connStr, "?") {
			connStr = strings.Replace(connStr, "&sslmode=disable", "?sslmode=disable", 1)
		}
	}

	// Add a database validation step to ensure PostgreSQL is truly ready
	if err := cm.validateDatabaseConnection(connStr, 5); err != nil {
		return nil, errors.NewExternalError("database validation failed: %v", err)
	}

	cm.containerURL = connStr

	parsedURL, err := url.Parse(connStr)
	if err != nil {
		return nil, errors.NewExternalError("failed to parse PostgreSQL URL: %v", err)
	}

	return parsedURL, nil
}

// validateDatabaseConnection attempts to connect to the database and run a simple query
// to verify it's truly ready for operations. It will retry with exponential backoff.
func (cm *ContainerManager) validateDatabaseConnection(connStr string, maxRetries int) error {
	var (
		db  *sql.DB
		err error
	)

	// Import the PostgreSQL driver
	_ = pq.Driver{}

	for i := 0; i < maxRetries; i++ {
		// Try to open a connection
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
			continue
		}

		// Try a simple query to verify the connection works
		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(queryCtx)
		cancel()

		if err == nil {
			// Connection successful, do a sample query to verify
			var result int
			err = db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
			if err == nil && result == 1 {
				db.Close()
				return nil // Success!
			}
		}

		// Close the connection before retrying
		if db != nil {
			db.Close()
		}

		// Wait with increasing delay before retry
		time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
	}

	return errors.NewProcessingError("failed to validate database connection after %d attempts: %v", maxRetries, err)
}

// initializeSQLite returns a SQLite in-memory URL (no container needed)
func (cm *ContainerManager) initializeSQLite() (*url.URL, error) {
	// SQLite doesn't require a container, use in-memory database
	cm.containerURL = "sqlite://memory:"

	parsedURL, err := url.Parse(cm.containerURL)
	if err != nil {
		return nil, errors.NewExternalError("failed to parse SQLite URL: %v", err)
	}

	return parsedURL, nil
}

// Cleanup tears down the container and closes any open connections
func (cm *ContainerManager) Cleanup() error {
	// Close Aerospike client if it exists
	if cm.aerospikeClient != nil {
		cm.aerospikeClient.Close()
	}

	// Call cleanup function if it exists
	if cm.cleanupFunc != nil {
		return cm.cleanupFunc()
	}

	return nil
}

// GetStoreType returns the configured store type
func (cm *ContainerManager) GetStoreType() UTXOStoreType {
	return cm.storeType
}

// GetContainerURL returns the connection URL for the container
func (cm *ContainerManager) GetContainerURL() string {
	return cm.containerURL
}
