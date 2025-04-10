package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/docker/go-connections/nat"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PostgresTestContainer struct {
	Container testcontainers.Container
	Host      string
	Port      string
	User      string
	Password  string
	Database  string
}

func RunPostgresTestContainer(ctx context.Context, testID string) (*PostgresTestContainer, error) {
	postgresUser := "postgres"
	postgresPassword := "really_strong_password_change_me"
	postgresDB := "postgres"

	initScript, err := resolveProjectPath("scripts/postgres/init_dev.sh")

	if err != nil {
		return nil, errors.NewServiceError("could not resolve absolute path for init script", err)
	}

	// Create a wait strategy that combines multiple conditions
	waitForLogs := wait.ForLog("database system is ready to accept connections").WithOccurrence(2)

	// Basic connectivity check
	waitForExec := wait.ForExec([]string{"pg_isready", "-U", postgresUser}).
		WithStartupTimeout(30 * time.Second)

	// SQL query check
	waitForSQL := wait.ForSQL("5432/tcp", "postgres", func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			postgresUser, postgresPassword, host, port.Port(), postgresDB)
	}).WithQuery("SELECT 1").WithStartupTimeout(30 * time.Second)

	// Combine the strategies
	waitStrategy := wait.ForAll(waitForExec, waitForSQL, waitForLogs)

	req := testcontainers.ContainerRequest{
		Image: "postgres:latest",
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_DB":       postgresDB,
		},
		WaitingFor: waitStrategy,
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      initScript,
				ContainerFilePath: "/docker-entrypoint-initdb.d/init_dev.sh",
				FileMode:          0755,
			},
		},
	}

	// Define retry parameters
	maxRetries := 5
	retryBackoff := 2 * time.Second
	retryTimeout := 30 * time.Second
	startTime := time.Now()

	var (
		container testcontainers.Container
		lastErr   error
	)

	// Retry loop for container creation
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Check if we've exceeded our total retry timeout
			if time.Since(startTime) > retryTimeout {
				return nil, errors.NewServiceError("timeout exceeded while trying to start postgres container", lastErr)
			}

			// Log retry attempt
			fmt.Printf("Retrying postgres container creation (attempt %d/%d) after error: %v\n",
				attempt+1, maxRetries, lastErr)

			// Wait before retrying with exponential backoff
			time.Sleep(retryBackoff)
			retryBackoff *= 2 // Exponential backoff
		}

		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})

		if err == nil {
			// Success! Break out of retry loop
			break
		}

		lastErr = err

		// Check if this is a port binding error
		if strings.Contains(err.Error(), "bind: address already in use") {
			// This is a port binding error, continue with retries
			continue
		} else {
			// This is some other error, don't retry
			return nil, errors.NewServiceError("failed to start postgres container", err)
		}
	}

	// Check if we exhausted all retries
	if container == nil {
		return nil, errors.NewServiceError(fmt.Sprintf("failed to start postgres container after %d attempts", maxRetries), lastErr)
	}

	// Get container state
	state, err := container.State(ctx)
	if err != nil {
		return nil, errors.NewServiceError("failed to get container state", err)
	}

	// Check if container is running
	if !state.Running {
		return nil, errors.NewServiceError("postgres container is not running after startup", nil)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, errors.NewServiceError("failed to get postgres container host", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, errors.NewServiceError("failed to get mapped port for postgres container", err)
	}

	postgresContainer := &PostgresTestContainer{
		Container: container,
		Host:      host,
		Port:      port.Port(),
		User:      postgresUser,
		Password:  postgresPassword,
		Database:  postgresDB,
	}

	// Final connectivity check
	if err := verifyDatabaseConnection(ctx, postgresContainer); err != nil {
		// Attempt to clean up before returning error
		_ = container.Terminate(ctx)
		return nil, errors.NewServiceError("failed to verify database connection", err)
	}

	return postgresContainer, nil
}

// verifyDatabaseConnection attempts to connect to the PostgreSQL database
// to ensure it's truly ready for use
func verifyDatabaseConnection(ctx context.Context, container *PostgresTestContainer) error {
	// Create a retry context with timeout
	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Attempt to connect with retries
	for {
		select {
		case <-retryCtx.Done():
			return errors.NewServiceError("timeout waiting for database to be ready", nil)
		default:
			connStr := container.ConnectionString()

			db, err := sql.Open("postgres", connStr)
			if err != nil {
				time.Sleep(500 * time.Millisecond)

				continue
			}

			// Try to ping the database
			if err = db.PingContext(ctx); err != nil {
				db.Close()
				time.Sleep(500 * time.Millisecond)

				continue
			}

			// Successfully connected
			db.Close()

			return nil
		}
	}
}

func (p *PostgresTestContainer) Terminate(ctx context.Context) error {
	return p.Container.Terminate(ctx)
}

func (p *PostgresTestContainer) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		p.User, p.Password, p.Host, p.Port, p.Database)
}

func resolveProjectPath(relPath string) (string, error) {
	root, err := os.Getwd()

	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}

		parent := filepath.Dir(root)

		if parent == root {
			return "", errors.NewServiceError("project root (go.mod) not found")
		}

		root = parent
	}

	return filepath.Join(root, relPath), nil
}
