package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		return nil, errors.New("could not resolve absolute path for init script")
	}

	req := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"15432:5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_DB":       postgresDB,
		},
		WaitingFor: wait.ForExec([]string{"pg_isready", "-U", postgresUser}).
			WithStartupTimeout(30 * time.Second).
			WithPollInterval(2 * time.Second),

		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      initScript,
				ContainerFilePath: "/docker-entrypoint-initdb.d/init_dev.sh",
				FileMode:          0755,
			},
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}

	return &PostgresTestContainer{
		Container: container,
		Host:      host,
		Port:      port.Port(),
		User:      postgresUser,
		Password:  postgresPassword,
		Database:  postgresDB,
	}, nil
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
			return "", errors.New("project root (go.mod) not found")
		}

		root = parent
	}

	return filepath.Join(root, relPath), nil
}
