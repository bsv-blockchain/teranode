package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupTestPostgresContainer() (string, func() error, error) {
	ctx := context.Background()

	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	postgresC, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second),
			wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		return "", nil, err
	}

	host, err := postgresC.Host(ctx)
	if err != nil {
		return "", nil, err
	}

	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	connStr := fmt.Sprintf("postgres://postgres:password@%s:%s/testdb?sslmode=disable", host, port.Port())

	teardown := func() error {
		return postgresC.Terminate(ctx)
	}

	return connStr, teardown, nil
}
