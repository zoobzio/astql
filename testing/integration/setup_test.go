// Package integration provides integration tests for astql using real databases.
package integration

import (
	"context"
	"database/sql"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Shared containers - lazily initialized
var (
	sharedPgContainer      *PostgresContainer
	sharedMariaDBContainer *MariaDBContainer
	sharedMSSQLContainer   *MSSQLContainer

	pgOnce      sync.Once
	mariadbOnce sync.Once
	mssqlOnce   sync.Once

	// Track which containers were started for cleanup
	containersStarted = struct {
		pg      bool
		mariadb bool
		mssql   bool
	}{}
)

// TestMain sets up shared containers for all integration tests.
func TestMain(m *testing.M) {
	// Note: We can't check testing.Short() here because flag.Parse() hasn't been called yet.
	// The individual tests check for short mode themselves.

	// Run tests
	code := m.Run()

	// Cleanup any containers that were started
	ctx := context.Background()

	if containersStarted.pg && sharedPgContainer != nil {
		if sharedPgContainer.conn != nil {
			_ = sharedPgContainer.conn.Close(ctx)
		}
		if sharedPgContainer.container != nil {
			_ = sharedPgContainer.container.Terminate(ctx)
		}
	}

	if containersStarted.mariadb && sharedMariaDBContainer != nil {
		if sharedMariaDBContainer.db != nil {
			_ = sharedMariaDBContainer.db.Close()
		}
		if sharedMariaDBContainer.container != nil {
			_ = sharedMariaDBContainer.container.Terminate(ctx)
		}
	}

	if containersStarted.mssql && sharedMSSQLContainer != nil {
		if sharedMSSQLContainer.db != nil {
			_ = sharedMSSQLContainer.db.Close()
		}
		if sharedMSSQLContainer.container != nil {
			_ = sharedMSSQLContainer.container.Terminate(ctx)
		}
	}

	os.Exit(code)
}

// getPostgresContainer returns the shared PostgreSQL container, starting it if needed.
func getPostgresContainer(t *testing.T) *PostgresContainer {
	t.Helper()

	pgOnce.Do(func() {
		ctx := context.Background()

		container, err := postgres.Run(ctx,
			"docker.io/postgres:16-alpine",
			postgres.WithDatabase("astql_test"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
		)
		if err != nil {
			log.Fatalf("Failed to start postgres container: %v", err)
		}

		connStr, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			log.Fatalf("Failed to get connection string: %v", err)
		}

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			log.Fatalf("Failed to connect to postgres: %v", err)
		}

		sharedPgContainer = &PostgresContainer{
			container: container,
			conn:      conn,
			connStr:   connStr,
		}
		containersStarted.pg = true
	})

	return sharedPgContainer
}

// getMariaDBContainer returns the shared MariaDB container, starting it if needed.
func getMariaDBContainer(t *testing.T) *MariaDBContainer {
	t.Helper()

	mariadbOnce.Do(func() {
		ctx := context.Background()

		container, err := mariadb.Run(ctx,
			"docker.io/mariadb:11",
			mariadb.WithDatabase("astql_test"),
			mariadb.WithUsername("test"),
			mariadb.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("mariadbd: ready for connections").
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			log.Fatalf("Failed to start mariadb container: %v", err)
		}

		connStr, err := container.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("Failed to get connection string: %v", err)
		}

		db, err := sql.Open("mysql", connStr)
		if err != nil {
			log.Fatalf("Failed to connect to mariadb: %v", err)
		}

		// Wait for connection to be ready
		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(time.Second)
		}

		sharedMariaDBContainer = &MariaDBContainer{
			container: container,
			db:        db,
			connStr:   connStr,
		}
		containersStarted.mariadb = true
	})

	return sharedMariaDBContainer
}

// getMSSQLContainer returns the shared MSSQL container, starting it if needed.
func getMSSQLContainer(t *testing.T) *MSSQLContainer {
	t.Helper()

	mssqlOnce.Do(func() {
		ctx := context.Background()

		container, err := mssql.Run(ctx,
			"mcr.microsoft.com/mssql/server:2022-latest",
			mssql.WithAcceptEULA(),
			mssql.WithPassword("Test@12345"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("SQL Server is now ready for client connections").
					WithStartupTimeout(120*time.Second),
			),
		)
		if err != nil {
			log.Fatalf("Failed to start mssql container: %v", err)
		}

		connStr, err := container.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("Failed to get connection string: %v", err)
		}

		db, err := sql.Open("sqlserver", connStr)
		if err != nil {
			log.Fatalf("Failed to connect to mssql: %v", err)
		}

		// Wait for connection to be ready
		for i := 0; i < 60; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(time.Second)
		}

		sharedMSSQLContainer = &MSSQLContainer{
			container: container,
			db:        db,
			connStr:   connStr,
		}
		containersStarted.mssql = true
	})

	return sharedMSSQLContainer
}
