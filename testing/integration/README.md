# Integration Tests

Integration tests verify astql against real database instances using Docker containers.

This is a **separate Go module** to keep heavy test dependencies (testcontainers, database drivers) out of the main `astql` package. Consumers of `astql` will not download these dependencies.

## Prerequisites

- Docker must be running
- Sufficient resources for database containers

## Supported Databases

| Database | Container | Version |
|----------|-----------|---------|
| PostgreSQL | `postgres:16-alpine` | 16.x |
| MariaDB | `mariadb:11` | 11.x |
| MS SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | 2022 |
| SQLite | In-memory | Latest |

## Running Integration Tests

```bash
# From the integration directory
cd testing/integration

# Run all integration tests
go test -v ./...

# Run specific database tests
go test -v -run TestPostgres
go test -v -run TestMariaDB
go test -v -run TestMSSQL
go test -v -run TestSQLite
```

## Coverage

Integration tests can contribute to coverage of the main `astql` package using `-coverpkg`:

```bash
# Run integration tests with coverage of main package
go test -coverpkg=github.com/zoobzio/astql/... -coverprofile=coverage.out ./...

# View coverage report
go tool cover -func=coverage.out

# Generate HTML report
go tool cover -html=coverage.out -o coverage.html
```

This works because `-coverpkg` instruments packages across module boundaries.

## Container Lifecycle

Containers are managed via [testcontainers-go](https://golang.testcontainers.org/):

- **Lazy initialisation**: Containers start only when a test requires them
- **Shared instances**: All tests share containers via `sync.Once`
- **Automatic cleanup**: `TestMain` terminates containers after all tests complete

This approach minimises startup time when running a subset of tests.

## Writing Integration Tests

### Test Structure

```go
func TestPostgres_FeatureName(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    container := getPostgresContainer(t)
    // container.conn is a *pgx.Conn ready to use

    // Create test schema
    _, err := container.conn.Exec(ctx, `CREATE TABLE ...`)

    // Run assertions
}
```

### Skip in Short Mode

Integration tests must check for short mode to allow fast unit test runs:

```go
if testing.Short() {
    t.Skip("Skipping integration test in short mode")
}
```

### Database Isolation

Each test should:

1. Create its own tables with unique names or use transactions
2. Clean up after itself if not using transactions
3. Not assume any pre-existing state beyond an empty database

### Container Access

| Function | Returns |
|----------|---------|
| `getPostgresContainer(t)` | `*PostgresContainer` with `conn *pgx.Conn` |
| `getMariaDBContainer(t)` | `*MariaDBContainer` with `db *sql.DB` |
| `getMSSQLContainer(t)` | `*MSSQLContainer` with `db *sql.DB` |

SQLite tests use in-memory databases directly without containers.

## CI Behaviour

Integration tests run in CI with Docker available. Since this is a separate module, it must be tested separately:

```bash
# Unit tests only (main module, fast)
go test ./...

# Integration tests (from integration directory, requires Docker)
cd testing/integration && go test ./...

# Combined coverage
cd testing/integration && go test -coverpkg=github.com/zoobzio/astql/... -coverprofile=integration.out ./...
```
