# Integration Tests

Integration tests verify astql against real database instances using Docker containers.

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
# Run all integration tests
make test-integration

# Run specific database tests
go test -v ./testing/integration/... -run TestPostgres
go test -v ./testing/integration/... -run TestMariaDB
go test -v ./testing/integration/... -run TestMSSQL
go test -v ./testing/integration/... -run TestSQLite
```

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

Integration tests run in CI with Docker available. They are excluded from short-mode unit test runs:

```bash
# Unit tests only (CI fast path)
go test -short ./...

# Full test suite (CI complete)
go test ./...
```
