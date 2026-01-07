# Testing

This directory contains the testing infrastructure for astql.

## Structure

```
testing/
├── helpers.go           # Test utilities and assertions
├── helpers_test.go      # Tests for the helpers themselves
├── benchmarks/          # Performance benchmarks
│   └── render_benchmark_test.go
└── integration/         # Integration tests with real databases
    ├── setup_test.go    # Shared container setup
    ├── postgres_test.go
    ├── sqlite_test.go
    ├── mariadb_test.go
    └── mssql_test.go
```

## Running Tests

```bash
# Unit tests (fast, no external dependencies)
make test

# Unit tests with race detector
make test-race

# Integration tests (requires Docker)
make test-integration

# All tests
make test-all

# Benchmarks
make bench
```

## Test Helpers

The `testing` package provides domain-specific helpers for astql tests:

| Helper | Purpose |
|--------|---------|
| `TestInstance(t)` | Creates a fully-featured ASTQL instance with users, posts, comments, orders, and products tables |
| `AssertSQL(t, expected, actual)` | Compares SQL strings with detailed diff on mismatch |
| `AssertParams(t, expected, actual)` | Validates parameter lists |
| `AssertContainsParam(t, params, param)` | Checks for a specific parameter |
| `AssertNoError(t, err)` | Fails if error is not nil |
| `AssertError(t, err)` | Fails if error is nil |
| `AssertErrorContains(t, err, substr)` | Validates error message content |
| `AssertPanics(t, fn)` | Verifies a function panics |
| `AssertPanicsWithMessage(t, fn, substr)` | Verifies panic message content |

All helpers call `t.Helper()` for clean stack traces.

## Coverage Target

The project targets 70% code coverage. Coverage below 60% is considered failing.

```bash
# Generate coverage report
make coverage

# Full coverage including integration tests
make coverage-all
```

## Writing Tests

### Unit Tests

Place unit tests alongside the code they test:

```
builder.go
builder_test.go
```

Use the test helpers from the `testing` package:

```go
import (
    "testing"

    astqltest "github.com/zoobzio/astql/testing"
)

func TestSomething(t *testing.T) {
    instance := astqltest.TestInstance(t)

    result, err := astql.Select(instance.T("users")).Render(postgres.New())
    astqltest.AssertNoError(t, err)
    astqltest.AssertSQL(t, `SELECT * FROM "users"`, result.SQL)
}
```

### Integration Tests

See [integration/README.md](integration/README.md) for guidance on writing integration tests.

### Benchmarks

See [benchmarks/README.md](benchmarks/README.md) for guidance on writing performance benchmarks.
