# astql

[![CI](https://github.com/zoobzio/astql/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/zoobzio/astql/branch/main/graph/badge.svg)](https://codecov.io/gh/zoobzio/astql)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/astql)](https://goreportcard.com/report/github.com/zoobzio/astql)
[![CodeQL](https://github.com/zoobzio/astql/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/astql.svg)](https://pkg.go.dev/github.com/zoobzio/astql)
[![License](https://img.shields.io/github/license/zoobzio/astql)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/astql)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/astql)](https://github.com/zoobzio/astql/releases)

Type-safe SQL query builder with DBML schema validation.

Build queries as an AST, validate against your schema, render to parameterized SQL. Supports PostgreSQL, SQLite, MySQL, and SQL Server.

## The Problem

SQL query builders in Go typically accept arbitrary strings:

```go
// Dangerous: field names from user input
db.Select(userFields...).Where(userColumn + " = ?", value)
```

This creates SQL injection vectors. Even with parameterized values, untrusted column names or table names can be exploited. And when you need to support multiple databases, you end up with string interpolation for dialect differences—another injection surface.

## The Solution

ASTQL builds queries as an Abstract Syntax Tree, validated against your DBML schema:

```go
import "github.com/zoobzio/astql/pkg/postgres"

// 1. Define schema
instance, _ := astql.NewFromDBML(project)

// 2. Build query
query := astql.Select(instance.T("users")).
    Fields(instance.F("username"), instance.F("email")).
    Where(instance.C(instance.F("active"), astql.EQ, instance.P("is_active"))).
    Limit(10)

// 3. Render SQL
result, _ := query.Render(postgres.New())
// result.SQL: SELECT "username", "email" FROM "users" WHERE "active" = :is_active LIMIT 10
// result.RequiredParams: []string{"is_active"}
```

You get:

- **Schema validation** — `T("users")` and `F("email")` checked against DBML
- **Parameterized output** — values are placeholders, never interpolated
- **Dialect rendering** — same AST renders to PostgreSQL, SQLite, MySQL, or SQL Server

No string concatenation. No injection vulnerabilities.

## Features

- **Schema-validated** — Tables and fields checked against DBML at build time
- **Injection-resistant** — Parameterized queries, quoted identifiers, no string interpolation
- **Multi-provider** — PostgreSQL, SQLite, MySQL, SQL Server with dialect-specific rendering
- **Type-safe** — Instance-based API prevents direct struct construction
- **Composable** — Subqueries, JOINs, CASE expressions, aggregates, string/date functions

## Use Cases

- [Implement pagination](docs/4.cookbook/1.pagination.md) — LIMIT/OFFSET and cursor patterns
- [Add vector search](docs/4.cookbook/2.vector-search.md) — pgvector similarity queries
- [Handle upserts](docs/4.cookbook/3.upserts.md) — ON CONFLICT patterns
- [Build type-safe ORMs](docs/4.cookbook/4.orm-foundation.md) — power query builders like cereal

## Install

```bash
go get github.com/zoobzio/astql
go get github.com/zoobzio/dbml
```

Requires Go 1.24+.

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/zoobzio/astql"
    "github.com/zoobzio/astql/pkg/postgres"
    "github.com/zoobzio/dbml"
)

func main() {
    // Define schema
    project := dbml.NewProject("myapp")
    users := dbml.NewTable("users")
    users.AddColumn(dbml.NewColumn("id", "bigint"))
    users.AddColumn(dbml.NewColumn("username", "varchar"))
    users.AddColumn(dbml.NewColumn("email", "varchar"))
    project.AddTable(users)

    // Create instance
    instance, err := astql.NewFromDBML(project)
    if err != nil {
        panic(err)
    }

    // Build and render
    result, err := astql.Select(instance.T("users")).
        Fields(instance.F("username"), instance.F("email")).
        OrderBy(instance.F("username"), astql.ASC).
        Render(postgres.New())

    if err != nil {
        panic(err)
    }

    fmt.Println(result.SQL)
    // SELECT "username", "email" FROM "users" ORDER BY "username" ASC
}
```

## API Reference

| Function                       | Purpose                           |
| ------------------------------ | --------------------------------- |
| `NewFromDBML(project)`         | Create instance from DBML schema  |
| `Select(table)`                | Start a SELECT query              |
| `Insert(table)`                | Start an INSERT query             |
| `Update(table)`                | Start an UPDATE query             |
| `Delete(table)`                | Start a DELETE query              |
| `Count(table)`                 | Start a COUNT query               |
| `instance.T(name)`             | Get validated table reference     |
| `instance.F(name)`             | Get validated field reference     |
| `instance.P(name)`             | Get validated parameter reference |
| `instance.C(field, op, value)` | Create condition                  |
| `instance.And(conditions...)`  | Combine with AND                  |
| `instance.Or(conditions...)`   | Combine with OR                   |
| `builder.Render(provider)`     | Render to SQL                     |

See [API Reference](docs/5.reference/1.api.md) for complete documentation.

## Schema Validation

Queries are validated against your DBML schema at construction time:

```go
instance.T("users")                        // Valid table
instance.T("users; DROP TABLE users--")    // Panics: table not in schema

instance.F("email")                        // Valid field
instance.F("id' OR '1'='1")                // Panics: field not in schema
```

Use `Try` variants for runtime validation:

```go
table, err := instance.TryT(userInput)     // Returns error instead of panic
field, err := instance.TryF(fieldName)
```

## Providers

Use `Render()` with the appropriate provider for your database:

```go
import (
    "github.com/zoobzio/astql/pkg/postgres"
    "github.com/zoobzio/astql/pkg/sqlite"
    "github.com/zoobzio/astql/pkg/mysql"
    "github.com/zoobzio/astql/pkg/mssql"
)

result, _ := query.Render(postgres.New())  // PostgreSQL
result, _ := query.Render(sqlite.New())    // SQLite
result, _ := query.Render(mysql.New())     // MySQL
result, _ := query.Render(mssql.New())     // SQL Server
```

Each provider handles dialect differences automatically (quoting, date functions, pagination syntax, etc.).

## Documentation

- [Overview](docs/1.overview.md) — what astql does and why
- **Learn**
  - [Quickstart](docs/2.learn/1.quickstart.md) — get started in minutes
  - [Concepts](docs/2.learn/2.concepts.md) — tables, fields, params, conditions, builders
  - [Architecture](docs/2.learn/3.architecture.md) — AST structure, render pipeline, security layers
- **Guides**
  - [Schema Validation](docs/3.guides/1.schema-validation.md) — DBML integration and validation
  - [Conditions](docs/3.guides/2.conditions.md) — WHERE, AND/OR, subqueries, BETWEEN
  - [Joins](docs/3.guides/3.joins.md) — INNER, LEFT, RIGHT, CROSS joins
  - [Aggregates](docs/3.guides/4.aggregates.md) — GROUP BY, HAVING, window functions
  - [Testing](docs/3.guides/5.testing.md) — testing patterns for query builders
- **Cookbook**
  - [ORM Foundation](docs/4.cookbook/4.orm-foundation.md) — building type-safe ORMs with cereal
  - [Pagination](docs/4.cookbook/1.pagination.md) — LIMIT/OFFSET and cursor patterns
  - [Vector Search](docs/4.cookbook/2.vector-search.md) — pgvector similarity queries
  - [Upserts](docs/4.cookbook/3.upserts.md) — ON CONFLICT patterns
- **Reference**
  - [API](docs/5.reference/1.api.md) — complete function documentation
  - [Operators](docs/5.reference/2.operators.md) — all comparison and special operators

## Contributing

Contributions welcome! Please ensure:

- Tests pass: `make test`
- Code is formatted: `go fmt ./...`
- No lint errors: `make lint`

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

MIT License — see [LICENSE](LICENSE) for details.
