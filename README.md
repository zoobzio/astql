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

Build queries as an AST, validate against your schema, render to parameterized SQL.

## Build, Validate, Render

```go
// Build
query := astql.Select(instance.T("users")).
    Fields(instance.F("username"), instance.F("email")).
    Where(instance.C(instance.F("active"), astql.EQ, instance.P("is_active")))

// Validate — T(), F(), P() check against your DBML schema

// Render
result, _ := query.Render(postgres.New())
// SELECT "username", "email" FROM "users" WHERE "active" = :is_active
```

Tables and fields validated at construction. Values always parameterized. Identifiers quoted per dialect. Use `TryT`, `TryF`, `TryP` for runtime validation with error returns.

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
    users.AddColumn(dbml.NewColumn("active", "boolean"))
    project.AddTable(users)

    // Create instance
    instance, err := astql.NewFromDBML(project)
    if err != nil {
        panic(err)
    }

    // Build and render
    result, err := astql.Select(instance.T("users")).
        Fields(instance.F("username"), instance.F("email")).
        Where(instance.C(instance.F("active"), astql.EQ, instance.P("is_active"))).
        OrderBy(instance.F("username"), astql.ASC).
        Limit(10).
        Render(postgres.New())

    if err != nil {
        panic(err)
    }

    fmt.Println(result.SQL)
    // SELECT "username", "email" FROM "users" WHERE "active" = :is_active ORDER BY "username" ASC LIMIT 10
    fmt.Println(result.RequiredParams)
    // [is_active]
}
```

## Providers

Same AST, different dialects:

```go
import (
    "github.com/zoobzio/astql/pkg/postgres"
    "github.com/zoobzio/astql/pkg/sqlite"
    "github.com/zoobzio/astql/pkg/mariadb"
    "github.com/zoobzio/astql/pkg/mssql"
)

result, _ := query.Render(postgres.New())  // PostgreSQL
result, _ := query.Render(sqlite.New())    // SQLite
result, _ := query.Render(mariadb.New())   // MariaDB
result, _ := query.Render(mssql.New())     // SQL Server
```

Each provider handles dialect differences — identifier quoting, pagination syntax, date functions, vendor-specific operators.

## Why ASTQL?

- **Schema-validated** — `T("users")` and `F("email")` checked against DBML at build time
- **Injection-resistant** — parameterized values, quoted identifiers, no string concatenation
- **Multi-dialect** — one query, four databases
- **Composable** — subqueries, JOINs, aggregates, window functions, CASE expressions

## Documentation

- [Overview](docs/1.overview.md) — what astql does and why

**Learn**
- [Quickstart](docs/2.learn/1.quickstart.md) — get started in minutes
- [Concepts](docs/2.learn/2.concepts.md) — tables, fields, params, conditions, builders
- [Architecture](docs/2.learn/3.architecture.md) — AST structure, render pipeline, security layers

**Guides**
- [Schema Validation](docs/3.guides/1.schema-validation.md) — DBML integration and validation
- [Conditions](docs/3.guides/2.conditions.md) — WHERE, AND/OR, subqueries, BETWEEN
- [Joins](docs/3.guides/3.joins.md) — INNER, LEFT, RIGHT, CROSS joins
- [Aggregates](docs/3.guides/4.aggregates.md) — GROUP BY, HAVING, window functions
- [Testing](docs/3.guides/5.testing.md) — testing patterns for query builders

**Cookbook**
- [Pagination](docs/4.cookbook/1.pagination.md) — LIMIT/OFFSET and cursor patterns
- [Vector Search](docs/4.cookbook/2.vector-search.md) — pgvector similarity queries
- [Upserts](docs/4.cookbook/3.upserts.md) — ON CONFLICT patterns
- [ORM Foundation](docs/4.cookbook/4.orm-foundation.md) — building type-safe ORMs with cereal

**Reference**
- [API](docs/5.reference/1.api.md) — complete function documentation
- [Operators](docs/5.reference/2.operators.md) — all comparison and special operators

## Contributing

Contributions welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).
