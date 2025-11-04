# ASTQL - Schema-Validated SQL Query Builder

[![CI](https://github.com/zoobzio/astql/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/zoobzio/astql/branch/main/graph/badge.svg)](https://codecov.io/gh/zoobzio/astql)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/astql)](https://goreportcard.com/report/github.com/zoobzio/astql)
[![CodeQL](https://github.com/zoobzio/astql/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/astql.svg)](https://pkg.go.dev/github.com/zoobzio/astql)
[![License](https://img.shields.io/github/license/zoobzio/astql)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/astql)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/astql)](https://github.com/zoobzio/astql/releases)

ASTQL is a **SQL injection-resistant** PostgreSQL query builder for Go that validates all queries against a DBML schema before rendering. It uses an Abstract Syntax Tree (AST) approach to prevent SQL injection attacks through schema validation and parameterized queries.

## Key Features

- ✅ **SQL Injection Prevention**: Schema validation blocks injection attempts at query construction time
- ✅ **DBML Schema Integration**: Validates tables and fields against your DBML schema
- ✅ **PostgreSQL Focused**: Optimized for PostgreSQL with full feature support
- ✅ **Type Safety**: Instance-based API prevents direct struct creation
- ✅ **Rich Query Support**: JOINs, subqueries, aggregates, CASE expressions, and more
- ✅ **Parameterized Queries**: All user values use named parameters (`:param` style)
- ✅ **Comprehensive Testing**: 85%+ test coverage with security-focused tests

## Installation

```bash
go get github.com/zoobzio/astql
go get github.com/zoobzio/dbml  # For schema definitions
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/zoobzio/astql"
    "github.com/zoobzio/dbml"
)

func main() {
    // Define your schema using DBML
    project := dbml.NewProject("myapp")

    users := dbml.NewTable("users")
    users.AddColumn(dbml.NewColumn("id", "bigint"))
    users.AddColumn(dbml.NewColumn("username", "varchar"))
    users.AddColumn(dbml.NewColumn("email", "varchar"))
    users.AddColumn(dbml.NewColumn("active", "boolean"))
    project.AddTable(users)

    // Create ASTQL instance from schema
    instance, err := astql.NewFromDBML(project)
    if err != nil {
        panic(err)
    }

    // Build a query - all tables/fields validated against schema
    result, err := astql.Select(instance.T("users")).
        Fields(instance.F("username"), instance.F("email")).
        Where(instance.C(instance.F("active"), "=", instance.P("is_active"))).
        OrderBy(instance.F("username"), "ASC").
        Limit(10).
        Render()

    if err != nil {
        panic(err)
    }

    fmt.Println(result.SQL)
    // Output: SELECT "username", "email" FROM "users" WHERE "active" = :is_active ORDER BY "username" ASC LIMIT 10

    fmt.Println(result.RequiredParams)
    // Output: [is_active]
}
```

## Core Concepts

### Instance-Based Validation

ASTQL uses **instance-based validation** - each instance is bound to a specific DBML schema:

```go
// All types created through the instance
instance.T("users")       // Table - validates against schema
instance.F("email")       // Field - validates against schema
instance.P("user_email")  // Parameter - validates identifier
instance.C(field, op, param)  // Condition - validates field exists

// This WON'T compile (types are internal):
// types.Table{Name: "users"}  // ❌ Cannot access internal package
```

### Security by Design

**Schema Validation Prevents Injection:**
```go
// Valid query - table/field exist in schema
instance.T("users")           // ✅ Validated
instance.F("email")           // ✅ Validated

// Injection attempts caught at construction
instance.T("users; DROP TABLE users--")  // ❌ Panics: table not found in schema
instance.F("id OR 1=1")                  // ❌ Panics: field not found in schema
instance.P("id; DELETE FROM users")      // ❌ Panics: invalid parameter name
```

**Defense in Depth:**
1. Schema validation (tables/fields must exist)
2. Identifier validation (alphanumeric + underscore only)
3. SQL keyword blocking (`;`, `--`, `'`, `OR`, `AND`, etc.)
4. Quoted identifiers (prevents injection via special chars)
5. Parameterized queries (values never interpolated)

## Query Examples

### SELECT with WHERE and JOIN

```go
result, _ := astql.Select(instance.T("users", "u")).
    Fields(
        instance.WithTable(instance.F("username"), "u"),
        instance.WithTable(instance.F("title"), "p"),
    ).
    InnerJoin(
        instance.T("posts", "p"),
        astql.CF(
            instance.WithTable(instance.F("id"), "u"),
            "=",
            instance.WithTable(instance.F("user_id"), "p"),
        ),
    ).
    Where(instance.And(
        instance.C(instance.WithTable(instance.F("active"), "u"), "=", instance.P("is_active")),
        instance.C(instance.WithTable(instance.F("published"), "p"), "=", instance.P("is_published")),
    )).
    Render()

// SELECT u."username", p."title" FROM "users" u
// INNER JOIN "posts" p ON u."id" = p."user_id"
// WHERE (u."active" = :is_active AND p."published" = :is_published)
```

### INSERT with ON CONFLICT

```go
result, _ := astql.Insert(instance.T("users")).
    Values(map[types.Field]types.Param{
        instance.F("username"): instance.P("username"),
        instance.F("email"):    instance.P("email"),
    }).
    OnConflict(instance.F("email")).
    DoUpdate().
    Set(instance.F("username"), instance.P("username")).
    Build().
    Returning(instance.F("id")).
    Render()

// INSERT INTO "users" ("email", "username") VALUES (:email, :username)
// ON CONFLICT ("email") DO UPDATE SET "username" = :username
// RETURNING "id"
```

### UPDATE with RETURNING

```go
result, _ := astql.Update(instance.T("users")).
    Set(instance.F("active"), instance.P("new_status")).
    Where(instance.C(instance.F("id"), "=", instance.P("user_id"))).
    Returning(instance.F("updated_at")).
    Render()

// UPDATE "users" SET "active" = :new_status
// WHERE "id" = :user_id
// RETURNING "updated_at"
```

### Aggregates with GROUP BY

```go
result, _ := astql.Select(instance.T("orders")).
    Fields(instance.F("user_id")).
    SelectExpr(astql.As(astql.Sum(instance.F("total")), "total_spent")).
    SelectExpr(astql.As(astql.CountField(instance.F("id")), "order_count")).
    GroupBy(instance.F("user_id")).
    Having(
        instance.C(instance.F("total"), ">", instance.P("min_total")),
    ).
    Render()

// SELECT "user_id", SUM("total") AS "total_spent", COUNT("id") AS "order_count"
// FROM "orders"
// GROUP BY "user_id"
// HAVING "total" > :min_total
```

### Subqueries

```go
// Nested subquery with automatic parameter namespacing
subquery := astql.Sub(
    astql.Select(instance.T("posts")).
        Fields(instance.F("user_id")).
        Where(instance.C(instance.F("published"), "=", instance.P("is_published"))),
)

result, _ := astql.Select(instance.T("users")).
    Where(astql.CSub(instance.F("id"), types.IN, subquery)).
    Render()

// SELECT * FROM "users"
// WHERE "id" IN (SELECT "user_id" FROM "posts" WHERE "published" = :sq1_is_published)
// Parameters are automatically prefixed (sq1_, sq2_, sq3_) to prevent collisions
```

### CASE Expressions

```go
caseExpr := astql.Case().
    When(instance.C(instance.F("age"), "<", instance.P("young_age")), instance.P("young")).
    When(instance.C(instance.F("age"), ">=", instance.P("old_age")), instance.P("old")).
    Else(instance.P("middle")).
    As("age_group").
    Build()

result, _ := astql.Select(instance.T("users")).
    Fields(instance.F("username")).
    SelectExpr(caseExpr).
    Render()

// SELECT "username",
// CASE WHEN "age" < :young_age THEN :young
//      WHEN "age" >= :old_age THEN :old
//      ELSE :middle END AS "age_group"
// FROM "users"
```

### Math Functions

```go
result, _ := astql.Select(instance.T("products")).
    SelectExpr(astql.As(astql.Round(instance.F("price"), instance.P("decimals")), "rounded_price")).
    SelectExpr(astql.As(astql.Floor(instance.F("rating")), "floor_rating")).
    SelectExpr(astql.As(astql.Power(instance.F("quantity"), instance.P("exponent")), "qty_squared")).
    Render()

// SELECT ROUND("price", :decimals) AS "rounded_price",
//        FLOOR("rating") AS "floor_rating",
//        POWER("quantity", :exponent) AS "qty_squared"
// FROM "products"
```

## Supported Features

### Query Types
- SELECT (with DISTINCT)
- INSERT (with ON CONFLICT, RETURNING)
- UPDATE (with RETURNING)
- DELETE (with RETURNING)
- COUNT

### JOINs
- INNER JOIN
- LEFT JOIN
- RIGHT JOIN
- CROSS JOIN

### Expressions
- Aggregates: SUM, AVG, MIN, MAX, COUNT, COUNT(DISTINCT)
- CASE/WHEN/THEN/ELSE
- COALESCE
- NULLIF
- Math: ROUND, FLOOR, CEIL, ABS, POWER, SQRT

### Advanced Features
- Subqueries (IN, NOT IN, EXISTS, NOT EXISTS)
- Nested subqueries (up to 3 levels)
- GROUP BY and HAVING
- ORDER BY with ASC/DESC
- LIMIT and OFFSET
- Field comparisons (field-to-field conditions)
- NULL checks (IS NULL, IS NOT NULL)

## Security Features

### What's Protected

✅ **Table names** - Must exist in schema, always quoted
✅ **Field names** - Must exist in schema, always quoted
✅ **Parameter names** - Validated, no SQL keywords
✅ **Parameter values** - Parameterized queries only
✅ **Operators** - Enum-based, no user control
✅ **Aliases** - Validated and quoted
✅ **Table aliases** - Restricted to single letters (a-z)

### SQL Injection Prevention

The package prevents SQL injection through multiple layers:

1. **Schema Validation**: All tables and fields must exist in your DBML schema
2. **Identifier Validation**: Strict alphanumeric + underscore rules
3. **SQL Keyword Blocking**: Rejects `;`, `--`, `'`, `"`, `OR`, `AND`, etc.
4. **Quoted Identifiers**: PostgreSQL double-quote escaping
5. **Parameterized Queries**: Named parameters (`:param`), never string interpolation

Example of blocked injection attempts:
```go
instance.T("users; DROP TABLE users--")  // ❌ Panics: table not in schema
instance.F("id' OR '1'='1")              // ❌ Panics: field not in schema
instance.P("id; DELETE FROM users")      // ❌ Panics: invalid identifier
astql.As(expr, "x; DROP TABLE users")   // ❌ Panics: invalid alias
```

## Error Handling

ASTQL uses a **fail-fast** approach:

### Panics (Development Errors)
These indicate programming errors caught during query construction:
```go
instance.T("nonexistent")  // panic: table not found in schema
instance.F("bad_field")    // panic: field not found in schema
instance.P("invalid-name") // panic: invalid parameter name
```

### Try Variants (Runtime Validation)
For dynamic input, use Try variants that return errors:
```go
table, err := instance.TryT(userInput)    // Returns error instead of panic
field, err := instance.TryF(fieldName)    // Safe for runtime validation
param, err := instance.TryP(paramName)    // Validates parameter names
```

## Testing

```bash
# Run all tests
make test

# Run with race detector
make test-race

# Generate coverage report
make coverage

# Run linter
make lint

# Full CI simulation
make ci
```

**Current Test Coverage: 85%+** with 128 passing tests including comprehensive SQL injection prevention tests.

## Architecture

```
astql/
├── instance.go          # DBML schema validation
├── builder.go           # Query builder (SELECT, INSERT, etc.)
├── expressions.go       # Expression helpers (SUM, CASE, etc.)
├── render.go            # PostgreSQL SQL generation
├── internal/types/      # Internal AST types (not accessible externally)
│   ├── ast.go
│   ├── field.go
│   ├── table.go
│   └── operator.go
└── *_test.go            # Comprehensive test suite
```

## Comparison with Other Builders

| Feature | ASTQL | GORM | sqlx | squirrel |
|---------|-------|------|------|----------|
| SQL Injection Prevention | ✅ Schema validation | ⚠️ Trust developer | ⚠️ Trust developer | ⚠️ Trust developer |
| Schema Validation | ✅ DBML-based | ❌ Runtime only | ❌ None | ❌ None |
| Type Safety | ✅ Instance-based | ⚠️ Reflection | ❌ String-based | ❌ String-based |
| PostgreSQL Features | ✅ Full support | ✅ Full | ✅ Full | ✅ Full |
| Subquery Support | ✅ With depth limits | ✅ Yes | ✅ Yes | ✅ Yes |
| Learning Curve | Medium | High | Low | Low |

## Roadmap

- [ ] Add support for CTEs (WITH clauses)
- [ ] Window functions (OVER, PARTITION BY)
- [ ] Array operations (PostgreSQL arrays)
- [ ] JSON/JSONB operations
- [ ] Full-text search support
- [ ] Query plan analysis helpers
- [ ] Additional database support (MySQL, SQLite)

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Security

For security vulnerabilities, please see our [Security Policy](SECURITY.md) for responsible disclosure guidelines. Please do not create public issues for security vulnerabilities.

## License

ASTQL is released under the [MIT License](LICENSE).

---

**Note**: This package is PostgreSQL-focused. While the architecture could support other databases, current development prioritizes PostgreSQL features and security.
