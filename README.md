[![CI](https://github.com/zoobzio/astql/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/zoobzio/astql/branch/main/graph/badge.svg)](https://codecov.io/gh/zoobzio/astql)
[![CodeQL](https://github.com/zoobzio/astql/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobzio/astql/actions/workflows/codeql.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/astql)](https://goreportcard.com/report/github.com/zoobzio/astql)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/astql.svg)](https://pkg.go.dev/github.com/zoobzio/astql)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# ASTQL - Abstract Syntax Tree Query Language

ASTQL is a secure, type-safe SQL query builder for Go that uses an Abstract Syntax Tree (AST) approach to prevent SQL injection attacks. It provides a fluent API for building complex SQL queries programmatically while maintaining strict validation and type safety.

## Key Features

- **Security First**: Prevents SQL injection through strict validation and AST-based query construction
- **Type Safety**: All query components are strongly typed with compile-time checks
- **Provider Pattern**: Extensible architecture supporting multiple database engines
- **Schema Builder**: YAML/JSON to SQL query generation for zero-deployment environments
- **Field Registration**: Automatic field discovery and validation via Sentinel integration
- **Rich Query Support**: Complex queries including JOINs, subqueries, CTEs, and more
- **Error Handling**: Try variants for graceful error handling instead of panics

## Core Principles

1. **No String Literals** - All table names, field names, and parameters must go through validated functions
2. **Validation at Creation** - Invalid fields/tables cause immediate panics, not runtime SQL errors
3. **Parameters Only** - User values can only be passed as parameters, never as raw SQL
4. **Clean Separation** - AST structure is separate from SQL rendering (providers)
5. **Exact Naming** - Table names must exactly match struct names (e.g., `User` not `users`)

## Installation

```bash
go get github.com/zoobzio/astql
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/zoobzio/astql"
    "github.com/zoobzio/astql/providers/postgres"
)

func main() {
    // Define tables and fields - must match struct names exactly
    users := astql.T("User", "u")
    posts := astql.T("Post", "p")
    
    // Build a complex query
    query := postgres.Select(users).
        Fields(
            astql.F("id"),
            astql.F("username"),
            astql.F("email"),
        ).
        InnerJoin(posts, astql.C(astql.F("id").WithTable("u"), astql.EQ, astql.F("user_id").WithTable("p"))).
        Where(astql.And(
            astql.C(astql.F("active"), astql.EQ, astql.P("active")),
            astql.C(astql.F("created_at"), astql.GE, astql.P("since")),
        )).
        OrderBy(astql.F("created_at"), astql.DESC).
        Limit(10)
    
    // Build the query
    ast, err := query.Build()
    if err != nil {
        panic(err)
    }
    
    // Generate SQL
    sql, params := postgres.NewProvider().ToSQL(ast)
    fmt.Println(sql)
    // Output: SELECT u.id, u.username, u.email FROM User AS u INNER JOIN Post AS p ON u.id = p.user_id WHERE (u.active = $1 AND u.created_at >= $2) ORDER BY u.created_at DESC LIMIT 10
}
```

## Basic Usage Examples

```go
// SELECT specific fields
query := postgres.Select(astql.T("User")).
    Fields(
        astql.F("id"),
        astql.F("name"),
        astql.F("email"),
    ).
    Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
    OrderBy(astql.F("name"), astql.ASC).
    Limit(10)

// INSERT with RETURNING
query := postgres.Insert(astql.T("User")).
    Values(map[types.Field]types.Param{
        astql.F("name"):  astql.P("userName"),
        astql.F("email"): astql.P("userEmail"),
        astql.F("age"):   astql.P("userAge"),
    }).
    Returning(astql.F("id"), astql.F("created_at"))

// UPDATE with complex WHERE
query := postgres.Update(astql.T("User")).
    Set(astql.F("name"), astql.P("newName")).
    Set(astql.F("updated_at"), astql.P("now")).
    Where(astql.And(
        astql.C(astql.F("id"), astql.EQ, astql.P("userId")),
        astql.C(astql.F("active"), astql.EQ, astql.P("true")),
    ))

// DELETE with JOIN
query := postgres.Delete(astql.T("posts", "p")).
    Using(astql.T("users", "u")).
    Where(astql.And(
        astql.C(astql.F("user_id").WithTable("p"), astql.EQ, astql.F("id").WithTable("u")),
        astql.C(astql.F("banned").WithTable("u"), astql.EQ, astql.P("true")),
    ))
```

## Schema Builder

The schema builder allows you to define queries using YAML or JSON, perfect for LLM-generated queries:

```yaml
operation: SELECT
table: users
alias: u
fields:
  - id
  - username
  - email
where:
  logic: AND
  conditions:
    - field: active
      operator: "="
      param: active
    - field: role
      operator: IN
      param: roles
order_by:
  - field: created_at
    direction: DESC
limit: 20
```

```go
// Parse and build query from schema
var schema postgres.QuerySchema
if err := yaml.Unmarshal(yamlData, &schema); err != nil {
    return err
}

ast, err := postgres.BuildFromSchema(&schema)
if err != nil {
    return err
}

sql, params := postgres.NewProvider().ToSQL(ast)
```

## Security Features

### Internal Types Package
All core types are defined in an internal package, preventing external users from creating struct literals directly:

```go
// This will NOT compile for external users:
// field := types.Field{Name: "users"} // ❌ Cannot access internal package

// Must use validated constructors:
field := astql.F("username") // ✅ Validated and safe
```

### Try Variants
For handling untrusted input (like schemas from LLMs), use Try variants that return errors instead of panicking:

```go
// Regular variant - panics on invalid input
field := astql.F("invalid-field-name!") // Panics!

// Try variant - returns error
field, err := astql.TryF("invalid-field-name!")
if err != nil {
    // Handle error gracefully
    return fmt.Errorf("invalid field: %w", err)
}
```

### Comprehensive Validation
- Table names must be alphanumeric with underscores
- Field names follow strict naming conventions
- Parameter names are validated against injection patterns
- All operators and functions are allowlisted

## Provider Support

Currently supported:
- **PostgreSQL**: Full support including ON CONFLICT, RETURNING, and advanced SQL features
- **SQLite**: Support for core features with OR IGNORE/REPLACE, RETURNING (3.35.0+), and adapted SQL features

Coming soon:
- MySQL/MariaDB
- Microsoft SQL Server

## Advanced Features

### Subqueries
```go
subquery := postgres.Select(astql.T("Order")).
    Fields(astql.F("user_id")).
    Where(astql.C(astql.F("total"), astql.GT, astql.P("min_total")))

mainQuery := postgres.Select(astql.T("users")).
    Fields(astql.F("*")).
    Where(astql.CSub(astql.F("id"), astql.IN, subquery))
```

### CASE Expressions
```go
caseExpr := postgres.Case().
    When(astql.C(astql.F("age"), astql.LT, astql.P("adult_age")), astql.P("minor")).
    When(astql.C(astql.F("age"), astql.LT, astql.P("senior_age")), astql.P("adult")).
    Else(astql.P("senior"))

query := postgres.Select(users).SelectExpr(caseExpr.As("age_group"))
```

## Integration with Sentinel

ASTQL uses [Sentinel](https://github.com/zoobzio/sentinel) for struct metadata extraction and validation:

```go
// Your model with db tags
type User struct {
    ID    int    `db:"id"`
    Name  string `db:"name"`
    Email string `db:"email"`
    Age   int    `db:"age"`
}

// Register with Sentinel (usually in init or setup)
admin := sentinel.NewAdmin()
admin.Seal()
sentinel.Inspect[User]()

// Now these are valid:
astql.T("User")   // ✓ Exact struct name
astql.F("id")     // ✓
astql.F("name")   // ✓
astql.F("email")  // ✓

// These will panic:
astql.T("users")         // ✗ panic: table 'users' not found (must use "User")
astql.F("invalid_field") // ✗ panic: invalid field
```

## Error Handling Philosophy

ASTQL follows a **fail-fast** approach for query construction:

### Panics (Immediate Failures)
These indicate programming errors that should be caught during development:
- Invalid table names (not registered with Sentinel)
- Invalid field names (not in struct)
- Invalid parameter names (SQL keywords, special characters)
- Invalid aliases (not single lowercase letter for tables)

```go
// These panic immediately:
astql.T("nonexistent")  // panic: table not found
astql.F("bad_field")    // panic: field not found
astql.P("SELECT")       // panic: SQL keyword not allowed
```

### Try Variants (Graceful Errors)
For dynamic query construction, use Try variants:

```go
table, err := astql.TryT(userInput)  // Returns error instead of panic
field, err := astql.TryF(fieldName)  // Safe for runtime validation
param, err := astql.TryP(paramName)  // Validates parameter names
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

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to get started.

### Security

For security vulnerabilities, please see our [Security Policy](SECURITY.md) for responsible disclosure guidelines. Please do not create public issues for security vulnerabilities.

## License

ASTQL is released under the [MIT License](LICENSE).