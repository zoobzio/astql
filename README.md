# ASTQL - Type-Safe Query Builder

ASTQL is a minimal, type-safe Abstract Syntax Tree (AST) based query builder designed for AI agents and secure query generation.

## Core Principles

1. **No String Literals** - All table names, field names, and parameters must go through validated functions
2. **Validation at Creation** - Invalid fields/tables cause immediate panics, not runtime SQL errors
3. **Parameters Only** - User values can only be passed as parameters, never as raw SQL
4. **Clean Separation** - AST structure is separate from SQL rendering (providers)

## Basic Usage

```go
// Setup: Register your models with Sentinel for validation
astql.SetupTestModels()

// SELECT specific fields
query := astql.Select(astql.T("users")).
    Fields(
        astql.F("id"),
        astql.F("name"),
        astql.F("email"),
    ).
    Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
    OrderBy(astql.F("name"), astql.ASC).
    Limit(10).
    MustBuild()

// SELECT * (no Fields() call)
query := astql.Select(astql.T("users")).
    Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
    MustBuild()

// INSERT query
query := astql.Insert(astql.T("users")).
    Values(map[astql.Field]astql.Param{
        astql.F("name"):  astql.P("userName"),
        astql.F("email"): astql.P("userEmail"),
        astql.F("age"):   astql.P("userAge"),
    }).
    MustBuild()

// UPDATE query
query := astql.Update(astql.T("users")).
    Set(astql.F("name"), astql.P("newName")).
    Set(astql.F("email"), astql.P("newEmail")).
    Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
    MustBuild()

// DELETE query
query := astql.Delete(astql.T("users")).
    Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
    MustBuild()

// COUNT query
query := astql.Count(astql.T("users")).
    Where(astql.C(astql.F("age"), astql.GE, astql.P("minAge"))).
    MustBuild()
```

## Core Functions

### Table Creation
- `T(name string, alias ...string)` - Creates a validated table reference
  - Validates table name against Sentinel registry
  - Panics if table is not registered

### Field Creation  
- `F(name string)` - Creates a validated field reference
  - Validates field name against Sentinel registry
  - Panics if field is not registered
  - Use `.WithTable(alias)` to add table prefix

### Parameter Creation
- `P(name string)` - Creates a named parameter reference
- `P1()`, `P2()`, etc. - Positional parameter shortcuts

### Condition Building
- `C(field, operator, param)` - Creates a simple condition
- `And(conditions...)` - Combines conditions with AND logic
- `Or(conditions...)` - Combines conditions with OR logic

## Complex WHERE Clauses

```go
// Nested AND/OR conditions
query := astql.Select(astql.T("users")).
    Where(
        astql.Or(
            astql.C(astql.F("age"), astql.GT, astql.P("minAge")),
            astql.And(
                astql.C(astql.F("name"), astql.LIKE, astql.P("namePattern")),
                astql.C(astql.F("email"), astql.NOT_LIKE, astql.P("emailPattern")),
            ),
        ),
    ).
    MustBuild()
```

## Security Model

1. **Field Validation**: All field names are validated against structs registered with Sentinel
2. **Table Validation**: Table names are derived from registered struct types
3. **No Raw SQL**: It's impossible to inject raw SQL strings - everything goes through type-safe functions
4. **Parameters Only**: User input can only be passed through parameters, which will be properly escaped by the SQL driver

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
astql.F("id")     // ✓
astql.F("name")   // ✓
astql.F("email")  // ✓

// This will panic:
astql.F("invalid_field") // ✗ panic: invalid field
```

## Next Steps

This is the minimal core implementation. Future additions:
- SQL providers (PostgreSQL, MySQL, etc.) that render AST to SQL
- Support for JOINs, subqueries, CTEs
- Aggregate functions and GROUP BY
- More operators (BETWEEN, EXISTS, etc.)
- Schema generation from AST