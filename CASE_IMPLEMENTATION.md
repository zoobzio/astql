# CASE Expression Implementation

## Overview
We successfully implemented SQL CASE expressions while maintaining our security-first approach.

## Key Features

### 1. Type-Safe API
```go
Case().
    When(condition, result).
    When(condition, result).
    Else(result).
    As(alias).
    Build()
```

### 2. Security Maintained
- **Conditions**: Reuse existing validated `ConditionItem` interface
- **Results**: Parameter references only (`Param`), no literals
- **Aliases**: Validated against registered field aliases
- **No injection vectors**: All values remain parameterized

### 3. Supported Patterns

#### Basic CASE in SELECT
```go
PostgresSelect(T("users")).
    Fields(F("name")).
    SelectCase(
        Case().
            When(C(F("age"), LT, P("minorAge")), P("minorLabel")).
            When(C(F("age"), LT, P("seniorAge")), P("adultLabel")).
            Else(P("seniorLabel")).
            As("age_group").
            Build(),
    )
```

Generates:
```sql
SELECT name, 
    CASE 
        WHEN age < :minorAge THEN :minorLabel
        WHEN age < :seniorAge THEN :adultLabel
        ELSE :seniorLabel
    END AS age_group
FROM users
```

#### Complex Conditions
```go
When(
    And(
        C(F("age"), GE, P("minAge")),
        C(F("age"), LE, P("maxAge")),
    ),
    P("inRangeLabel"),
)
```

#### Field Comparisons in CASE
```go
When(CF(F("created_at"), GT, F("updated_at")), P("recentlyCreated"))
```

### 4. Implementation Details

- `CaseExpression` type with `WhenClause` array
- `CaseBuilder` for fluent API
- Implements `ConditionItem` for future WHERE clause support
- Integrated with `FieldExpression` for SELECT clauses
- Context-aware rendering for proper parameter handling

### 5. Current Limitations

1. **CASE in WHERE**: Not yet implemented (requires wrapping in a comparison condition)
2. **Schema support**: Requires PostgreSQL-specific schema builder
3. **Nested CASE**: Not tested but should work

### 6. Security Analysis

✅ **No new attack surface**: Reuses validated components
✅ **Parameter-only values**: Consistent with security model  
✅ **Validated aliases**: Prevents arbitrary string injection
✅ **Type-safe API**: Compile-time validation

## Usage Examples

### Age Categorization
```go
params := map[string]interface{}{
    "minorAge": 18,
    "minorLabel": "Minor",
    "seniorAge": 65,
    "adultLabel": "Adult",
    "seniorLabel": "Senior",
}
```

### Status Mapping
```go
Case().
    When(C(F("status"), EQ, P("pendingStatus")), P("pendingLabel")).
    When(C(F("status"), EQ, P("activeStatus")), P("activeLabel")).
    Else(P("unknownLabel")).
    As("status_text")
```

## Future Enhancements

1. Support CASE in WHERE clause with comparison wrapper
2. Add PostgreSQL-specific schema builder for declarative CASE
3. Support for searched CASE (without initial expression)
4. Validate reasonable nesting depth for CASE expressions