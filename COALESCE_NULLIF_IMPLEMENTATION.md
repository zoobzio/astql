# COALESCE and NULLIF Implementation

## Overview
We successfully implemented COALESCE and NULLIF functions for null handling in SQL queries while maintaining our security-first approach.

## Key Features

### 1. COALESCE Function
Returns the first non-null value from a list of parameters.

**API:**
```go
Coalesce(values ...Param) CoalesceExpression
```

**Usage:**
```go
PostgresSelect(T("users")).
    Fields(F("id")).
    SelectCoalesce(
        Coalesce(P("nickname"), P("firstName"), P("defaultName")).
            As("display_name"),
    )
```

**Generates:**
```sql
SELECT id, COALESCE(:nickname, :firstName, :defaultName) AS display_name FROM users
```

### 2. NULLIF Function
Returns NULL if two values are equal, otherwise returns the first value.

**API:**
```go
NullIf(value1, value2 Param) NullIfExpression
```

**Usage:**
```go
PostgresSelect(T("users")).
    SelectNullIf(
        NullIf(P("status"), P("deletedStatus")).
            As("active_status"),
    )
```

**Generates:**
```sql
SELECT NULLIF(:status, :deletedStatus) AS active_status FROM users
```

## Implementation Details

### Type Structure
```go
type CoalesceExpression struct {
    Values []Param  // All parameters
    Alias  string   // Optional alias
}

type NullIfExpression struct {
    Value1 Param   // First value
    Value2 Param   // Comparison value
    Alias  string  // Optional alias
}
```

### Integration
- Added to `FieldExpression` for SELECT support
- `SelectCoalesce()` and `SelectNullIf()` methods on PostgresBuilder
- Context-aware rendering in PostgresProvider
- Alias validation against registered field aliases

## Security Analysis

✅ **No new attack vectors**: Only accepts Param references
✅ **Parameter-only values**: Consistent with security model
✅ **Validated aliases**: Prevents arbitrary string injection
✅ **Input validation**: COALESCE requires minimum 2 values

## Common Use Cases

### 1. Default Display Names
```go
// Show nickname, fall back to first name, then "Guest"
Coalesce(P("nickname"), P("firstName"), P("defaultName"))
```

### 2. Handling Deleted Records
```go
// Convert 'deleted' status to NULL for filtering
NullIf(P("status"), P("deletedValue"))
```

### 3. Combined with Other Features
```go
// Works with aggregates, CASE, and other expressions
PostgresSelect(T("users")).
    SelectExpr(Sum(F("amount")).As("total")).
    SelectCase(/* case expression */).
    SelectCoalesce(/* coalesce expression */)
```

## Benefits

1. **Null Safety**: Elegant handling of NULL values
2. **Data Normalization**: Convert sentinel values to NULL
3. **Display Logic**: Fallback chains for user-facing data
4. **Clean Queries**: Avoid complex CASE statements for simple null handling

## Next Steps

With COALESCE and NULLIF implemented, the next priorities could be:
1. **UPDATE...FROM** - Multi-table updates
2. **STRING_AGG** - String concatenation aggregate
3. **COUNT(DISTINCT)** - Unique value counting

These null-handling functions provide essential functionality for real-world applications while maintaining our strict security model.