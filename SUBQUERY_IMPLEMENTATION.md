# Subquery Implementation Summary

## Overview
We successfully implemented secure subquery support for the astql AST query builder while maintaining the security-first approach.

## Key Features Implemented

### 1. Type System Extensions
- Added `SubqueryCondition` type to represent conditions with subqueries
- Added `Subquery` type that wraps either `*QueryAST` or `*PostgresAST`
- Added `EXISTS` and `NOT EXISTS` operators

### 2. Builder API
- `Sub(builder)` - Creates subquery from regular AST builder
- `SubPG(builder)` - Creates subquery from PostgreSQL builder  
- `CSub(field, operator, subquery)` - For IN/NOT IN conditions
- `CSubExists(operator, subquery)` - For EXISTS/NOT EXISTS conditions

### 3. Security Features
- **Depth Limiting**: Maximum subquery depth of 3 levels to prevent DoS
- **Parameter Namespacing**: Subquery parameters prefixed with `sq1_`, `sq2_`, etc.
- **Validation**: All subqueries inherit parent query validation
- **Type Safety**: Compile-time validation of operator/field combinations

### 4. Declarative Schema Support
```yaml
where:
  field: customer_id
  operator: IN
  subquery:
    operation: SELECT
    table: customers
    fields: [id]
    where:
      field: country
      operator: "="
      param: country
```

### 5. Supported Query Patterns

#### IN Subquery
```go
// WHERE customer_id IN (SELECT id FROM customers WHERE country = :country)
subquery := Sub(
    Select(T("customers")).
    Fields(F("id")).
    Where(C(F("country"), EQ, P("country")))
)
ast := PostgresSelect(T("orders")).
    Where(CSub(F("customer_id"), IN, subquery))
```

#### EXISTS Subquery
```go
// WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
subquery := SubPG(
    PostgresSelect(T("orders")).
    Where(CF(F("customer_id"), EQ, F("id").WithTable("customers")))
)
ast := PostgresSelect(T("customers")).
    Where(CSubExists(EXISTS, subquery))
```

#### Nested Subqueries
```go
// Supports up to 3 levels deep with automatic parameter namespacing
```

## Implementation Details

### renderContext
- Tracks subquery depth
- Handles parameter namespacing
- Prevents excessive nesting

### PostgresProvider Updates
- `renderSelectWithContext` - Context-aware rendering
- `renderConditionWithContext` - Handles all condition types
- `renderSubqueryCondition` - Subquery-specific rendering
- `renderSubquery` - Recursive subquery rendering

### Validation
- `CSub` validates IN/NOT IN require a field
- `CSubExists` validates EXISTS/NOT EXISTS don't take a field
- Invalid operators panic immediately (fail-fast)

## Testing
- Comprehensive unit tests for all subquery types
- Schema-based subquery tests
- Security validation tests
- Example tests for documentation

## Security Considerations
1. **No New Attack Surface**: Reuses existing validated components
2. **Whitelist Only**: Subqueries use same field/table validation
3. **Parameter Safety**: All values remain parameterized
4. **Resource Protection**: Depth limits prevent nested query bombs

## Future Considerations
- Subqueries in SELECT clause (scalar subqueries)
- Subqueries in FROM clause (derived tables)
- Lateral subqueries
- CTE support (which builds on this foundation)