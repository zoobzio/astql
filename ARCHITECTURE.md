# ASTQL Architecture: Progressive Complexity

## Overview

ASTQL has been redesigned from a superset approach to a progressive complexity architecture. This document explains the design decisions and benefits.

## Previous Architecture (Superset)

The old architecture had:
- Single `QueryAST` containing ALL possible features from ALL databases
- Runtime validation when providers rejected unsupported features
- "Debug hell" - errors only discovered at render time

```go
// Old approach - everything in one AST
type QueryAST struct {
    // Universal features
    Target string
    Fields []Field
    
    // SQL features
    Distinct bool
    Joins    []Join
    
    // MongoDB features  
    Hint map[string]int
    
    // DynamoDB features
    ConsistentRead bool
    // ... etc
}
```

## New Architecture (Progressive Complexity)

### Core Principles

1. **Base functionality in core** - Only truly universal features
2. **Provider-specific extensions** - Each provider extends the core
3. **Type-safe at compile time** - Can't use wrong features
4. **Schema-driven safety** - AI generates schemas, not raw queries

### Structure

```
astql/
├── operator.go          # Shared types (Operator, Direction, etc)
├── ast/
│   └── core.go         # Core QueryAST with universal features
├── builder/
│   └── core.go         # Core Builder with basic methods
├── schema/
│   └── core.go         # Core Schema for serialization
└── providers/
    ├── postgres/
    │   ├── ast.go      # PostgresAST extends QueryAST
    │   ├── builder.go  # PostgresBuilder extends Builder
    │   ├── schema.go   # PostgresSchema extends Schema
    │   └── provider.go # Render PostgresAST → SQL
    └── mongodb/
        ├── ast.go      # MongoDBAST extends QueryAST
        ├── builder.go  # MongoDBBuilder extends Builder
        ├── schema.go   # MongoDBSchema extends Schema
        └── provider.go # Render MongoDBAST → MongoDB query
```

### Type Safety Example

```go
// ✅ This compiles - PostgreSQL has Distinct()
postgres.Select("users").
    Distinct().
    OnConflictDoNothing("email")

// ❌ This does NOT compile - MongoDB doesn't have Distinct()
mongodb.Select("users").
    Distinct() // Compile error: method not found

// ✅ This compiles - MongoDB has WithHint()
mongodb.Select("users").
    WithHint("index_1", 1)

// ❌ This does NOT compile - PostgreSQL doesn't have WithHint()  
postgres.Select("users").
    WithHint("index", 1) // Compile error: method not found
```

## Benefits

### 1. Compile-Time Safety
- Impossible to use unsupported features
- IDE autocomplete shows only available methods
- No runtime surprises

### 2. Clear Provider Boundaries
- Each provider owns its complete stack
- No confusion about what works where
- Provider-specific optimizations possible

### 3. Progressive Learning
- Start with core features
- Add provider-specific features as needed
- Documentation can be provider-specific

### 4. AI Safety
- AI generates structured schemas
- Each provider validates its own schema format
- No raw SQL/NoSQL query generation
- Injection attacks prevented by design

## Schema-Driven Approach

Each provider defines its own schema format:

```yaml
# PostgreSQL schema
select: users
distinct: true
on_conflict:
  columns: [email]
  action: update
returning: [id]

# MongoDB schema  
select: users
hint:
  email_1: 1
pipeline:
  - type: $group
    value:
      _id: "$status"
```

## Migration Guide

From old superset:
```go
// Old
query := astql.Select("users").
    Distinct(). // Runtime error on MongoDB
    Build()
```

To new progressive:
```go
// New - PostgreSQL
query := postgres.Select("users").
    Distinct(). // Compile-time safe
    MustBuild()

// New - MongoDB  
query := mongodb.Select("users").
    WithHint("index", 1). // Provider-specific
    MustBuild()
```

## Design Decisions

1. **No `interface{}` or `any` types** - Everything strongly typed
2. **Embedding over interfaces** - Providers embed and extend core types
3. **Schema per provider** - Each provider handles its own serialization
4. **Explicit over implicit** - Clear which provider you're using

## Trade-offs

### Pros
- Type safety at compile time
- Clear feature boundaries
- Better IDE support
- Easier to understand and maintain

### Cons
- More code (each provider has full stack)
- Can't share provider instances
- Must know provider at compile time

## Future Considerations

1. **Provider detection** - Could add runtime provider detection for schemas
2. **Feature discovery** - Could add capability querying
3. **Cross-provider queries** - Could add query translation between providers
4. **Provider plugins** - Could support dynamic provider loading

## Conclusion

The progressive complexity architecture solves the fundamental tension between:
- Wanting type-safe, provider-specific features
- Needing a unified schema format for AI safety
- Avoiding `any` types throughout the system

By having each provider own its complete stack (AST, Builder, Schema, Renderer), we achieve compile-time safety while maintaining the flexibility needed for real-world applications.