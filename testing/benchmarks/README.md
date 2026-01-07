# Benchmarks

Performance benchmarks for astql query rendering.

## Running Benchmarks

```bash
# Run all benchmarks
make bench

# Run with memory allocation stats
go test -bench=. -benchmem ./testing/benchmarks/...

# Run specific benchmark
go test -bench=BenchmarkComplexQuery -benchmem ./testing/benchmarks/...

# Run with longer duration for stability
go test -bench=. -benchmem -benchtime=5s ./testing/benchmarks/...

# Compare before/after changes
go test -bench=. -benchmem -count=10 ./testing/benchmarks/... > before.txt
# ... make changes ...
go test -bench=. -benchmem -count=10 ./testing/benchmarks/... > after.txt
benchstat before.txt after.txt
```

## Benchmark Categories

### Query Rendering

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkSimpleSelect` | Basic `SELECT *` |
| `BenchmarkSelectWithFields` | SELECT with explicit columns |
| `BenchmarkSelectWithWhere` | SELECT with WHERE clause |
| `BenchmarkSelectWithMultipleConditions` | Complex AND/OR conditions |
| `BenchmarkSelectWithJoin` | INNER JOIN rendering |
| `BenchmarkSelectWithOrderByLimit` | ORDER BY + LIMIT + OFFSET |
| `BenchmarkSelectWithAggregates` | SUM, COUNT with GROUP BY |
| `BenchmarkInsert` | Basic INSERT |
| `BenchmarkInsertWithReturning` | INSERT with RETURNING clause |
| `BenchmarkUpdate` | UPDATE with WHERE |
| `BenchmarkDelete` | DELETE with WHERE |
| `BenchmarkCount` | COUNT query |

### Advanced Features

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkCaseExpression` | CASE WHEN rendering |
| `BenchmarkWindowFunction` | ROW_NUMBER() OVER |
| `BenchmarkBetween` | BETWEEN conditions |
| `BenchmarkSubquery` | Subquery in WHERE |
| `BenchmarkDistinctOn` | DISTINCT ON (PostgreSQL) |
| `BenchmarkForUpdate` | Row locking |
| `BenchmarkNullsOrdering` | NULLS FIRST/LAST |
| `BenchmarkTypeCast` | Type casting |
| `BenchmarkCoalesce` | COALESCE function |
| `BenchmarkComplexQuery` | Real-world complex query |

### Component Creation

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkFieldCreation` | Field reference creation overhead |
| `BenchmarkTableCreation` | Table reference creation overhead |
| `BenchmarkParamCreation` | Parameter creation overhead |
| `BenchmarkConditionCreation` | Condition creation overhead |

## Writing Benchmarks

### Structure

```go
func BenchmarkFeatureName(b *testing.B) {
    instance := createBenchmarkInstance(b)

    // Setup outside the loop
    table := instance.T("users")

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        _, err := astql.Select(table).Render(postgres.New())
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### Guidelines

1. **Use `b.ResetTimer()`** after setup to exclude initialisation from measurements
2. **Use `b.ReportAllocs()`** to track memory allocations
3. **Check errors** to ensure the benchmark measures successful operations
4. **Reuse setup** where possible to isolate the operation being measured
5. **Name descriptively** so benchmark output is self-documenting

## CI Integration

Benchmarks run in CI to track performance over time. Results are not used as pass/fail criteria but provide visibility into performance changes.
