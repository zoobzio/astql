# OpenSearch Provider for ASTQL

The OpenSearch provider enables ASTQL to work with OpenSearch/Elasticsearch clusters, providing type-safe query building for document search and management.

## Features

- **Full-text search** with highlighting and relevance scoring
- **Complex boolean queries** with AND/OR logic
- **Range queries** for numeric and date fields
- **Document CRUD operations** (insert, update, delete)
- **Aggregations** support (count)
- **Field selection** and source filtering
- **Multiple analyzers** (standard, simple, keyword, etc.)
- **Refresh policies** for write operations
- **Schema-driven queries** via YAML/JSON

## Usage

```go
import (
    "github.com/zoobzio/astql"
    "github.com/zoobzio/astql/providers/opensearch"
)

// Create provider and configure table
provider := opensearch.NewProvider()
provider.RegisterTable("articles", opensearch.TableConfig{
    IDField:         "id",
    DefaultAnalyzer: opensearch.AnalyzerStandard,
    RefreshPolicy:   opensearch.RefreshFalse,
})

// Search documents
query := opensearch.Search(astql.T("articles")).
    Where(astql.C(astql.F("content"), astql.LIKE, astql.P("search_term"))).
    WithHighlight(astql.F("title"), astql.F("content")).
    OrderBy(astql.F("published_at"), astql.DESC).
    Limit(10)

ast, _ := query.Build()
result, _ := provider.Render(ast)
```

## Query Operations

### Search (SELECT)
- Match all documents
- Term queries for exact matches
- Wildcard queries for pattern matching
- Range queries for numeric/date fields
- Boolean combinations (must/should/must_not)
- Highlighting support
- Relevance scoring with min_score
- Sorting and pagination

### Insert
- Single document indexing
- Custom document IDs
- Refresh policy control

### Update
- Update by query with script execution
- Partial document updates

### Delete
- Delete by ID
- Delete by query

### Count
- Count all documents
- Count with filters

## OpenSearch-Specific Features

1. **Text Analysis**
   - Multiple analyzer types
   - Field-specific analyzers
   - Custom analyzer configuration

2. **Source Filtering**
   - Include specific fields
   - Exclude large fields
   - Optimize response size

3. **Relevance Control**
   - Minimum score filtering
   - Field boosting (future)
   - Custom scoring (future)

## Schema Support

Define queries in YAML/JSON:

```yaml
operation: search
table: articles
where:
  and:
    - field: status
      operator: "="
      value: published
    - field: content
      operator: like
      value: search_term
highlight: [title, content]
order_by:
  published_at: desc
limit: 20
```

## Security

All queries follow ASTQL's security model:
- No arbitrary strings in queries
- All identifiers validated through Sentinel
- Parameters properly escaped
- Query injection prevention at compile time