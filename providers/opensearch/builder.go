package opensearch

import (
	"github.com/zoobzio/astql"
)

// Builder extends the base Builder with OpenSearch-specific methods.
type Builder struct {
	*astql.Builder
	osAst *AST
}

// Build creates the final AST.
func (b *Builder) Build() (*AST, error) {
	if b.GetError() != nil {
		return nil, b.GetError()
	}

	baseAST := b.GetAST()
	b.osAst.QueryAST = baseAST
	return b.osAst, nil
}

// WithHighlight adds fields to highlight in search results.
func (b *Builder) WithHighlight(fields ...astql.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.osAst.Highlight = append(b.osAst.Highlight, fields...)
	return b
}

// WithSourceIncludes specifies fields to include in response.
func (b *Builder) WithSourceIncludes(fields ...astql.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.osAst.SourceIncludes = append(b.osAst.SourceIncludes, fields...)
	return b
}

// WithSourceExcludes specifies fields to exclude from response.
func (b *Builder) WithSourceExcludes(fields ...astql.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.osAst.SourceExcludes = append(b.osAst.SourceExcludes, fields...)
	return b
}

// WithMinScore sets minimum relevance score for results.
func (b *Builder) WithMinScore(score float64) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.osAst.MinScore = &score
	return b
}

// WithAnalyzer sets the analyzer for text searches.
func (b *Builder) WithAnalyzer(analyzer AnalyzerType) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.osAst.Analyzer = &analyzer
	return b
}

// Select creates a SELECT query for searching documents.
func Select(table astql.Table) *Builder {
	builder := astql.Select(table)
	osAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		osAst:   osAst,
	}
}

// Insert creates an INSERT query for indexing documents.
func Insert(table astql.Table) *Builder {
	builder := astql.Insert(table)
	osAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		osAst:   osAst,
	}
}

// Update creates an UPDATE query for updating documents.
func Update(table astql.Table) *Builder {
	builder := astql.Update(table)
	osAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		osAst:   osAst,
	}
}

// Delete creates a DELETE query for removing documents.
func Delete(table astql.Table) *Builder {
	builder := astql.Delete(table)
	osAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		osAst:   osAst,
	}
}

// Count creates a COUNT query for counting documents.
func Count(table astql.Table) *Builder {
	builder := astql.Count(table)
	osAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		osAst:   osAst,
	}
}

// Search is an alias for Select that's more natural for OpenSearch.
func Search(table astql.Table) *Builder {
	return Select(table)
}

// Values overrides to return OpenSearch Builder.
func (b *Builder) Values(values map[astql.Field]astql.Param) *Builder {
	b.Builder.Values(values)
	return b
}

// Where overrides to return OpenSearch Builder.
func (b *Builder) Where(condition astql.ConditionItem) *Builder {
	b.Builder.Where(condition)
	return b
}

// Set overrides to return OpenSearch Builder.
func (b *Builder) Set(field astql.Field, value astql.Param) *Builder {
	b.Builder.Set(field, value)
	return b
}

// Fields overrides to return OpenSearch Builder.
func (b *Builder) Fields(fields ...astql.Field) *Builder {
	b.Builder.Fields(fields...)
	return b
}

// OrderBy overrides to return OpenSearch Builder.
func (b *Builder) OrderBy(field astql.Field, direction astql.Direction) *Builder {
	b.Builder.OrderBy(field, direction)
	return b
}

// Limit overrides to return OpenSearch Builder.
func (b *Builder) Limit(limit int) *Builder {
	b.Builder.Limit(limit)
	return b
}

// Offset overrides to return OpenSearch Builder.
func (b *Builder) Offset(offset int) *Builder {
	b.Builder.Offset(offset)
	return b
}
