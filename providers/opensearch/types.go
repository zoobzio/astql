package opensearch

import (
	"github.com/zoobzio/astql"
)

// QueryType represents OpenSearch query types.
type QueryType string

// OpenSearch query types - fully enumerated.
const (
	QueryTypeTerm        QueryType = "term"         // Exact match
	QueryTypeMatch       QueryType = "match"        // Full-text search
	QueryTypeMatchPhrase QueryType = "match_phrase" // Phrase search
	QueryTypeRange       QueryType = "range"        // Range queries
	QueryTypeWildcard    QueryType = "wildcard"     // Wildcard search
	QueryTypePrefix      QueryType = "prefix"       // Prefix search
	QueryTypeExists      QueryType = "exists"       // Field exists
	QueryTypeBool        QueryType = "bool"         // Boolean combination
)

// RefreshPolicy controls when changes are visible.
type RefreshPolicy string

// OpenSearch refresh policies.
const (
	RefreshTrue    RefreshPolicy = "true"     // Refresh immediately
	RefreshWaitFor RefreshPolicy = "wait_for" // Wait for refresh
	RefreshFalse   RefreshPolicy = "false"    // Don't refresh (default)
)

// SortOrder represents sort direction.
type SortOrder string

// Sort orders.
const (
	SortAsc  SortOrder = "asc"
	SortDesc SortOrder = "desc"
)

// AnalyzerType represents text analyzers.
type AnalyzerType string

// Common analyzers - enumerated, not arbitrary.
const (
	AnalyzerStandard   AnalyzerType = "standard"
	AnalyzerSimple     AnalyzerType = "simple"
	AnalyzerWhitespace AnalyzerType = "whitespace"
	AnalyzerKeyword    AnalyzerType = "keyword"
	AnalyzerEnglish    AnalyzerType = "english"
)

// TableConfig defines how an ASTQL table maps to OpenSearch.
type TableConfig struct {
	// Document ID field
	IDField string

	// Default analyzer for text fields
	DefaultAnalyzer AnalyzerType

	// Refresh policy for write operations
	RefreshPolicy RefreshPolicy
}

// AST extends the base QueryAST with OpenSearch-specific features.
//
//nolint:govet // field alignment not critical for AST types
type AST struct {
	*astql.QueryAST

	// OpenSearch-specific fields
	TableConfig    *TableConfig
	Analyzer       *AnalyzerType // Override analyzer for searches
	Highlight      []astql.Field // Fields to highlight in results
	SourceIncludes []astql.Field // Fields to include in response
	SourceExcludes []astql.Field // Fields to exclude from response
	MinScore       *float64      // Minimum relevance score
}

// SearchField represents a field with search-specific options.
//
//nolint:govet // field alignment not critical for search fields
type SearchField struct {
	Field    astql.Field
	Boost    *float64      // Boost factor for relevance
	Analyzer *AnalyzerType // Field-specific analyzer
}

// NewAST creates an OpenSearch AST from a base QueryAST.
func NewAST(baseAST *astql.QueryAST) *AST {
	return &AST{
		QueryAST: baseAST,
	}
}

// Validate checks if the AST is valid for OpenSearch operations.
func (ast *AST) Validate() error {
	// First validate the base AST
	if err := ast.QueryAST.Validate(); err != nil {
		return err
	}

	// Add OpenSearch-specific validation here
	// For example, certain operations might require specific fields

	return nil
}
