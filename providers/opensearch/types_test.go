package opensearch_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/opensearch"
)

func TestOpenSearchTypes(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Query type constants", func(t *testing.T) {
		queryTypes := map[opensearch.QueryType]string{
			opensearch.QueryTypeTerm:        "term",
			opensearch.QueryTypeMatch:       "match",
			opensearch.QueryTypeMatchPhrase: "match_phrase",
			opensearch.QueryTypeRange:       "range",
			opensearch.QueryTypeWildcard:    "wildcard",
			opensearch.QueryTypePrefix:      "prefix",
			opensearch.QueryTypeExists:      "exists",
			opensearch.QueryTypeBool:        "bool",
		}

		for qt, expected := range queryTypes {
			if string(qt) != expected {
				t.Errorf("QueryType %v has value %q, expected %q", qt, string(qt), expected)
			}
		}
	})

	t.Run("Refresh policy constants", func(t *testing.T) {
		policies := map[opensearch.RefreshPolicy]string{
			opensearch.RefreshTrue:    "true",
			opensearch.RefreshWaitFor: "wait_for",
			opensearch.RefreshFalse:   "false",
		}

		for rp, expected := range policies {
			if string(rp) != expected {
				t.Errorf("RefreshPolicy %v has value %q, expected %q", rp, string(rp), expected)
			}
		}
	})

	t.Run("Analyzer type constants", func(t *testing.T) {
		analyzers := map[opensearch.AnalyzerType]string{
			opensearch.AnalyzerStandard:   "standard",
			opensearch.AnalyzerSimple:     "simple",
			opensearch.AnalyzerWhitespace: "whitespace",
			opensearch.AnalyzerKeyword:    "keyword",
			opensearch.AnalyzerEnglish:    "english",
		}

		for at, expected := range analyzers {
			if string(at) != expected {
				t.Errorf("AnalyzerType %v has value %q, expected %q", at, string(at), expected)
			}
		}
	})

	t.Run("AST Validate with valid base AST", func(t *testing.T) {
		baseAST := &astql.QueryAST{
			Operation: astql.OpSelect,
			Target:    astql.T("test_articles"),
		}

		osAST := opensearch.NewAST(baseAST)

		err := osAST.Validate()
		if err != nil {
			t.Errorf("Expected no error for valid AST, got: %v", err)
		}
	})

	t.Run("AST Validate with invalid base AST", func(t *testing.T) {
		// Create AST with missing target
		baseAST := &astql.QueryAST{
			Operation: astql.OpSelect,
			// Target is missing
		}

		osAST := opensearch.NewAST(baseAST)

		err := osAST.Validate()
		if err == nil {
			t.Error("Expected error for invalid AST")
		}
		if err.Error() != "target table is required" {
			t.Errorf("Expected 'target table is required' error, got: %v", err)
		}
	})

	t.Run("AST with all OpenSearch features", func(t *testing.T) {
		baseAST := &astql.QueryAST{
			Operation: astql.OpSelect,
			Target:    astql.T("test_articles"),
		}

		osAST := opensearch.NewAST(baseAST)

		// Set all OpenSearch-specific features
		analyzer := opensearch.AnalyzerEnglish
		osAST.Analyzer = &analyzer

		osAST.Highlight = []astql.Field{
			astql.F("title"),
			astql.F("content"),
		}

		osAST.SourceIncludes = []astql.Field{
			astql.F("title"),
			astql.F("author"),
		}

		osAST.SourceExcludes = []astql.Field{
			astql.F("content"),
		}

		minScore := 0.5
		osAST.MinScore = &minScore

		// Validate should still pass
		err := osAST.Validate()
		if err != nil {
			t.Errorf("Expected no error with all features set, got: %v", err)
		}
	})

	t.Run("TableConfig structure", func(t *testing.T) {
		config := opensearch.TableConfig{
			IDField:         "doc_id",
			DefaultAnalyzer: opensearch.AnalyzerStandard,
			RefreshPolicy:   opensearch.RefreshWaitFor,
		}

		if config.IDField != "doc_id" {
			t.Errorf("Expected ID field 'doc_id', got %q", config.IDField)
		}
		if config.DefaultAnalyzer != opensearch.AnalyzerStandard {
			t.Errorf("Expected standard analyzer, got %v", config.DefaultAnalyzer)
		}
		if config.RefreshPolicy != opensearch.RefreshWaitFor {
			t.Errorf("Expected wait_for refresh policy, got %v", config.RefreshPolicy)
		}
	})

	t.Run("SearchField structure", func(t *testing.T) {
		boost := 2.0
		analyzer := opensearch.AnalyzerEnglish

		searchField := opensearch.SearchField{
			Field:    astql.F("title"),
			Boost:    &boost,
			Analyzer: &analyzer,
		}

		if searchField.Field.Name != "title" {
			t.Errorf("Expected field name 'title', got %q", searchField.Field.Name)
		}
		if searchField.Boost == nil || *searchField.Boost != 2.0 {
			t.Error("Expected boost to be 2.0")
		}
		if searchField.Analyzer == nil || *searchField.Analyzer != opensearch.AnalyzerEnglish {
			t.Error("Expected English analyzer")
		}
	})
}
