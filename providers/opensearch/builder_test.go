package opensearch_test

import (
	"fmt"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/opensearch"
)

func TestOpenSearchBuilder(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Build with error returns error", func(t *testing.T) {
		// Create a builder and set an error
		builder := opensearch.Select(astql.T("test_articles"))
		builder.SetError(fmt.Errorf("test error"))

		_, err := builder.Build()
		if err == nil {
			t.Error("Expected Build to return error")
		}
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got: %v", err)
		}
	})

	t.Run("Search is alias for Select", func(t *testing.T) {
		// Both should create equivalent builders
		selectBuilder := opensearch.Select(astql.T("test_articles"))
		searchBuilder := opensearch.Search(astql.T("test_articles"))

		selectAST, _ := selectBuilder.Build()
		searchAST, _ := searchBuilder.Build()

		if selectAST.Operation != searchAST.Operation {
			t.Error("Search and Select should have same operation")
		}
		if selectAST.Target.Name != searchAST.Target.Name {
			t.Error("Search and Select should have same target")
		}
	})

	t.Run("Builder method chaining maintains type", func(t *testing.T) {
		// All methods should return *opensearch.Builder, not *astql.Builder
		builder := opensearch.Select(astql.T("test_articles"))

		// These should all return the OpenSearch builder
		b1 := builder.Where(astql.C(astql.F("title"), astql.LIKE, astql.P("search")))
		b2 := b1.Fields(astql.F("title"), astql.F("author"))
		b3 := b2.OrderBy(astql.F("published_at"), astql.DESC)
		b4 := b3.Limit(20)
		b5 := b4.Offset(40)
		b6 := b5.Set(astql.F("view_count"), astql.P("count")) // For UPDATE
		b7 := b6.Values(map[astql.Field]astql.Param{
			astql.F("title"): astql.P("title"),
		})

		// All should be the same instance
		if b1 != builder || b2 != builder || b3 != builder ||
			b4 != builder || b5 != builder || b6 != builder || b7 != builder {
			t.Error("All methods should return the same builder instance")
		}
	})

	t.Run("Complex query with all features", func(t *testing.T) {
		builder := opensearch.Search(astql.T("test_articles")).
			Where(astql.And(
				astql.C(astql.F("author"), astql.EQ, astql.P("author_name")),
				astql.Or(
					astql.C(astql.F("title"), astql.LIKE, astql.P("search")),
					astql.C(astql.F("content"), astql.LIKE, astql.P("search")),
				),
			)).
			WithHighlight(astql.F("title"), astql.F("content")).
			WithSourceIncludes(astql.F("title"), astql.F("author"), astql.F("published_at")).
			WithSourceExcludes(astql.F("content")).
			WithMinScore(0.5).
			WithAnalyzer(opensearch.AnalyzerEnglish).
			OrderBy(astql.F("published_at"), astql.DESC).
			Limit(20).
			Offset(0)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build complex query: %v", err)
		}

		// Verify all features are set
		if len(ast.Highlight) != 2 {
			t.Errorf("Expected 2 highlight fields, got %d", len(ast.Highlight))
		}
		if len(ast.SourceIncludes) != 3 {
			t.Errorf("Expected 3 source includes, got %d", len(ast.SourceIncludes))
		}
		if len(ast.SourceExcludes) != 1 {
			t.Errorf("Expected 1 source exclude, got %d", len(ast.SourceExcludes))
		}
		if ast.MinScore == nil || *ast.MinScore != 0.5 {
			t.Error("Expected min score to be 0.5")
		}
		if ast.Analyzer == nil || *ast.Analyzer != opensearch.AnalyzerEnglish {
			t.Error("Expected English analyzer")
		}
	})

	t.Run("Error propagation through method chain", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles"))

		// Set an error early
		builder.SetError(fmt.Errorf("early error"))

		// Continue chaining methods
		builder.
			WithHighlight(astql.F("title")).
			WithMinScore(1.0).
			Limit(10)

		// Error should persist
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected error to persist")
		}
		if err.Error() != "early error" {
			t.Errorf("Expected 'early error', got: %v", err)
		}
	})
}
