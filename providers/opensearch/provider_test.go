package opensearch_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/opensearch"
)

func TestOpenSearchProvider(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	// Create and configure provider
	provider := opensearch.NewProvider()

	// Register table configurations
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField:         "id",
		DefaultAnalyzer: opensearch.AnalyzerStandard,
		RefreshPolicy:   opensearch.RefreshFalse,
	})

	provider.RegisterTable("test_search_products", opensearch.TableConfig{
		IDField:         "sku",
		DefaultAnalyzer: opensearch.AnalyzerKeyword,
		RefreshPolicy:   opensearch.RefreshWaitFor,
	})

	t.Run("SELECT with match all", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_search test_articles") {
			t.Errorf("Expected _search operation, got: %s", result.SQL)
		}
		if !contains(result.SQL, "match_all") {
			t.Errorf("Expected match_all query, got: %s", result.SQL)
		}
	})

	t.Run("SELECT with term query", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Where(astql.C(astql.F("author"), astql.EQ, astql.P("author_name")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "term") {
			t.Errorf("Expected term query, got: %s", result.SQL)
		}
		if !contains(result.SQL, "author") {
			t.Errorf("Expected author field, got: %s", result.SQL)
		}
	})

	t.Run("SELECT with range query", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_search_products")).
			Where(astql.C(astql.F("price"), astql.LE, astql.P("max_price"))).
			OrderBy(astql.F("price"), astql.ASC)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "range") {
			t.Errorf("Expected range query, got: %s", result.SQL)
		}
		if !contains(result.SQL, "lte") {
			t.Errorf("Expected lte operator, got: %s", result.SQL)
		}
		if !contains(result.SQL, "sort=") {
			t.Errorf("Expected sort clause, got: %s", result.SQL)
		}
	})

	t.Run("SELECT with complex bool query", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_search_products")).
			Where(astql.And(
				astql.C(astql.F("category"), astql.EQ, astql.P("cat")),
				astql.C(astql.F("in_stock"), astql.EQ, astql.P("stock")),
				astql.Or(
					astql.C(astql.F("price"), astql.LT, astql.P("max_price")),
					astql.C(astql.F("name"), astql.LIKE, astql.P("search_term")),
				),
			))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "bool") {
			t.Errorf("Expected bool query, got: %s", result.SQL)
		}
		if !contains(result.SQL, "must") {
			t.Errorf("Expected must clause, got: %s", result.SQL)
		}
		if !contains(result.SQL, "should") {
			t.Errorf("Expected should clause for OR, got: %s", result.SQL)
		}
	})

	t.Run("SELECT with highlighting", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Where(astql.C(astql.F("content"), astql.LIKE, astql.P("search_term"))).
			WithHighlight(astql.F("title"), astql.F("content"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Result should contain highlight configuration
		if !contains(result.SQL, "wildcard") {
			t.Errorf("Expected wildcard query for LIKE, got: %s", result.SQL)
		}
	})

	t.Run("SELECT with field selection", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Fields(astql.F("title"), astql.F("author")).
			Limit(10).
			Offset(20)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "size=10") {
			t.Errorf("Expected size=10, got: %s", result.SQL)
		}
		if !contains(result.SQL, "from=20") {
			t.Errorf("Expected from=20, got: %s", result.SQL)
		}
	})

	t.Run("INSERT document", func(t *testing.T) {
		builder := opensearch.Insert(astql.T("test_articles")).
			Values(map[astql.Field]astql.Param{
				astql.F("id"):      astql.P("doc_id"),
				astql.F("title"):   astql.P("doc_title"),
				astql.F("content"): astql.P("doc_content"),
				astql.F("author"):  astql.P("doc_author"),
			})

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_doc/:doc_id test_articles") {
			t.Errorf("Expected document index with ID, got: %s", result.SQL)
		}
		if !contains(result.SQL, "doc=") {
			t.Errorf("Expected document data, got: %s", result.SQL)
		}

		// Check all params are required
		if len(result.RequiredParams) != 4 {
			t.Errorf("Expected 4 required params, got: %d", len(result.RequiredParams))
		}
	})

	t.Run("UPDATE by query", func(t *testing.T) {
		builder := opensearch.Update(astql.T("test_articles")).
			Set(astql.F("view_count"), astql.P("new_count")).
			Where(astql.C(astql.F("author"), astql.EQ, astql.P("author_name")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_update_by_query") {
			t.Errorf("Expected _update_by_query, got: %s", result.SQL)
		}
		if !contains(result.SQL, "script") {
			t.Errorf("Expected script for update, got: %s", result.SQL)
		}
	})

	t.Run("DELETE by ID", func(t *testing.T) {
		builder := opensearch.Delete(astql.T("test_articles")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("doc_id")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_doc/:doc_id") {
			t.Errorf("Expected delete by ID, got: %s", result.SQL)
		}
	})

	t.Run("DELETE by query", func(t *testing.T) {
		builder := opensearch.Delete(astql.T("test_articles")).
			Where(astql.C(astql.F("published_at"), astql.LT, astql.P("cutoff_date")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_delete_by_query") {
			t.Errorf("Expected _delete_by_query, got: %s", result.SQL)
		}
		if !contains(result.SQL, "range") {
			t.Errorf("Expected range query, got: %s", result.SQL)
		}
	})

	t.Run("COUNT documents", func(t *testing.T) {
		builder := opensearch.Count(astql.T("test_articles")).
			Where(astql.C(astql.F("author"), astql.EQ, astql.P("author_name")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_count") {
			t.Errorf("Expected _count operation, got: %s", result.SQL)
		}
		if !contains(result.SQL, "term") {
			t.Errorf("Expected term query, got: %s", result.SQL)
		}
	})

	t.Run("IN operator for terms query", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Where(astql.C(astql.F("tags"), astql.IN, astql.P("tag_list")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "terms") {
			t.Errorf("Expected terms query (plural) for IN, got: %s", result.SQL)
		}
	})

	t.Run("IS NULL check", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Where(astql.C(astql.F("published_at"), astql.IsNull, astql.P("null_value")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "exists") {
			t.Errorf("Expected exists query for NULL check, got: %s", result.SQL)
		}
		if !contains(result.SQL, "must_not") {
			t.Errorf("Expected must_not for IsNull, got: %s", result.SQL)
		}
	})

	t.Run("Source filtering", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			WithSourceIncludes(astql.F("title"), astql.F("author")).
			WithSourceExcludes(astql.F("content"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Should have source filtering in the query
		if !contains(result.SQL, "_search") {
			t.Errorf("Expected search operation, got: %s", result.SQL)
		}
	})

	t.Run("Minimum score filtering", func(t *testing.T) {
		builder := opensearch.Select(astql.T("test_articles")).
			Where(astql.C(astql.F("content"), astql.LIKE, astql.P("search_term"))).
			WithMinScore(0.5)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "_search") {
			t.Errorf("Expected search operation, got: %s", result.SQL)
		}
	})

	t.Run("Unsupported operations", func(t *testing.T) {
		// OpenSearch doesn't support JOIN - test with invalid operation
		ast := &opensearch.AST{
			QueryAST: &astql.QueryAST{
				Operation: astql.Operation("JOIN"), // Not a real operation
				Target:    astql.T("test_articles"),
			},
		}

		_, err := provider.Render(ast)
		if err == nil {
			t.Error("Expected error for unsupported operation")
		}
		if !contains(err.Error(), "unsupported operation") {
			t.Errorf("Wrong error message: %v", err)
		}
	})
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
