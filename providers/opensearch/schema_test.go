package opensearch_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/opensearch"
)

func TestSchemaLoader(t *testing.T) {
	// Setup
	astql.SetupTestModels()
	provider := opensearch.NewProvider()

	// Register tables
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

	loader := opensearch.NewSchemaLoader(provider)

	t.Run("Simple search query", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_articles",
			"where": map[string]interface{}{
				"field":    "author",
				"operator": "=",
				"value":    "author_name",
			},
			"limit": 10,
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "_search") {
			t.Errorf("Expected search operation, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "term") {
			t.Errorf("Expected term query, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "size=10") {
			t.Errorf("Expected size=10, got: %s", result.SQL)
		}
	})

	t.Run("Complex boolean query", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "select",
			"table":     "test_search_products",
			"where": map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{
						"field":    "in_stock",
						"operator": "=",
						"value":    "stock_status",
					},
					map[string]interface{}{
						"or": []interface{}{
							map[string]interface{}{
								"field":    "price",
								"operator": "<",
								"value":    "max_price",
							},
							map[string]interface{}{
								"field":    "name",
								"operator": "like",
								"value":    "search_term",
							},
						},
					},
				},
			},
			"order_by": map[string]interface{}{
				"price": "asc",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "bool") {
			t.Errorf("Expected bool query, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "must") {
			t.Errorf("Expected must clause, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "should") {
			t.Errorf("Expected should clause, got: %s", result.SQL)
		}
	})

	t.Run("Search with highlighting", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_articles",
			"where": map[string]interface{}{
				"field":    "content",
				"operator": "like",
				"value":    "search_term",
			},
			"highlight": []interface{}{"title", "content"},
			"min_score": 0.5,
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "wildcard") {
			t.Errorf("Expected wildcard query, got: %s", result.SQL)
		}
	})

	t.Run("Insert document", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "insert",
			"table":     "test_articles",
			"values": map[string]interface{}{
				"id":           "doc_id",
				"title":        "doc_title",
				"content":      "doc_content",
				"author":       "doc_author",
				"published_at": "publish_date",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "_doc/:doc_id") {
			t.Errorf("Expected document insert with ID, got: %s", result.SQL)
		}
		if len(result.RequiredParams) != 5 {
			t.Errorf("Expected 5 required params, got: %d", len(result.RequiredParams))
		}
	})

	t.Run("Update by query", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "update",
			"table":     "test_articles",
			"set": map[string]interface{}{
				"view_count": "new_count",
			},
			"where": map[string]interface{}{
				"field":    "author",
				"operator": "=",
				"value":    "author_name",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "_update_by_query") {
			t.Errorf("Expected update by query, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "script") {
			t.Errorf("Expected script for update, got: %s", result.SQL)
		}
	})

	t.Run("Delete by ID", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "delete",
			"table":     "test_articles",
			"where": map[string]interface{}{
				"field":    "id",
				"operator": "=",
				"value":    "doc_id",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "_doc/:doc_id") {
			t.Errorf("Expected delete by ID, got: %s", result.SQL)
		}
	})

	t.Run("Count with filter", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "count",
			"table":     "test_search_products",
			"where": map[string]interface{}{
				"field":    "category",
				"operator": "=",
				"value":    "category_name",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "_count") {
			t.Errorf("Expected count operation, got: %s", result.SQL)
		}
	})

	t.Run("Range query", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_search_products",
			"where": map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{
						"field":    "price",
						"operator": ">=",
						"value":    "min_price",
					},
					map[string]interface{}{
						"field":    "price",
						"operator": "<=",
						"value":    "max_price",
					},
				},
			},
			"order_by": []interface{}{
				map[string]interface{}{
					"price": "asc",
				},
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "range") {
			t.Errorf("Expected range query, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "gte") {
			t.Errorf("Expected gte operator, got: %s", result.SQL)
		}
		if !hasSubstring(result.SQL, "lte") {
			t.Errorf("Expected lte operator, got: %s", result.SQL)
		}
	})

	t.Run("Field selection with source filtering", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation":       "search",
			"table":           "test_articles",
			"fields":          []interface{}{"title", "author"},
			"source_excludes": []interface{}{"content"},
			"limit":           5,
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "size=5") {
			t.Errorf("Expected size=5, got: %s", result.SQL)
		}
	})

	t.Run("WITH analyzer override", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_articles",
			"where": map[string]interface{}{
				"field":    "content",
				"operator": "like",
				"value":    "search_term",
			},
			"analyzer": "english",
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		if ast.Analyzer == nil || *ast.Analyzer != opensearch.AnalyzerEnglish {
			t.Errorf("Expected English analyzer")
		}
	})

	t.Run("IN operator", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_articles",
			"where": map[string]interface{}{
				"field":    "tags",
				"operator": "in",
				"value":    "tag_list",
			},
		}

		ast, err := loader.LoadQuery(schema)
		if err != nil {
			t.Fatalf("Failed to load query: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !hasSubstring(result.SQL, "terms") {
			t.Errorf("Expected terms query for IN, got: %s", result.SQL)
		}
	})

	t.Run("Invalid schema - missing operation", func(t *testing.T) {
		schema := map[string]interface{}{
			"table": "test_articles",
		}

		_, err := loader.LoadQuery(schema)
		if err == nil {
			t.Error("Expected error for missing operation")
		}
		if !hasSubstring(err.Error(), "missing operation") {
			t.Errorf("Wrong error message: %v", err)
		}
	})

	t.Run("Invalid schema - bad where clause", func(t *testing.T) {
		schema := map[string]interface{}{
			"operation": "search",
			"table":     "test_articles",
			"where":     "invalid",
		}

		_, err := loader.LoadQuery(schema)
		if err == nil {
			t.Error("Expected error for invalid where clause")
		}
	})
}

func hasSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}
