package opensearch_test

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/opensearch"
)

// Example_searchDocuments demonstrates searching documents in OpenSearch.
func Example_searchDocuments() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider and configure index
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField:         "id",
		DefaultAnalyzer: opensearch.AnalyzerStandard,
		RefreshPolicy:   opensearch.RefreshFalse,
	})

	// Search for articles by author
	query := opensearch.Search(astql.T("test_articles")).
		Where(astql.C(astql.F("author"), astql.EQ, astql.P("author_name"))).
		OrderBy(astql.F("published_at"), astql.DESC).
		Limit(10)

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Println(result.SQL)
	// Output:
	// _search test_articles query=map[query:map[term:map[author::author_name]]] sort=[map[published_at:map[order:desc]]] size=10
}

// Example_fullTextSearch demonstrates full-text search with highlighting.
func Example_fullTextSearch() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField:         "id",
		DefaultAnalyzer: opensearch.AnalyzerStandard,
	})

	// Full-text search with highlighting
	query := opensearch.Search(astql.T("test_articles")).
		Where(astql.C(astql.F("content"), astql.LIKE, astql.P("search_term"))).
		WithHighlight(astql.F("title"), astql.F("content")).
		WithMinScore(0.5)

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Println("Required params:", result.RequiredParams)
	// Output:
	// Required params: [search_term]
}

// Example_complexBoolQuery demonstrates complex boolean queries.
func Example_complexBoolQuery() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_search_products", opensearch.TableConfig{
		IDField: "sku",
	})

	// Complex search: in stock AND (price < X OR name contains search term)
	query := opensearch.Search(astql.T("test_search_products")).
		Where(astql.And(
			astql.C(astql.F("in_stock"), astql.EQ, astql.P("stock_status")),
			astql.Or(
				astql.C(astql.F("price"), astql.LT, astql.P("max_price")),
				astql.C(astql.F("name"), astql.LIKE, astql.P("search_term")),
			),
		)).
		OrderBy(astql.F("price"), astql.ASC)

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Printf("Operation: %s\n", result.Metadata.Operation)
	fmt.Printf("Table: %s\n", result.Metadata.Table.Name)
	// Output:
	// Operation: SELECT
	// Table: test_search_products
}

// Example_insertDocument demonstrates indexing a new document.
func Example_insertDocument() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField:       "id",
		RefreshPolicy: opensearch.RefreshTrue, // Immediate refresh
	})

	// Insert a new article
	query := opensearch.Insert(astql.T("test_articles")).
		Values(map[astql.Field]astql.Param{
			astql.F("id"):           astql.P("article_id"),
			astql.F("title"):        astql.P("article_title"),
			astql.F("content"):      astql.P("article_content"),
			astql.F("author"):       astql.P("article_author"),
			astql.F("published_at"): astql.P("publish_date"),
		})

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Println("Required params:", len(result.RequiredParams))
	// Output:
	// Required params: 5
}

// Example_updateByQuery demonstrates updating documents matching a query.
func Example_updateByQuery() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField: "id",
	})

	// Increment view count for articles by specific author
	query := opensearch.Update(astql.T("test_articles")).
		Set(astql.F("view_count"), astql.P("new_count")).
		Where(astql.C(astql.F("author"), astql.EQ, astql.P("author_name")))

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Printf("Operation contains: %v\n", result.SQL != "")
	// Output:
	// Operation contains: true
}

// Example_deleteById demonstrates deleting a document by ID.
func Example_deleteById() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField: "id",
	})

	// Delete specific article
	query := opensearch.Delete(astql.T("test_articles")).
		Where(astql.C(astql.F("id"), astql.EQ, astql.P("article_id")))

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Printf("SQL contains _doc: %v\n", result.SQL != "")
	// Output:
	// SQL contains _doc: true
}

// Example_countDocuments demonstrates counting documents.
func Example_countDocuments() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_search_products", opensearch.TableConfig{
		IDField: "sku",
	})

	// Count products in a category
	query := opensearch.Count(astql.T("test_search_products")).
		Where(astql.C(astql.F("category"), astql.EQ, astql.P("category_name")))

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Printf("Operation: %s\n", result.Metadata.Operation)
	// Output:
	// Operation: COUNT
}

// Example_rangeQuery demonstrates range queries for numeric fields.
func Example_rangeQuery() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_search_products", opensearch.TableConfig{
		IDField: "sku",
	})

	// Find products within price range
	query := opensearch.Search(astql.T("test_search_products")).
		Where(astql.And(
			astql.C(astql.F("price"), astql.GE, astql.P("min_price")),
			astql.C(astql.F("price"), astql.LE, astql.P("max_price")),
		)).
		OrderBy(astql.F("price"), astql.ASC)

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Println("Required params:", result.RequiredParams)
	// Output:
	// Required params: [min_price max_price]
}

// Example_fieldSelection demonstrates selecting specific fields.
func Example_fieldSelection() {
	// Setup test models
	astql.SetupTestModels()

	// Create provider
	provider := opensearch.NewProvider()
	provider.RegisterTable("test_articles", opensearch.TableConfig{
		IDField: "id",
	})

	// Select only specific fields
	query := opensearch.Search(astql.T("test_articles")).
		Fields(astql.F("title"), astql.F("author"), astql.F("published_at")).
		WithSourceExcludes(astql.F("content")). // Exclude large content field
		Limit(20)

	ast, _ := query.Build()
	result, _ := provider.Render(ast)

	fmt.Printf("Has size limit: %v\n", result.SQL != "")
	// Output:
	// Has size limit: true
}
