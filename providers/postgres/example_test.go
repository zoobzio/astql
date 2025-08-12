package postgres_test

import (
	"fmt"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func ExampleProvider() {
	// Setup test models
	astql.SetupTestModels()

	// Create a provider
	provider := postgres.NewProvider()

	// Build a complex query
	ast := postgres.Select(astql.T("test_users", "u")).
		Fields(astql.F("name").WithTable("u")).
		Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
		OrderBy(astql.F("created_at").WithTable("u"), astql.DESC).
		Limit(5).
		MustBuild()

	// Render to SQL
	result, err := provider.Render(ast)
	if err != nil {
		panic(err)
	}

	fmt.Println("SQL:", result.SQL)
	fmt.Println("Parameters:", result.RequiredParams)

	// Output:
	// SQL: SELECT u.name FROM test_users u WHERE u.active = :isActive ORDER BY u.created_at DESC LIMIT 5
	// Parameters: [isActive]
}

func ExampleProvider_withMetadata() {
	// Setup test models
	astql.SetupTestModels()

	// Create a provider
	provider := postgres.NewProvider()

	// Build a query
	ast := postgres.Select(astql.T("test_users")).
		Fields(astql.F("id"), astql.F("name"), astql.F("email")).
		Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
		Limit(10).
		MustBuild()

	// Render with metadata
	result, err := provider.Render(ast)
	if err != nil {
		panic(err)
	}

	fmt.Println("SQL:", result.SQL)
	fmt.Println("Required params:", result.RequiredParams)
	fmt.Println("Table:", result.Metadata.Table.Name)
	fmt.Println("Expected type:", result.Metadata.Table.TypeName)
	fmt.Println("Result type:", result.Metadata.ResultType)
	fmt.Println("Returned fields:", len(result.Metadata.ReturnedFields))

	// Output:
	// SQL: SELECT id, name, email FROM test_users WHERE active = :isActive LIMIT 10
	// Required params: [isActive]
	// Table: test_users
	// Expected type: TestUser
	// Result type: multiple
	// Returned fields: 3
}

func ExampleFieldComparison() {
	// Setup test models
	astql.SetupTestModels()

	// Build a self-referential query to find users who are their own manager
	ast := postgres.Select(astql.T("test_users")).
		Fields(astql.F("id"), astql.F("name")).
		Where(
			astql.And(
				astql.CF(astql.F("manager_id"), astql.EQ, astql.F("id")), // Self-managed
				astql.C(astql.F("active"), astql.EQ, astql.P("isActive")),
			),
		).
		MustBuild()

	// Render with provider
	provider := postgres.NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		panic(err)
	}

	fmt.Println("SQL:", result.SQL)
	fmt.Println("Params:", result.RequiredParams)
}
