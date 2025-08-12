package astql_test

import (
	"fmt"

	"github.com/zoobzio/astql"
)

func ExampleSelect() {
	// Setup test models for validation
	astql.SetupTestModels()

	// Build a SELECT query using type-safe functions
	query := astql.Select(astql.T("test_users", "u")).
		Fields(
			astql.F("id"),
			astql.F("name"),
			astql.F("email"),
		).
		Where(
			astql.And(
				astql.C(astql.F("age"), astql.GT, astql.P("minAge")),
				astql.C(astql.F("email"), astql.LIKE, astql.P("emailPattern")),
			),
		).
		OrderBy(astql.F("name"), astql.ASC).
		Limit(10).
		MustBuild()

	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Table: %s\n", query.Target.Name)
	fmt.Printf("Fields: %d\n", len(query.Fields))

	// Output:
	// Operation: SELECT
	// Table: test_users
	// Fields: 3
}

func ExampleInsert() {
	// Setup test models for validation
	astql.SetupTestModels()

	// Build an INSERT query with parameters
	query := astql.Insert(astql.T("test_users")).
		Values(map[astql.Field]astql.Param{
			astql.F("name"):  astql.P("userName"),
			astql.F("email"): astql.P("userEmail"),
			astql.F("age"):   astql.P("userAge"),
		}).
		MustBuild()

	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Value sets: %d\n", len(query.Values))

	// Output:
	// Operation: INSERT
	// Value sets: 1
}

func ExampleOr() {
	// Setup test models for validation
	astql.SetupTestModels()

	// Build a query with OR conditions
	query := astql.Select(astql.T("test_users")).
		Where(
			astql.Or(
				astql.C(astql.F("age"), astql.LT, astql.P("minAge")),
				astql.C(astql.F("age"), astql.GT, astql.P("maxAge")),
			),
		).
		MustBuild()

	// The WHERE clause will be: (age < :minAge OR age > :maxAge)
	fmt.Printf("Has WHERE clause: %v\n", query.WhereClause != nil)

	// Output:
	// Has WHERE clause: true
}

func ExampleSelect_allFields() {
	// Setup test models for validation
	astql.SetupTestModels()

	// Build a SELECT * query by not specifying fields
	query := astql.Select(astql.T("test_users")).
		Where(astql.C(astql.F("email"), astql.LIKE, astql.P("pattern"))).
		MustBuild()

	// This will generate: SELECT * FROM test_users WHERE email LIKE :pattern
	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Has specific fields: %v\n", query.Fields != nil)

	// Output:
	// Operation: SELECT
	// Has specific fields: false
}

func ExampleCount() {
	// Setup test models for validation
	astql.SetupTestModels()

	// Build a COUNT query with a WHERE clause
	query := astql.Count(astql.T("test_users")).
		Where(astql.C(astql.F("age"), astql.GE, astql.P("minAge"))).
		MustBuild()

	// This will generate: SELECT COUNT(*) FROM test_users WHERE age >= :minAge
	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Has WHERE: %v\n", query.WhereClause != nil)

	// Output:
	// Operation: COUNT
	// Has WHERE: true
}
