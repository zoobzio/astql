package postgres_test

import (
	"fmt"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func ExampleSelect_withJoin() {
	// Setup test models
	astql.SetupTestModels()

	// Build a SELECT with JOIN
	query := postgres.Select(astql.T("test_users", "u")).
		Fields(
			astql.F("id").WithTable("u"),
			astql.F("name").WithTable("u"),
		).
		LeftJoin(
			astql.T("test_users", "m"),
			astql.C(
				astql.F("manager_id").WithTable("u"),
				astql.EQ,
				astql.P("managerId"), // Will be replaced with field reference later
			),
		).
		Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
		MustBuild()

	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Joins: %d\n", len(query.Joins))
	fmt.Printf("Join type: %s\n", query.Joins[0].Type)

	// Output:
	// Operation: SELECT
	// Joins: 1
	// Join type: LEFT JOIN
}

func ExampleSelect_withAggregates() {
	// Setup test models
	astql.SetupTestModels()

	// Build a SELECT with aggregates and GROUP BY
	query := postgres.Select(astql.T("test_users")).
		Fields(astql.F("age")).
		SelectExpr(postgres.CountField(astql.F("id")).As("user_count")).
		SelectExpr(postgres.Avg(astql.F("age")).As("avg_age")).
		GroupBy(astql.F("age")).
		Having(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
		OrderBy(astql.F("age"), astql.DESC).
		MustBuild()

	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Group by fields: %d\n", len(query.GroupBy))
	fmt.Printf("Aggregates: %d\n", len(query.FieldExpressions))
	fmt.Printf("Having conditions: %d\n", len(query.Having))

	// Output:
	// Operation: SELECT
	// Group by fields: 1
	// Aggregates: 2
	// Having conditions: 1
}

func ExampleInsert_withConflict() {
	// Setup test models
	astql.SetupTestModels()

	// Build an INSERT with ON CONFLICT
	query := postgres.Insert(astql.T("test_users")).
		Values(map[astql.Field]astql.Param{
			astql.F("email"): astql.P("email"),
			astql.F("name"):  astql.P("name"),
			astql.F("age"):   astql.P("age"),
		}).
		OnConflict(astql.F("email")).
		DoUpdate(map[astql.Field]astql.Param{
			astql.F("name"):       astql.P("name"),
			astql.F("updated_at"): astql.P("now"),
		}).
		Returning(astql.F("id"), astql.F("created_at")).
		MustBuild()

	fmt.Printf("Operation: %s\n", query.Operation)
	fmt.Printf("Has conflict clause: %v\n", query.OnConflict != nil)
	fmt.Printf("Conflict action: %s\n", query.OnConflict.Action)
	fmt.Printf("Returning fields: %d\n", len(query.Returning))

	// Output:
	// Operation: INSERT
	// Has conflict clause: true
	// Conflict action: DO UPDATE
	// Returning fields: 2
}
