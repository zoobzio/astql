package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
)

func TestPostgresBuilderExtensions(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("RightJoin method", func(t *testing.T) {
		builder := postgres.Select(astql.T("test_users", "u")).
			RightJoin(
				astql.T("test_orders", "o"),
				astql.CF(astql.F("id").WithTable("u"), astql.EQ, astql.F("customer_id").WithTable("o")),
			)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build with RightJoin: %v", err)
		}

		if len(ast.Joins) != 1 {
			t.Fatalf("Expected 1 join, got %d", len(ast.Joins))
		}

		join := ast.Joins[0]
		if join.Type != postgres.RightJoin {
			t.Errorf("Expected RIGHT join type, got %v", join.Type)
		}
		if join.Table.Name != "test_orders" {
			t.Errorf("Expected join table 'test_orders', got %s", join.Table.Name)
		}
	})

	t.Run("WhereField method", func(t *testing.T) {
		// WhereField takes field, operator, param
		builder := postgres.Select(astql.T("test_users")).
			WhereField(astql.F("created_at"), astql.LT, astql.P("cutoff_date"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build with WhereField: %v", err)
		}

		if ast.WhereClause == nil {
			t.Fatal("Expected WHERE clause")
		}

		// Verify it's a condition
		if _, ok := ast.WhereClause.(astql.Condition); !ok {
			t.Error("Expected Condition in WHERE clause")
		}
	})

	t.Run("Offset method", func(t *testing.T) {
		builder := postgres.Select(astql.T("test_users")).
			Limit(10).
			Offset(20)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build with Offset: %v", err)
		}

		if ast.Offset == nil {
			t.Fatal("Expected offset to be set")
		}
		if *ast.Offset != 20 {
			t.Errorf("Expected offset 20, got %d", *ast.Offset)
		}
	})

	t.Run("Multiple WhereField creates AND condition", func(t *testing.T) {
		builder := postgres.Select(astql.T("test_users")).
			WhereField(astql.F("age"), astql.GT, astql.P("min_age")).
			WhereField(astql.F("age"), astql.LT, astql.P("max_age"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		// Should create an AND group
		group, ok := ast.WhereClause.(astql.ConditionGroup)
		if !ok {
			t.Fatal("Expected ConditionGroup for multiple WhereField")
		}
		if group.Logic != astql.AND {
			t.Errorf("Expected AND logic, got %s", group.Logic)
		}
		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
		}
	})

	t.Run("Complex join query with all join types", func(t *testing.T) {
		builder := postgres.Select(astql.T("test_users", "u")).
			InnerJoin(
				astql.T("test_posts", "p"),
				astql.CF(astql.F("id").WithTable("u"), astql.EQ, astql.F("user_id").WithTable("p")),
			).
			LeftJoin(
				astql.T("test_categories", "c"),
				astql.CF(astql.F("category_id").WithTable("p"), astql.EQ, astql.F("id").WithTable("c")),
			).
			RightJoin(
				astql.T("test_orders", "o"),
				astql.CF(astql.F("id").WithTable("u"), astql.EQ, astql.F("customer_id").WithTable("o")),
			)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build complex join: %v", err)
		}

		if len(ast.Joins) != 3 {
			t.Fatalf("Expected 3 joins, got %d", len(ast.Joins))
		}

		// Verify join types
		expectedTypes := []postgres.JoinType{
			postgres.InnerJoin,
			postgres.LeftJoin,
			postgres.RightJoin,
		}

		for i, join := range ast.Joins {
			if join.Type != expectedTypes[i] {
				t.Errorf("Join %d: expected type %v, got %v", i, expectedTypes[i], join.Type)
			}
		}
	})

	t.Run("Builder with pagination", func(t *testing.T) {
		builder := postgres.Select(astql.T("test_users")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("status"))).
			OrderBy(astql.F("created_at"), astql.DESC).
			Limit(50).
			Offset(100)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		if ast.Limit == nil || *ast.Limit != 50 {
			t.Error("Expected limit 50")
		}
		if ast.Offset == nil || *ast.Offset != 100 {
			t.Error("Expected offset 100")
		}
	})
}
