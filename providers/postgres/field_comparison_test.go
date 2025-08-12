package postgres_test

import (
	"fmt"
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestFieldComparisons(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("JOIN with field comparison", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users", "u")).
			Fields(
				astql.F("id").WithTable("u"),
				astql.F("name").WithTable("u"),
				astql.F("name").WithTable("m"),
			).
			LeftJoin(
				astql.T("test_users", "m"),
				astql.CF(
					astql.F("manager_id").WithTable("u"),
					astql.EQ,
					astql.F("id").WithTable("m"),
				),
			).
			Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT u.id, u.name, m.name FROM test_users u LEFT JOIN test_users m ON u.manager_id = m.id WHERE u.active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should only have one parameter (isActive)
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})

	t.Run("WHERE with field comparison", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Where(
				astql.And(
					astql.C(astql.F("active"), astql.EQ, astql.P("isActive")),
					astql.CF(astql.F("created_at"), astql.GT, astql.F("updated_at")),
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE (active = :isActive AND created_at > updated_at)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Mixed comparisons in OR", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Where(
				astql.Or(
					astql.CF(astql.F("manager_id"), astql.EQ, astql.F("id")),       // Self-managed
					astql.C(astql.F("manager_id"), astql.IsNull, astql.P("dummy")), // No manager
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE (manager_id = id OR manager_id IS NULL)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have no parameters (IsNull doesn't use params)
		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("Field comparison with table aliases", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users", "a")).
			InnerJoin(
				astql.T("test_users", "b"),
				astql.And(
					astql.CF(
						astql.F("manager_id").WithTable("a"),
						astql.EQ,
						astql.F("id").WithTable("b"),
					),
					astql.CF(
						astql.F("created_at").WithTable("a"),
						astql.GT,
						astql.F("created_at").WithTable("b"),
					),
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users a INNER JOIN test_users b ON (a.manager_id = b.id AND a.created_at > b.created_at)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Field comparison validation", func(t *testing.T) {
		// Both fields must be valid
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid field")
			}
		}()

		// This should panic because "invalid_field" is not registered
		postgres.Select(astql.T("test_users")).
			Where(
				astql.CF(
					astql.F("invalid_field"),
					astql.EQ,
					astql.F("id"),
				),
			).
			MustBuild()
	})

	t.Run("Complex nested conditions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Where(
				astql.And(
					astql.C(astql.F("active"), astql.EQ, astql.P("isActive")),
					astql.Or(
						astql.CF(astql.F("age"), astql.GT, astql.F("manager_id")), // Weird but valid
						astql.And(
							astql.CF(astql.F("created_at"), astql.LT, astql.F("updated_at")),
							astql.C(astql.F("email"), astql.LIKE, astql.P("pattern")),
						),
					),
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE (active = :isActive AND (age > manager_id OR (created_at < updated_at AND email LIKE :pattern)))"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have 2 parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})
}

func ExampleCF() {
	// Setup test models
	astql.SetupTestModels()

	// Build a query with field comparison in JOIN
	ast := postgres.Select(astql.T("test_users", "u")).
		Fields(
			astql.F("name").WithTable("u"),
			astql.F("name").WithTable("m"),
		).
		LeftJoin(
			astql.T("test_users", "m"),
			astql.CF(
				astql.F("manager_id").WithTable("u"),
				astql.EQ,
				astql.F("id").WithTable("m"),
			),
		).
		MustBuild()

	provider := postgres.NewProvider()
	result, _ := provider.Render(ast)

	fmt.Println("SQL:", result.SQL)

	// Output:
	// SQL: SELECT u.name, m.name FROM test_users u LEFT JOIN test_users m ON u.manager_id = m.id
}
