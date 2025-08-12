package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestCoalesceAndNullIf(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("Basic COALESCE", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectCoalesce(
				postgres.Coalesce(astql.P("nickname"), astql.P("firstName"), astql.P("defaultName")).
					As("display_name"),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, COALESCE(:nickname, :firstName, :defaultName) AS display_name FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have all three parameters
		if len(result.RequiredParams) != 3 {
			t.Errorf("Expected 3 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("COALESCE without alias", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectCoalesce(
				postgres.Coalesce(astql.P("field1"), astql.P("field2")),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COALESCE(:field1, :field2) FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COALESCE with many values", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectCoalesce(
				postgres.Coalesce(
					astql.P("val1"),
					astql.P("val2"),
					astql.P("val3"),
					astql.P("val4"),
					astql.P("defaultVal"),
				).As("result"),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COALESCE(:val1, :val2, :val3, :val4, :defaultVal) AS result FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Basic NULLIF", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectNullIf(
				postgres.NullIf(astql.P("statusField"), astql.P("deletedValue")).
					As("status"),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, NULLIF(:statusField, :deletedValue) AS status FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have both parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("Multiple expressions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectCoalesce(
				postgres.Coalesce(astql.P("nickname"), astql.P("name")).As("display_name"),
			).
			SelectNullIf(
				postgres.NullIf(astql.P("status"), astql.P("inactive")).As("active_status"),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, COALESCE(:nickname, :name) AS display_name, NULLIF(:status, :inactive) AS active_status FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COALESCE requires at least 2 values", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for COALESCE with < 2 values")
			}
		}()

		// This should panic
		postgres.Coalesce(astql.P("single"))
	})

	t.Run("Invalid alias should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid alias")
			}
		}()

		// This should panic - unregistered alias
		postgres.Coalesce(astql.P("val1"), astql.P("val2")).
			As("invalid_unregistered_alias")
	})
}

func TestCoalesceWithOtherFeatures(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("COALESCE with aggregates and CASE", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectExpr(postgres.Sum(astql.F("age")).As("total_age")).
			SelectCase(
				postgres.Case().
					When(astql.C(astql.F("active"), astql.EQ, astql.P("isActive")), astql.P("activeStatus")).
					Else(astql.P("inactiveStatus")).
					As("status").
					Build(),
			).
			SelectCoalesce(
				postgres.Coalesce(astql.P("nickname"), astql.P("name"), astql.P("anonymous")).
					As("display_name"),
			).
			GroupBy(astql.F("id")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, SUM(age) AS total_age, CASE WHEN active = :isActive THEN :activeStatus ELSE :inactiveStatus END AS status, COALESCE(:nickname, :name, :anonymous) AS display_name FROM test_users GROUP BY id"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})
}
