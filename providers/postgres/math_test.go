package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestMathFunctions(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("ROUND with precision", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectMath(postgres.Round(astql.F("age"), astql.P("precision")).As("rounded_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, ROUND(age, :precision) AS rounded_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Check parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "precision" {
			t.Errorf("Expected params [precision], got %v", result.RequiredParams)
		}
	})

	t.Run("ROUND without precision", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Round(astql.F("age")).As("rounded_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT ROUND(age) AS rounded_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("FLOOR function", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Floor(astql.F("age")).As("min_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT FLOOR(age) AS min_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("CEIL function", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Ceil(astql.F("age")).As("max_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT CEIL(age) AS max_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("ABS function", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Abs(astql.F("age")).As("total_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT ABS(age) AS total_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("POWER function", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Power(astql.F("age"), astql.P("exponent")).As("age_power")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT POWER(age, :exponent) AS age_power FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Check parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "exponent" {
			t.Errorf("Expected params [exponent], got %v", result.RequiredParams)
		}
	})

	t.Run("SQRT function", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Sqrt(astql.F("age")).As("age_sqrt")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT SQRT(age) AS age_sqrt FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Multiple math functions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("name")).
			SelectMath(postgres.Round(astql.F("age"), astql.P("precision")).As("rounded_age")).
			SelectMath(postgres.Abs(astql.F("age")).As("total_age")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT name, ROUND(age, :precision) AS rounded_age, ABS(age) AS total_age FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Math functions with WHERE clause", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectMath(postgres.Round(astql.F("age")).As("rounded_age")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT ROUND(age) AS rounded_age FROM test_users WHERE active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Math function requires valid field", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid field")
			}
		}()

		// This should panic
		postgres.Round(astql.F("invalid_field"))
	})

	t.Run("Invalid alias should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid alias")
			}
		}()

		// This should panic - unregistered alias
		postgres.Round(astql.F("age")).As("invalid_unregistered_alias")
	})

	t.Run("Math with aggregates and other expressions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.Avg(astql.F("age")).As("avg_age")).
			SelectMath(postgres.Round(astql.F("age"), astql.P("precision")).As("rounded_age")).
			SelectCoalesce(postgres.Coalesce(astql.P("nickname"), astql.P("name")).As("display_name")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT AVG(age) AS avg_age, ROUND(age, :precision) AS rounded_age, COALESCE(:nickname, :name) AS display_name FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})
}
