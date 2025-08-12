package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestCountDistinct(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("Basic COUNT(DISTINCT)", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.CountDistinct(astql.F("email")).As("unique_emails")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COUNT(DISTINCT email) AS unique_emails FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COUNT(DISTINCT) with regular fields", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("name")).
			SelectExpr(postgres.CountDistinct(astql.F("email")).As("unique_emails")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT name, COUNT(DISTINCT email) AS unique_emails FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COUNT(DISTINCT) with table alias", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users", "u")).
			SelectExpr(postgres.CountDistinct(astql.F("email").WithTable("u")).As("unique_emails")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COUNT(DISTINCT u.email) AS unique_emails FROM test_users u"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COUNT(DISTINCT) with GROUP BY", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("active")).
			SelectExpr(postgres.CountDistinct(astql.F("email")).As("unique_emails")).
			GroupBy(astql.F("active")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT active, COUNT(DISTINCT email) AS unique_emails FROM test_users GROUP BY active"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Multiple aggregates including COUNT(DISTINCT)", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.CountField(astql.F("id")).As("count")).
			SelectExpr(postgres.CountDistinct(astql.F("email")).As("unique_emails")).
			SelectExpr(postgres.CountDistinct(astql.F("name")).As("unique_names")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COUNT(id) AS count, COUNT(DISTINCT email) AS unique_emails, COUNT(DISTINCT name) AS unique_names FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("COUNT(DISTINCT) requires valid field", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid field")
			}
		}()

		// This should panic
		_ = postgres.CountDistinct(astql.F("invalid_field"))
	})

	t.Run("COUNT(DISTINCT) with WHERE clause", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.CountDistinct(astql.F("email")).As("unique_emails")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COUNT(DISTINCT email) AS unique_emails FROM test_users WHERE active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Check parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})
}
