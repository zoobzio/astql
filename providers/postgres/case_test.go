package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestCaseExpressions(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("Basic CASE in SELECT", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("name")).
			SelectCase(
				postgres.Case().
					When(astql.C(astql.F("age"), astql.LT, astql.P("minorAge")), astql.P("minorLabel")).
					When(astql.C(astql.F("age"), astql.LT, astql.P("seniorAge")), astql.P("adultLabel")).
					Else(astql.P("seniorLabel")).
					As("age_group").
					Build(),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT name, CASE WHEN age < :minorAge THEN :minorLabel WHEN age < :seniorAge THEN :adultLabel ELSE :seniorLabel END AS age_group FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have all the parameters
		expectedParams := []string{"minorAge", "minorLabel", "seniorAge", "adultLabel", "seniorLabel"}
		if len(result.RequiredParams) != len(expectedParams) {
			t.Errorf("Expected %d params, got %d: %v", len(expectedParams), len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("CASE without ELSE", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectCase(
				postgres.Case().
					When(astql.C(astql.F("active"), astql.EQ, astql.P("trueValue")), astql.P("activeLabel")).
					When(astql.C(astql.F("active"), astql.EQ, astql.P("falseValue")), astql.P("inactiveLabel")).
					Build(),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT CASE WHEN active = :trueValue THEN :activeLabel WHEN active = :falseValue THEN :inactiveLabel END FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("CASE with complex conditions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			SelectCase(
				postgres.Case().
					When(
						astql.And(
							astql.C(astql.F("age"), astql.GE, astql.P("minAge")),
							astql.C(astql.F("age"), astql.LE, astql.P("maxAge")),
						),
						astql.P("inRangeLabel"),
					).
					Else(astql.P("outOfRangeLabel")).
					As("age_range").
					Build(),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT id, CASE WHEN (age >= :minAge AND age <= :maxAge) THEN :inRangeLabel ELSE :outOfRangeLabel END AS age_range FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("CASE in WHERE clause", func(t *testing.T) {
		// For now, skip this test as it requires a more complex implementation
		// CASE in WHERE would need a special condition type that compares CASE result to a value
		t.Skip("CASE in WHERE clause requires additional implementation")
	})

	t.Run("Multiple CASE expressions", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			SelectCase(
				postgres.Case().
					When(astql.C(astql.F("age"), astql.LT, astql.P("adultAge")), astql.P("minorLabel")).
					Else(astql.P("adultLabel")).
					As("age_category").
					Build(),
			).
			SelectCase(
				postgres.Case().
					When(astql.C(astql.F("active"), astql.EQ, astql.P("trueValue")), astql.P("activeLabel")).
					Else(astql.P("inactiveLabel")).
					As("status_label").
					Build(),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT CASE WHEN age < :adultAge THEN :minorLabel ELSE :adultLabel END AS age_category, CASE WHEN active = :trueValue THEN :activeLabel ELSE :inactiveLabel END AS status_label FROM test_users"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Invalid alias should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid alias")
			}
		}()

		// This should panic - unregistered alias
		postgres.Case().
			When(astql.C(astql.F("age"), astql.LT, astql.P("limit")), astql.P("label")).
			As("invalid_alias_not_registered").
			Build()
	})
}

func TestCaseWithFieldComparison(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	// Test CASE with field-to-field comparison
	ast := postgres.Select(astql.T("test_users")).
		SelectCase(
			postgres.Case().
				When(astql.CF(astql.F("created_at"), astql.GT, astql.F("updated_at")), astql.P("recentlyCreated")).
				When(astql.CF(astql.F("created_at"), astql.LT, astql.F("updated_at")), astql.P("recentlyUpdated")).
				Else(astql.P("unchanged")).
				As("status").
				Build(),
		).
		MustBuild()

	result, err := provider.Render(ast)
	if err != nil {
		t.Fatal(err)
	}

	expected := "SELECT CASE WHEN created_at > updated_at THEN :recentlyCreated WHEN created_at < updated_at THEN :recentlyUpdated ELSE :unchanged END AS status FROM test_users"
	if result.SQL != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
	}
}
