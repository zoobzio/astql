package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestSubqueries(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("Simple IN subquery", func(t *testing.T) {
		// SELECT * FROM test_orders WHERE customer_id IN (SELECT id FROM test_customers WHERE country = :country)
		subquery := astql.Sub(
			astql.Select(astql.T("test_customers")).
				Fields(astql.F("id")).
				Where(astql.C(astql.F("country"), astql.EQ, astql.P("country"))),
		)

		ast := postgres.Select(astql.T("test_orders")).
			Where(astql.CSub(astql.F("customer_id"), astql.IN, subquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_orders WHERE customer_id IN (SELECT id FROM test_customers WHERE country = :sq1_country)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have the namespaced parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "sq1_country" {
			t.Errorf("Expected params [sq1_country], got %v", result.RequiredParams)
		}
	})

	t.Run("NOT IN subquery", func(t *testing.T) {
		// SELECT * FROM test_users WHERE id NOT IN (SELECT manager_id FROM test_users WHERE manager_id IS NOT NULL)
		subquery := postgres.SubPG(
			postgres.Select(astql.T("test_users")).
				Fields(astql.F("manager_id")).
				Where(astql.C(astql.F("manager_id"), astql.IsNotNull, astql.P("dummy"))),
		)

		ast := postgres.Select(astql.T("test_users")).
			Where(astql.CSub(astql.F("id"), astql.NotIn, subquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE id NOT IN (SELECT manager_id FROM test_users WHERE manager_id IS NOT NULL)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have no parameters (IS NOT NULL doesn't use params)
		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("EXISTS subquery", func(t *testing.T) {
		// SELECT * FROM test_customers c WHERE EXISTS (SELECT 1 FROM test_orders o WHERE o.customer_id = c.id)
		field := astql.F("id") // For the subquery to reference
		subquery := postgres.SubPG(
			postgres.Select(astql.T("test_orders", "o")).
				Fields(astql.F("id")). // Just select something
				Where(astql.CF(
					astql.F("customer_id").WithTable("o"),
					astql.EQ,
					field.WithTable("c"),
				)),
		)

		ast := postgres.Select(astql.T("test_customers", "c")).
			Where(astql.CSubExists(astql.EXISTS, subquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_customers c WHERE EXISTS (SELECT id FROM test_orders o WHERE o.customer_id = c.id)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have no parameters
		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("NOT EXISTS subquery", func(t *testing.T) {
		// SELECT * FROM test_products WHERE NOT EXISTS (SELECT 1 FROM test_orders WHERE product_id = test_products.id)
		subquery := postgres.SubPG(
			postgres.Select(astql.T("test_orders")).
				Fields(astql.F("id")).
				Where(astql.CF(
					astql.F("product_id"),
					astql.EQ,
					astql.F("id").WithTable("test_products"),
				)),
		)

		ast := postgres.Select(astql.T("test_products")).
			Where(astql.CSubExists(astql.NotExists, subquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_products WHERE NOT EXISTS (SELECT id FROM test_orders WHERE product_id = test_products.id)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})

	t.Run("Nested subqueries", func(t *testing.T) {
		// SELECT * FROM test_users WHERE id IN (
		//   SELECT user_id FROM test_posts WHERE category_id IN (
		//     SELECT id FROM test_categorys WHERE name = :category
		//   )
		// )
		innerSubquery := astql.Sub(
			astql.Select(astql.T("test_categorys")).
				Fields(astql.F("id")).
				Where(astql.C(astql.F("name"), astql.EQ, astql.P("category"))),
		)

		outerSubquery := astql.Sub(
			astql.Select(astql.T("test_posts")).
				Fields(astql.F("user_id")).
				Where(astql.CSub(astql.F("category_id"), astql.IN, innerSubquery)),
		)

		ast := postgres.Select(astql.T("test_users")).
			Where(astql.CSub(astql.F("id"), astql.IN, outerSubquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE id IN (SELECT user_id FROM test_posts WHERE category_id IN (SELECT id FROM test_categorys WHERE name = :sq2_category))"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have the deeply nested parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "sq2_category" {
			t.Errorf("Expected params [sq2_category], got %v", result.RequiredParams)
		}
	})

	t.Run("Subquery with multiple parameters", func(t *testing.T) {
		// SELECT * FROM test_orders WHERE customer_id IN (
		//   SELECT id FROM test_customers WHERE country = :country AND active = :active
		// )
		subquery := astql.Sub(
			astql.Select(astql.T("test_customers")).
				Fields(astql.F("id")).
				Where(astql.And(
					astql.C(astql.F("country"), astql.EQ, astql.P("country")),
					astql.C(astql.F("active"), astql.EQ, astql.P("active")),
				)),
		)

		ast := postgres.Select(astql.T("test_orders")).
			Where(astql.CSub(astql.F("customer_id"), astql.IN, subquery)).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_orders WHERE customer_id IN (SELECT id FROM test_customers WHERE (country = :sq1_country AND active = :sq1_active))"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have both namespaced parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("Invalid operator for subquery", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid operator")
			}
		}()

		// This should panic - can't use = with subqueries
		subquery := astql.Sub(
			astql.Select(astql.T("test_users")).Fields(astql.F("id")),
		)

		astql.CSub(astql.F("id"), astql.EQ, subquery)
	})

	t.Run("EXISTS with wrong function should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for EXISTS with CSub")
			}
		}()

		// This should panic - EXISTS should use CSubExists
		subquery := astql.Sub(
			astql.Select(astql.T("test_users")).Fields(astql.F("id")),
		)

		astql.CSub(astql.F("id"), astql.EXISTS, subquery)
	})

	t.Run("IN with CSubExists should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for IN with CSubExists")
			}
		}()

		// This should panic - IN should use CSub
		subquery := astql.Sub(
			astql.Select(astql.T("test_users")).Fields(astql.F("id")),
		)

		astql.CSubExists(astql.IN, subquery)
	})
}
