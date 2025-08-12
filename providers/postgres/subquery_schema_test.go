package postgres_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"

	"gopkg.in/yaml.v3"
)

func TestSubquerySchema(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("IN subquery in JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "SELECT",
			"table": "test_orders",
			"where": {
				"field": "customer_id",
				"operator": "IN",
				"subquery": {
					"operation": "SELECT",
					"table": "test_customers",
					"fields": ["id"],
					"where": {
						"field": "country",
						"operator": "=",
						"param": "country"
					}
				}
			}
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatal(err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatal(err)
		}

		// Render directly - ast is already a postgres.AST
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_orders WHERE customer_id IN (SELECT id FROM test_customers WHERE country = :sq1_country)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have one parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "sq1_country" {
			t.Errorf("Expected params [sq1_country], got %v", result.RequiredParams)
		}
	})

	t.Run("EXISTS subquery in YAML", func(t *testing.T) {
		yamlQuery := `
operation: SELECT
table: test_customers
alias: c
where:
  operator: EXISTS
  subquery:
    operation: SELECT
    table: test_orders
    alias: o
    fields: [id]
    where:
      left_field: customer_id
      operator: "="
      right_field: id
`

		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlQuery), &schema); err != nil {
			t.Fatal(err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatal(err)
		}

		// Render directly - ast is already a postgres.AST
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_customers c WHERE EXISTS (SELECT id FROM test_orders o WHERE customer_id = id)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have no parameters
		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("Complex nested subquery schema", func(t *testing.T) {
		schema := postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Where: &postgres.ConditionSchema{
				Logic: "AND",
				Conditions: []postgres.ConditionSchema{
					{
						Field:    "active",
						Operator: "=",
						Param:    "isActive",
					},
					{
						Field:    "id",
						Operator: "IN",
						Subquery: &postgres.QuerySchema{
							Operation: "SELECT",
							Table:     "test_posts",
							Fields:    []string{"user_id"},
							Where: &postgres.ConditionSchema{
								Field:    "category_id",
								Operator: "IN",
								Subquery: &postgres.QuerySchema{
									Operation: "SELECT",
									Table:     "test_categorys",
									Fields:    []string{"id"},
									Where: &postgres.ConditionSchema{
										Field:    "name",
										Operator: "=",
										Param:    "category",
									},
								},
							},
						},
					},
				},
			},
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatal(err)
		}

		// Render directly - ast is already a postgres.AST
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE (active = :isActive AND id IN (SELECT user_id FROM test_posts WHERE category_id IN (SELECT id FROM test_categorys WHERE name = :sq2_category)))"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have two parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("NOT EXISTS with field should fail", func(t *testing.T) {
		schema := postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_products",
			Where: &postgres.ConditionSchema{
				Field:    "id", // This should cause an error
				Operator: "NOT EXISTS",
				Subquery: &postgres.QuerySchema{
					Operation: "SELECT",
					Table:     "test_orders",
					Fields:    []string{"id"},
				},
			},
		}

		_, err := postgres.BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for NOT EXISTS with field")
		}
		if err.Error() != "invalid where clause: NOT EXISTS operator does not take a field" {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("IN without field should fail", func(t *testing.T) {
		schema := postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_orders",
			Where: &postgres.ConditionSchema{
				// Missing field
				Operator: "IN",
				Subquery: &postgres.QuerySchema{
					Operation: "SELECT",
					Table:     "test_customers",
					Fields:    []string{"id"},
				},
			},
		}

		_, err := postgres.BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for IN without field")
		}
		if err.Error() != "invalid where clause: IN operator requires a field" {
			t.Errorf("Unexpected error: %v", err)
		}
	})
}
