package postgres_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"

	"gopkg.in/yaml.v3"
)

func TestFieldComparisonSchema(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("Field comparison in JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "SELECT",
			"table": "test_users",
			"alias": "u",
			"fields": ["id", "name"],
			"where": {
				"logic": "AND",
				"conditions": [
					{
						"field": "active",
						"operator": "=",
						"param": "isActive"
					},
					{
						"left_field": "created_at",
						"operator": ">",
						"right_field": "updated_at"
					}
				]
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

		expected := "SELECT id, name FROM test_users u WHERE (active = :isActive AND created_at > updated_at)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should only have one parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})

	t.Run("JOIN with field comparison in YAML", func(t *testing.T) {
		yamlQuery := `
operation: SELECT
table: test_users
alias: u
fields:
  - id
  - name
where:
  left_field: manager_id
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

		expected := "SELECT id, name FROM test_users u WHERE manager_id = id"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Should have no parameters
		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("Mixed conditions in schema", func(t *testing.T) {
		schema := postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Where: &postgres.ConditionSchema{
				Logic: "OR",
				Conditions: []postgres.ConditionSchema{
					{
						Field:    "email",
						Operator: "LIKE",
						Param:    "pattern",
					},
					{
						LeftField:  "manager_id",
						Operator:   "=",
						RightField: "id",
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

		expected := "SELECT * FROM test_users WHERE (email LIKE :pattern OR manager_id = id)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "pattern" {
			t.Errorf("Expected params [pattern], got %v", result.RequiredParams)
		}
	})
}
