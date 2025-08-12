package postgres_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"

	"gopkg.in/yaml.v3"
)

func TestBuildFromSchema(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Simple SELECT from JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "SELECT",
			"table": "test_users",
			"fields": ["id", "name", "email"],
			"where": {
				"field": "age",
				"operator": ">",
				"param": "minAge"
			},
			"order_by": [
				{"field": "name", "direction": "ASC"}
			],
			"limit": 10
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.Operation != astql.OpSelect {
			t.Errorf("Expected SELECT operation, got %s", ast.Operation)
		}

		if len(ast.QueryAST.Fields) != 3 {
			t.Errorf("Expected 3 fields, got %d", len(ast.QueryAST.Fields))
		}

		if *ast.QueryAST.Limit != 10 {
			t.Errorf("Expected limit 10, got %d", *ast.QueryAST.Limit)
		}
	})

	t.Run("Complex WHERE from YAML", func(t *testing.T) {
		yamlQuery := `
operation: SELECT
table: test_users
where:
  logic: OR
  conditions:
    - field: age
      operator: "<"
      param: minAge
    - logic: AND
      conditions:
        - field: name
          operator: LIKE
          param: namePattern
        - field: email
          operator: NOT LIKE
          param: emailPattern
`

		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.QueryAST.WhereClause == nil {
			t.Fatal("Expected WHERE clause")
		}

		// Check it's an OR group at the top level
		group, ok := ast.QueryAST.WhereClause.(astql.ConditionGroup)
		if !ok {
			t.Fatal("Expected ConditionGroup at top level")
		}

		if group.Logic != astql.OR {
			t.Errorf("Expected OR logic, got %s", group.Logic)
		}
	})

	t.Run("INSERT from JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "INSERT",
			"table": "test_users",
			"values": [
				{
					"name": "userName",
					"email": "userEmail",
					"age": "userAge"
				}
			]
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.Operation != astql.OpInsert {
			t.Errorf("Expected INSERT operation, got %s", ast.Operation)
		}

		if len(ast.QueryAST.Values) != 1 {
			t.Errorf("Expected 1 value set, got %d", len(ast.QueryAST.Values))
		}
	})

	t.Run("UPDATE from YAML", func(t *testing.T) {
		yamlQuery := `
operation: UPDATE
table: test_users
updates:
  name: newName
  email: newEmail
where:
  field: id
  operator: "="
  param: userId
`

		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.Operation != astql.OpUpdate {
			t.Errorf("Expected UPDATE operation, got %s", ast.Operation)
		}

		if len(ast.QueryAST.Updates) != 2 {
			t.Errorf("Expected 2 updates, got %d", len(ast.QueryAST.Updates))
		}
	})

	t.Run("DELETE from JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "DELETE",
			"table": "test_users",
			"where": {
				"field": "id",
				"operator": "=",
				"param": "userId"
			}
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.Operation != astql.OpDelete {
			t.Errorf("Expected DELETE operation, got %s", ast.Operation)
		}

		if ast.QueryAST.WhereClause == nil {
			t.Error("Expected WHERE clause for DELETE")
		}
	})

	t.Run("SELECT * (no fields)", func(t *testing.T) {
		jsonQuery := `{
			"operation": "SELECT",
			"table": "test_users"
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.QueryAST.Fields != nil {
			t.Error("Expected nil fields for SELECT *")
		}
	})

	t.Run("Invalid field returns error", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Fields:    []string{"invalid_field"},
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Error("Expected error for invalid field")
		}
		if !strings.Contains(err.Error(), "invalid_field") {
			t.Errorf("Error should mention invalid field: %v", err)
		}
	})

	t.Run("Invalid table returns error", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "invalid_table",
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Error("Expected error for invalid table")
		}
		if !strings.Contains(err.Error(), "invalid_table") {
			t.Errorf("Error should mention invalid table: %v", err)
		}
	})

	t.Run("COUNT from JSON", func(t *testing.T) {
		jsonQuery := `{
			"operation": "COUNT",
			"table": "test_users",
			"where": {
				"field": "age",
				"operator": ">=",
				"param": "minAge"
			}
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		if ast.Operation != astql.OpCount {
			t.Errorf("Expected COUNT operation, got %s", ast.Operation)
		}

		if ast.QueryAST.WhereClause == nil {
			t.Error("Expected WHERE clause")
		}
	})
}
