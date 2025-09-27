package sqlite

import (
	"strings"
	"testing"

	"github.com/zoobzio/sentinel"
	"gopkg.in/yaml.v3"
)

func TestBuildFromSchemaYAML(t *testing.T) {
	// Register test struct
	sentinel.Inspect[User]()

	t.Run("Basic SELECT", func(t *testing.T) {
		yamlContent := `
operation: SELECT
table: User
fields:
  - id
  - name
  - email
where:
  field: age
  operator: ">="
  param: minAge
order_by:
  - field: name
    direction: ASC
limit: 10
`
		var schema QuerySchema
		if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
			t.Fatalf("Failed to parse YAML: %v", err)
		}

		ast, err := BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("BuildFromSchema failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		// Check key components are present
		if !strings.Contains(result.SQL, "SELECT") {
			t.Error("Expected SELECT in SQL")
		}
		if !strings.Contains(result.SQL, `"age" >=`) {
			t.Error("Expected WHERE clause with age >= condition")
		}
		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Error("Expected ORDER BY in SQL")
		}
		if !strings.Contains(result.SQL, "LIMIT 10") {
			t.Error("Expected LIMIT 10 in SQL")
		}
	})

	t.Run("INSERT with OR IGNORE", func(t *testing.T) {
		yamlContent := `
operation: INSERT
table: User
values:
  - name: userName
    email: userEmail
    age: userAge
on_conflict:
  action: nothing
`
		var schema QuerySchema
		if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
			t.Fatalf("Failed to parse YAML: %v", err)
		}

		ast, err := BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("BuildFromSchema failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "INSERT OR IGNORE") {
			t.Error("Expected INSERT OR IGNORE in SQL")
		}

		// Check parameters
		expectedParams := []string{"userName", "userEmail", "userAge"}
		if len(result.RequiredParams) != len(expectedParams) {
			t.Errorf("Expected %d parameters, got %d", len(expectedParams), len(result.RequiredParams))
		}
	})

	t.Run("UPDATE with WHERE", func(t *testing.T) {
		yamlContent := `
operation: UPDATE
table: User
updates:
  name: newName
  email: newEmail
where:
  field: id
  operator: "="
  param: userId
`
		var schema QuerySchema
		if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
			t.Fatalf("Failed to parse YAML: %v", err)
		}

		ast, err := BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("BuildFromSchema failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "UPDATE") {
			t.Error("Expected UPDATE in SQL")
		}
		if !strings.Contains(result.SQL, "SET") {
			t.Error("Expected SET in SQL")
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Error("Expected WHERE in SQL")
		}
	})

	t.Run("GROUP BY with HAVING", func(t *testing.T) {
		// Register Order struct
		sentinel.Inspect[Order]()

		yamlContent := `
operation: SELECT
table: Order
fields:
  - user_id
group_by:
  - user_id
having:
  - field: user_id
    operator: ">"
    param: minCount
`
		var schema QuerySchema
		if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
			t.Fatalf("Failed to parse YAML: %v", err)
		}

		ast, err := BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("BuildFromSchema failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Error("Expected GROUP BY in SQL")
		}
		if !strings.Contains(result.SQL, "HAVING") {
			t.Error("Expected HAVING in SQL")
		}
	})

	t.Run("ON CONFLICT DO UPDATE", func(t *testing.T) {
		yamlContent := `
operation: INSERT
table: User
values:
  - name: userName
    email: userEmail
on_conflict:
  columns:
    - email
  action: update
  updates:
    name: newName
`
		var schema QuerySchema
		if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
			t.Fatalf("Failed to parse YAML: %v", err)
		}

		ast, err := BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("BuildFromSchema failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Error("Expected ON CONFLICT in SQL")
		}
		if !strings.Contains(result.SQL, "DO UPDATE SET") {
			t.Error("Expected DO UPDATE SET in SQL")
		}
	})
}

func TestSchemaValidation(t *testing.T) {
	sentinel.Inspect[User]()

	t.Run("SQL injection in param", func(t *testing.T) {
		schema := QuerySchema{
			Operation: "SELECT",
			Table:     "User",
			Where: &ConditionSchema{
				Field:    "id",
				Operator: "=",
				Param:    "1; DROP TABLE users--",
			},
		}

		_, err := BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for SQL injection in parameter")
		}
		if !strings.Contains(err.Error(), "invalid") {
			t.Errorf("Expected 'invalid' in error, got: %v", err)
		}
	})

	t.Run("Invalid table", func(t *testing.T) {
		schema := QuerySchema{
			Operation: "SELECT",
			Table:     "nonexistent_table",
		}

		_, err := BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for invalid table")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' in error, got: %v", err)
		}
	})

	t.Run("Invalid operator", func(t *testing.T) {
		schema := QuerySchema{
			Operation: "SELECT",
			Table:     "User",
			Where: &ConditionSchema{
				Field:    "id",
				Operator: "~", // PostgreSQL regex, not allowed
				Param:    "pattern",
			},
		}

		_, err := BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for invalid operator")
		}
	})

	t.Run("buildCondition with grouped conditions", func(t *testing.T) {
		condSchema := &ConditionSchema{
			Logic: "AND",
			Conditions: []ConditionSchema{
				{Field: "status", Operator: "=", Param: "active"},
				{Field: "age", Operator: ">", Param: "minAge"},
			},
		}

		cond, err := buildCondition(condSchema)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if cond == nil {
			t.Error("Expected condition to be created")
		}
	})

	t.Run("buildCondition with OR logic", func(t *testing.T) {
		condSchema := &ConditionSchema{
			Logic: "OR",
			Conditions: []ConditionSchema{
				{Field: "status", Operator: "=", Param: "active"},
				{Field: "status", Operator: "=", Param: "pending"},
			},
		}

		cond, err := buildCondition(condSchema)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if cond == nil {
			t.Error("Expected condition to be created")
		}
	})

	t.Run("buildCondition with invalid logic operator", func(t *testing.T) {
		condSchema := &ConditionSchema{
			Logic: "XOR",
			Conditions: []ConditionSchema{
				{Field: "status", Operator: "=", Param: "active"},
			},
		}

		_, err := buildCondition(condSchema)
		if err == nil {
			t.Error("Expected error for invalid logic operator")
		}
	})

	t.Run("buildCondition with empty conditions", func(t *testing.T) {
		condSchema := &ConditionSchema{
			Logic:      "AND",
			Conditions: []ConditionSchema{},
		}

		_, err := buildCondition(condSchema)
		if err == nil {
			t.Error("Expected error for empty conditions")
		}
	})

	t.Run("buildCondition with NULL operators", func(t *testing.T) {
		// Test IS NULL
		nullCond := &ConditionSchema{
			Field:    "email",
			Operator: "IS NULL",
		}

		cond, err := buildCondition(nullCond)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if cond == nil {
			t.Error("Expected condition to be created")
		}

		// Test IS NOT NULL
		notNullCond := &ConditionSchema{
			Field:    "email",
			Operator: "IS NOT NULL",
		}

		cond2, err := buildCondition(notNullCond)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if cond2 == nil {
			t.Error("Expected condition to be created")
		}

		// Test NULL operator with param should error
		invalidNullCond := &ConditionSchema{
			Field:    "email",
			Operator: "IS NULL",
			Param:    "something",
		}

		_, err = buildCondition(invalidNullCond)
		if err == nil {
			t.Error("Expected error when NULL operator has param")
		}
	})

	t.Run("validateOperation", func(t *testing.T) {
		valid := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "COUNT"}
		invalid := []string{"select", "insert", "update", "delete", "count", "TRUNCATE", "DROP", "CREATE", "ALTER", "", "LISTEN"}

		for _, op := range valid {
			if err := validateOperation(op); err != nil {
				t.Errorf("Expected %q to be valid operation: %v", op, err)
			}
		}

		for _, op := range invalid {
			if err := validateOperation(op); err == nil {
				t.Errorf("Expected %q to be invalid operation", op)
			}
		}
	})
}
