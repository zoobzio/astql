package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/zoobzio/sentinel"
	"gopkg.in/yaml.v3"
)

func ExampleBuildFromSchema_yaml() {
	// Example YAML content that an LLM might generate
	yamlContent := `
operation: SELECT
table: users
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

	// Parse YAML
	var schema QuerySchema
	if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
		panic(err)
	}

	// Set up Sentinel (would normally be done once at startup)
	initSentinel()
	ctx := context.Background()
	sentinel.Inspect[User](ctx)

	// Build AST from schema
	ast, err := BuildFromSchema(&schema)
	if err != nil {
		panic(err)
	}

	// Render to SQL (would normally use a provider)
	fmt.Printf("Operation: %s\n", ast.Operation)
	fmt.Printf("Table: %s\n", ast.Target.Name)
	fmt.Printf("Fields: %d\n", len(ast.Fields))
	fmt.Printf("Has WHERE: %v\n", ast.WhereClause != nil)
	fmt.Printf("Has ORDER BY: %v\n", len(ast.Ordering) > 0)
	fmt.Printf("Limit: %v\n", ast.Limit != nil && *ast.Limit == 10)

	// Output:
	// Operation: SELECT
	// Table: users
	// Fields: 3
	// Has WHERE: true
	// Has ORDER BY: true
	// Limit: true
}

func ExampleBuildFromSchema_json() {
	// Example JSON content that an LLM might generate
	jsonContent := `{
		"operation": "INSERT",
		"table": "users",
		"values": [{
			"name": "userName",
			"email": "userEmail"
		}],
		"returning": ["id"]
	}`

	// Parse JSON
	var schema QuerySchema
	if err := json.Unmarshal([]byte(jsonContent), &schema); err != nil {
		panic(err)
	}

	// Set up Sentinel (would normally be done once at startup)
	initSentinel()
	ctx := context.Background()
	sentinel.Inspect[User](ctx)

	// Build AST from schema
	ast, err := BuildFromSchema(&schema)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Operation: %s\n", ast.Operation)
	fmt.Printf("Values: %d row(s)\n", len(ast.Values))
	fmt.Printf("Returning: %d field(s)\n", len(ast.Returning))

	// Output:
	// Operation: INSERT
	// Values: 1 row(s)
	// Returning: 1 field(s)
}

func TestSchemaBuilderSecurity(t *testing.T) {
	SetupTest(t)

	// Test 1: SQL injection in parameter name
	maliciousSchema := &QuerySchema{
		Operation: "SELECT",
		Table:     "users",
		Where: &ConditionSchema{
			Field:    "id",
			Operator: "=",
			Param:    "1; DROP TABLE users--", // SQL injection attempt
		},
	}

	_, err := BuildFromSchema(maliciousSchema)
	if err == nil {
		t.Error("Expected error for SQL injection in parameter")
	}

	// Test 2: Unregistered table injection
	maliciousSchema2 := &QuerySchema{
		Operation: "SELECT",
		Table:     "users; DROP TABLE users--",
	}

	_, err = BuildFromSchema(maliciousSchema2)
	if err == nil {
		t.Error("Expected error for unregistered table")
	}

	// Test 3: Invalid operator
	maliciousSchema3 := &QuerySchema{
		Operation: "SELECT",
		Table:     "users",
		Where: &ConditionSchema{
			Field:    "id",
			Operator: "~", // PostgreSQL regex operator, not allowed
			Param:    "pattern",
		},
	}

	_, err = BuildFromSchema(maliciousSchema3)
	if err == nil {
		t.Error("Expected error for invalid operator")
	}
}
