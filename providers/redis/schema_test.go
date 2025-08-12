package redis_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/redis"
	"gopkg.in/yaml.v3"
)

func TestRedisSchemaQueries(t *testing.T) {
	astql.SetupTestModels()
	provider := redis.NewProvider()

	// Register table configurations
	provider.RegisterTable("test_users", redis.TableConfig{
		KeyPattern: "users:{id}",
		DataType:   redis.TypeHash,
		IDField:    "id",
	})

	provider.RegisterTable("test_sessions", redis.TableConfig{
		KeyPattern: "session:{id}",
		DataType:   redis.TypeString,
		IDField:    "id",
	})

	t.Run("SELECT from Hash using schema", func(t *testing.T) {
		yamlSchema := `
operation: SELECT
table: test_users
fields:
  - name
  - email
where:
  field: id
  operator: "="
  param: user_id
`
		var schema redis.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := redis.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "HMGET users::user_id name email"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("INSERT with TTL using schema", func(t *testing.T) {
		yamlSchema := `
operation: INSERT
table: test_sessions
values:
  - id: session_id
    data: session_data
ttl_param: ttl_seconds
`
		var schema redis.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := redis.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Should use SETEX for TTL
		if !contains(result.SQL, "SETEX session::session_id") {
			t.Errorf("Expected SETEX command, got: %s", result.SQL)
		}

		// Should include TTL parameter in required params
		hasParam := false
		for _, param := range result.RequiredParams {
			if param == "ttl_seconds" {
				hasParam = true
				break
			}
		}
		if !hasParam {
			t.Errorf("Expected ttl_seconds in required params, got: %v", result.RequiredParams)
		}
	})

	t.Run("NOTIFY using schema", func(t *testing.T) {
		yamlSchema := `
operation: NOTIFY
table: test_users
notify_payload: user_changed
`
		var schema redis.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := redis.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "PUBLISH test_users_changes :user_changed"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("Invalid field in schema returns error", func(t *testing.T) {
		yamlSchema := `
operation: SELECT
table: test_users
fields:
  - name
  - invalid_field
where:
  field: id
  operator: "="
  param: user_id
`
		var schema redis.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		_, err := redis.BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for invalid field")
		}
		if !contains(err.Error(), "invalid_field") {
			t.Errorf("Error should mention invalid field: %v", err)
		}
	})

	t.Run("AI-friendly error messages", func(t *testing.T) {
		// Test unsupported operation
		schema := &redis.QuerySchema{
			Operation: "JOIN", // Redis doesn't support JOINs
			Table:     "test_users",
		}

		_, err := redis.BuildFromSchema(schema)
		if err == nil {
			t.Error("Expected error for unsupported operation")
		}
		if !contains(err.Error(), "unsupported operation") {
			t.Errorf("Error should mention unsupported operation: %v", err)
		}
	})
}
