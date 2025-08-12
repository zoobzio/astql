package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
	"gopkg.in/yaml.v3"
)

// TestAIGeneratedSchemas demonstrates how an AI would generate query schemas
// without ever touching actual values, maintaining complete SQL injection safety.
func TestAIGeneratedSchemas(t *testing.T) {
	astql.SetupTestModels()
	provider := postgres.NewProvider()

	t.Run("AI generates user search query", func(t *testing.T) {
		// AI understands: "Find active users created after a certain date"
		// AI generates this YAML schema:
		aiGeneratedYAML := `
operation: SELECT
table: test_users
fields:
  - id
  - name
  - email
where:
  logic: AND
  conditions:
    - field: active
      operator: "="
      param: is_active
    - field: created_at
      operator: ">"
      param: after_date
order_by:
  - field: created_at
    direction: DESC
limit: 50
`
		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(aiGeneratedYAML), &schema); err != nil {
			t.Fatalf("failed to parse AI-generated YAML: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("failed to build from AI schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		// The AI never sees or handles actual values - only parameter names
		expected := "SELECT id, name, email FROM test_users WHERE (active = :is_active AND created_at > :after_date) ORDER BY created_at DESC LIMIT 50"
		if result.SQL != expected {
			t.Errorf("SQL mismatch:\nExpected: %s\nGot:      %s", expected, result.SQL)
		}

		// Parameters are tracked for the execution layer
		if len(result.RequiredParams) != 2 {
			t.Errorf("expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("AI generates complex update with RETURNING", func(t *testing.T) {
		// AI understands: "Update user name and active status, get back the modified record"
		aiGeneratedYAML := `
operation: UPDATE
table: test_users
updates:
  name: new_name
  active: is_active
  updated_at: current_time
where:
  field: id
  operator: "="
  param: user_id
returning:
  - id
  - name
  - active
  - updated_at
`
		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(aiGeneratedYAML), &schema); err != nil {
			t.Fatalf("failed to parse: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		// Note: Parameters are named, not values
		if len(result.RequiredParams) != 4 { // new_name, is_active, current_time, user_id
			t.Errorf("expected 4 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("AI hallucination is caught safely", func(t *testing.T) {
		// AI makes a mistake - uses a field that doesn't exist
		aiGeneratedYAML := `
operation: SELECT
table: test_users
fields:
  - id
  - username  # AI hallucinated - field is actually 'name'
  - email
where:
  field: is_verified  # Another hallucination
  operator: "="
  param: verified
`
		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(aiGeneratedYAML), &schema); err != nil {
			t.Fatalf("failed to parse: %v", err)
		}

		// This returns an error instead of panicking
		_, err := postgres.BuildFromSchema(&schema)
		if err == nil {
			t.Fatal("expected error for invalid field")
		}

		// Application can handle this gracefully
		t.Logf("AI error caught safely: %v", err)
	})

	t.Run("AI generates real-time notification query", func(t *testing.T) {
		// AI understands: "Send a notification when order status changes"
		aiGeneratedYAML := `
operation: NOTIFY
table: test_orders
notify_payload: order_status_change
`
		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(aiGeneratedYAML), &schema); err != nil {
			t.Fatalf("failed to parse: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "NOTIFY test_orders_changes, :order_status_change"
		if result.SQL != expected {
			t.Errorf("expected %q, got %q", expected, result.SQL)
		}
	})
}
