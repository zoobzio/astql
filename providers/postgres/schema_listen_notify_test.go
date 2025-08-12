package postgres_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
	"gopkg.in/yaml.v3"
)

func TestSchemaListenNotifyUnlisten(t *testing.T) {
	astql.SetupTestModels()
	provider := postgres.NewProvider()

	t.Run("LISTEN from schema", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "LISTEN",
			Table:     "test_users",
		}

		ast, err := postgres.BuildFromSchema(schema)
		if err != nil {
			t.Fatalf("failed to build LISTEN from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "LISTEN test_users_changes"
		if result.SQL != expected {
			t.Errorf("expected SQL %q, got %q", expected, result.SQL)
		}
	})

	t.Run("NOTIFY from schema", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation:     "NOTIFY",
			Table:         "test_users",
			NotifyPayload: "notification_data",
		}

		ast, err := postgres.BuildFromSchema(schema)
		if err != nil {
			t.Fatalf("failed to build NOTIFY from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "NOTIFY test_users_changes, :notification_data"
		if result.SQL != expected {
			t.Errorf("expected SQL %q, got %q", expected, result.SQL)
		}

		// Check parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "notification_data" {
			t.Errorf("expected param 'notification_data', got: %v", result.RequiredParams)
		}
	})

	t.Run("NOTIFY without payload should error", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "NOTIFY",
			Table:     "test_users",
			// NotifyPayload is missing
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for NOTIFY without payload")
		}
		if err.Error() != "NOTIFY requires a payload parameter name" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("UNLISTEN from schema", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "UNLISTEN",
			Table:     "test_users",
		}

		ast, err := postgres.BuildFromSchema(schema)
		if err != nil {
			t.Fatalf("failed to build UNLISTEN from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "UNLISTEN test_users_changes"
		if result.SQL != expected {
			t.Errorf("expected SQL %q, got %q", expected, result.SQL)
		}
	})

	t.Run("YAML schema example", func(t *testing.T) {
		yamlSchema := `
operation: NOTIFY
table: test_users
notify_payload: user_change_event
`
		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("failed to unmarshal YAML: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("failed to build from YAML schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "NOTIFY test_users_changes, :user_change_event"
		if result.SQL != expected {
			t.Errorf("expected SQL %q, got %q", expected, result.SQL)
		}
	})

	t.Run("JSON schema example", func(t *testing.T) {
		jsonSchema := `{
			"operation": "LISTEN",
			"table": "test_orders"
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonSchema), &schema); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		ast, err := postgres.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("failed to build from JSON schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		expected := "LISTEN test_orders_changes"
		if result.SQL != expected {
			t.Errorf("expected SQL %q, got %q", expected, result.SQL)
		}
	})
}
