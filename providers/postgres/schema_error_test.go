package postgres_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
)

func TestSchemaErrorHandling(t *testing.T) {
	astql.SetupTestModels()

	t.Run("invalid table name", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "invalid_table", // Not registered
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for invalid table")
		}
		if !strings.Contains(err.Error(), "invalid_table") {
			t.Errorf("error should mention invalid table name: %v", err)
		}
	})

	t.Run("invalid field in SELECT", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Fields:    []string{"id", "invalid_field"}, // invalid_field not registered
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for invalid field")
		}
		if !strings.Contains(err.Error(), "invalid_field") {
			t.Errorf("error should mention invalid field name: %v", err)
		}
	})

	t.Run("invalid field in WHERE", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Where: &postgres.ConditionSchema{
				Field:    "invalid_field",
				Operator: "=",
				Param:    "value",
			},
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for invalid field in WHERE")
		}
		if !strings.Contains(err.Error(), "invalid_field") {
			t.Errorf("error should mention invalid field name: %v", err)
		}
	})

	t.Run("invalid field in UPDATE", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "UPDATE",
			Table:     "test_users",
			Updates: map[string]string{
				"name":          "new_name",
				"invalid_field": "value",
			},
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for invalid field in UPDATE")
		}
		if !strings.Contains(err.Error(), "invalid_field") {
			t.Errorf("error should mention invalid field name: %v", err)
		}
	})

	t.Run("AI hallucination scenario", func(t *testing.T) {
		// Simulating an AI that generates a schema with typos/hallucinations
		//nolint:misspell // Intentional misspellings to test error handling
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_userz",                    // Typo: should be test_users
			Fields:    []string{"id", "nmae", "emial"}, // Typos in field names
			Where: &postgres.ConditionSchema{
				Field:    "actve", // Typo: should be active
				Operator: "=",
				Param:    "is_active",
			},
		}

		_, err := postgres.BuildFromSchema(schema)
		if err == nil {
			t.Fatal("expected error for AI hallucination")
		}
		// Should fail on the first invalid identifier (table name)
		if !strings.Contains(err.Error(), "test_userz") {
			t.Errorf("error should mention invalid table: %v", err)
		}
	})

	t.Run("valid schema should succeed", func(t *testing.T) {
		schema := &postgres.QuerySchema{
			Operation: "SELECT",
			Table:     "test_users",
			Fields:    []string{"id", "name", "email"},
			Where: &postgres.ConditionSchema{
				Field:    "active",
				Operator: "=",
				Param:    "is_active",
			},
		}

		ast, err := postgres.BuildFromSchema(schema)
		if err != nil {
			t.Fatalf("valid schema should not error: %v", err)
		}
		if ast == nil {
			t.Fatal("expected non-nil AST")
		}
	})
}
