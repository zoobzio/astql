package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
)

func TestListenNotifyUnlisten(t *testing.T) {
	astql.SetupTestModels()
	provider := postgres.NewProvider()

	// Test LISTEN
	t.Run("LISTEN query", func(t *testing.T) {
		table := astql.T("test_users")
		builder := postgres.Listen(table)
		pgAst, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build PostgreSQL LISTEN query: %v", err)
		}

		result, err := provider.Render(pgAst)
		if err != nil {
			t.Fatalf("Failed to render LISTEN query: %v", err)
		}

		expected := "LISTEN test_users_changes"
		if result.SQL != expected {
			t.Errorf("LISTEN query mismatch\nExpected: %s\nGot: %s", expected, result.SQL)
		}
	})

	// Test NOTIFY
	t.Run("NOTIFY query with payload", func(t *testing.T) {
		table := astql.T("test_users")
		payload := astql.Param{Type: astql.ParamNamed, Name: "payload"}

		builder := postgres.Notify(table, payload)
		pgAst, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build PostgreSQL NOTIFY query: %v", err)
		}

		result, err := provider.Render(pgAst)
		if err != nil {
			t.Fatalf("Failed to render NOTIFY query: %v", err)
		}

		expected := "NOTIFY test_users_changes, :payload"
		if result.SQL != expected {
			t.Errorf("NOTIFY query mismatch\nExpected: %s\nGot: %s", expected, result.SQL)
		}

		// Check that payload parameter is tracked
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "payload" {
			t.Errorf("Expected required param 'payload', got: %v", result.RequiredParams)
		}
	})

	// Test UNLISTEN
	t.Run("UNLISTEN query", func(t *testing.T) {
		table := astql.T("test_users")
		builder := postgres.Unlisten(table)
		pgAst, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build PostgreSQL UNLISTEN query: %v", err)
		}

		result, err := provider.Render(pgAst)
		if err != nil {
			t.Fatalf("Failed to render UNLISTEN query: %v", err)
		}

		expected := "UNLISTEN test_users_changes"
		if result.SQL != expected {
			t.Errorf("UNLISTEN query mismatch\nExpected: %s\nGot: %s", expected, result.SQL)
		}
	})
}
