package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestPostgresProviderMetadata(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("SELECT with metadata", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(
				astql.F("id"),
				astql.F("name"),
				astql.F("email"),
			).
			Where(
				astql.And(
					astql.C(astql.F("active"), astql.EQ, astql.P("isActive")),
					astql.C(astql.F("age"), astql.GE, astql.P("minAge")),
				),
			).
			Limit(10).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Check SQL uses named parameters
		expected := "SELECT id, name, email FROM test_users WHERE (active = :isActive AND age >= :minAge) LIMIT 10"
		if result.SQL != expected {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}

		// Check metadata
		if result.Metadata.Operation != astql.OpSelect {
			t.Errorf("Expected SELECT operation, got %s", result.Metadata.Operation)
		}

		if result.Metadata.Table.Name != "test_users" {
			t.Errorf("Expected table 'test_users', got '%s'", result.Metadata.Table.Name)
		}

		if result.Metadata.Table.TypeName != "TestUser" {
			t.Errorf("Expected type name 'TestUser', got '%s'", result.Metadata.Table.TypeName)
		}

		if result.Metadata.ResultType != astql.ResultMultiple {
			t.Errorf("Expected ResultMultiple, got %s", result.Metadata.ResultType)
		}

		if len(result.Metadata.ReturnedFields) != 3 {
			t.Errorf("Expected 3 returned fields, got %d", len(result.Metadata.ReturnedFields))
		}

		// Check field metadata
		for _, field := range result.Metadata.ReturnedFields {
			t.Logf("Field: %s, Type: %s, Nullable: %v", field.Name, field.DataType, field.IsNullable)

			// Verify we got type information from Sentinel
			switch field.Name {
			case "id":
				if field.DataType != "int" {
					t.Errorf("Expected 'id' to be int, got %s", field.DataType)
				}
			case "name", "email":
				if field.DataType != "string" {
					t.Errorf("Expected '%s' to be string, got %s", field.Name, field.DataType)
				}
			}
		}
	})

	t.Run("SELECT * with metadata", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Should get all fields from Sentinel
		if len(result.Metadata.ReturnedFields) < 5 {
			t.Errorf("Expected at least 5 fields for SELECT *, got %d", len(result.Metadata.ReturnedFields))
		}

		// Check we have nullable field info
		hasNullable := false
		for _, field := range result.Metadata.ReturnedFields {
			if field.IsNullable {
				hasNullable = true
				t.Logf("Nullable field: %s", field.Name)
			}
		}
		if !hasNullable {
			t.Error("Expected at least one nullable field (manager_id, deleted_at)")
		}
	})

	t.Run("INSERT with RETURNING metadata", func(t *testing.T) {
		ast := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"):  astql.P("userName"),
				astql.F("email"): astql.P("userEmail"),
			}).
			Returning(astql.F("id"), astql.F("created_at")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Check metadata
		if result.Metadata.Operation != astql.OpInsert {
			t.Errorf("Expected INSERT operation, got %s", result.Metadata.Operation)
		}

		if result.Metadata.ResultType != astql.ResultSingle {
			t.Errorf("Expected ResultSingle for RETURNING, got %s", result.Metadata.ResultType)
		}

		// Check modified fields
		if len(result.Metadata.ModifiedFields) != 2 {
			t.Errorf("Expected 2 modified fields, got %d", len(result.Metadata.ModifiedFields))
		}

		// Check returned fields
		if len(result.Metadata.ReturnedFields) != 2 {
			t.Errorf("Expected 2 returned fields, got %d", len(result.Metadata.ReturnedFields))
		}

		for _, field := range result.Metadata.ReturnedFields {
			if field.Name == "id" && field.DataType != "int" {
				t.Errorf("Expected 'id' to be int, got %s", field.DataType)
			}
		}
	})

	t.Run("Aggregate query metadata", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("manager_id")).
			SelectExpr(postgres.CountField(astql.F("id")).As("user_count")).
			SelectExpr(postgres.Avg(astql.F("age")).As("avg_age")).
			GroupBy(astql.F("manager_id")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Check SQL
		expected := "SELECT manager_id, COUNT(id) AS user_count, AVG(age) AS avg_age FROM test_users GROUP BY manager_id"
		if result.SQL != expected {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
		}

		// Check aggregate fields have numeric type
		for _, field := range result.Metadata.ReturnedFields {
			if field.Name == "user_count" || field.Name == "avg_age" {
				if field.DataType != "numeric" {
					t.Errorf("Expected aggregate field '%s' to be numeric, got %s", field.Name, field.DataType)
				}
			}
		}
	})

	t.Run("Parameter deduplication", func(t *testing.T) {
		// Use same parameter multiple times
		ast := postgres.Select(astql.T("test_users")).
			Where(
				astql.Or(
					astql.C(astql.F("created_at"), astql.GE, astql.P("date")),
					astql.C(astql.F("updated_at"), astql.GE, astql.P("date")),
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Should only have one 'date' parameter
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "date" {
			t.Errorf("Expected single 'date' param, got %v", result.RequiredParams)
		}

		// SQL should use :date twice
		expected := "SELECT * FROM test_users WHERE (created_at >= :date OR updated_at >= :date)"
		if result.SQL != expected {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})
}
