package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestPostgresProvider(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	provider := postgres.NewProvider()

	t.Run("SELECT query", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users", "u")).
			Fields(
				astql.F("id").WithTable("u"),
				astql.F("name").WithTable("u"),
			).
			Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
			OrderBy(astql.F("created_at").WithTable("u"), astql.DESC).
			Limit(10).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT u.id, u.name FROM test_users u WHERE u.active = :isActive ORDER BY u.created_at DESC LIMIT 10"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})

	t.Run("SELECT with JOIN using field comparison", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users", "u")).
			Fields(
				astql.F("id").WithTable("u"),
				astql.F("name").WithTable("u"),
				astql.F("name").WithTable("m"),
			).
			LeftJoin(
				astql.T("test_users", "m"),
				astql.CF(
					astql.F("manager_id").WithTable("u"),
					astql.EQ,
					astql.F("id").WithTable("m"),
				),
			).
			Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT u.id, u.name, m.name FROM test_users u LEFT JOIN test_users m ON u.manager_id = m.id WHERE u.active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})

	t.Run("SELECT with aggregates and GROUP BY", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Fields(astql.F("manager_id")).
			SelectExpr(
				postgres.CountField(astql.F("id")).As("user_count"),
			).
			SelectExpr(
				postgres.Avg(astql.F("age")).As("avg_age"),
			).
			GroupBy(astql.F("manager_id")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT manager_id, COUNT(id) AS user_count, AVG(age) AS avg_age FROM test_users GROUP BY manager_id"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params, got %v", result.RequiredParams)
		}
	})

	t.Run("INSERT with RETURNING", func(t *testing.T) {
		ast := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"):  astql.P("name"),
				astql.F("email"): astql.P("email"),
				astql.F("age"):   astql.P("age"),
			}).
			Returning(astql.F("id"), astql.F("created_at")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Check that SQL contains expected parts (map iteration order is not guaranteed)
		if !contains(result.SQL, "INSERT INTO test_users") ||
			!contains(result.SQL, ":name") ||
			!contains(result.SQL, ":email") ||
			!contains(result.SQL, ":age") ||
			!contains(result.SQL, "VALUES") ||
			!contains(result.SQL, "RETURNING id, created_at") {
			t.Errorf("SQL missing expected parts:\n%s", result.SQL)
		}

		if len(result.RequiredParams) != 3 {
			t.Errorf("Expected 3 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}

		// Check all expected params are present
		paramSet := make(map[string]bool)
		for _, p := range result.RequiredParams {
			paramSet[p] = true
		}

		for _, expected := range []string{"name", "email", "age"} {
			if !paramSet[expected] {
				t.Errorf("Missing expected param: %s", expected)
			}
		}
	})

	t.Run("INSERT with ON CONFLICT DO UPDATE", func(t *testing.T) {
		ast := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("email"): astql.P("email"),
				astql.F("name"):  astql.P("name"),
			}).
			OnConflict(astql.F("email")).
			DoUpdate(map[astql.Field]astql.Param{
				astql.F("name"):       astql.P("newName"),
				astql.F("updated_at"): astql.P("now"),
			}).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Due to map iteration, multiple valid outputs
		// First two params are for INSERT, next two are for UPDATE
		if len(result.RequiredParams) != 4 {
			t.Errorf("Expected 4 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}

		// Verify SQL contains expected parts
		if !contains(result.SQL, "INSERT INTO test_users") ||
			!contains(result.SQL, "VALUES") ||
			!contains(result.SQL, "ON CONFLICT (email) DO UPDATE SET") ||
			!contains(result.SQL, "name =") ||
			!contains(result.SQL, "updated_at =") {
			t.Errorf("SQL missing expected parts:\n%s", result.SQL)
		}
	})

	t.Run("UPDATE with RETURNING", func(t *testing.T) {
		ast := postgres.Update(astql.T("test_users")).
			Set(astql.F("name"), astql.P("newName")).
			Set(astql.F("updated_at"), astql.P("now")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			Returning(astql.F("id"), astql.F("updated_at")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		// Due to map iteration order
		if !contains(result.SQL, "UPDATE test_users SET") ||
			!contains(result.SQL, "WHERE id = :userId") ||
			!contains(result.SQL, ":newName") ||
			!contains(result.SQL, ":now") ||
			!contains(result.SQL, "RETURNING id, updated_at") {
			t.Errorf("SQL missing expected parts:\n%s", result.SQL)
		}

		if len(result.RequiredParams) != 3 {
			t.Errorf("Expected 3 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("DELETE with RETURNING", func(t *testing.T) {
		ast := postgres.Delete(astql.T("test_users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			Returning(astql.F("id"), astql.F("deleted_at")).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "DELETE FROM test_users WHERE id = :userId RETURNING id, deleted_at"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "userId" {
			t.Errorf("Expected params [userId], got %v", result.RequiredParams)
		}
	})

	t.Run("Complex query with AND/OR", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Where(
				astql.Or(
					astql.And(
						astql.C(astql.F("active"), astql.EQ, astql.P("isActive")),
						astql.C(astql.F("age"), astql.GE, astql.P("minAge")),
					),
					astql.C(astql.F("manager_id"), astql.IsNull, astql.P("dummy")),
				),
			).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT * FROM test_users WHERE ((active = :isActive AND age >= :minAge) OR manager_id IS NULL)"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
		}
	})

	t.Run("COUNT query", func(t *testing.T) {
		ast := postgres.Count(astql.T("test_users")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT COUNT(*) FROM test_users WHERE active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "isActive" {
			t.Errorf("Expected params [isActive], got %v", result.RequiredParams)
		}
	})

	t.Run("DISTINCT query", func(t *testing.T) {
		ast := postgres.Select(astql.T("test_users")).
			Distinct().
			Fields(astql.F("manager_id")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("isActive"))).
			MustBuild()

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatal(err)
		}

		expected := "SELECT DISTINCT manager_id FROM test_users WHERE active = :isActive"
		if result.SQL != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result.SQL)
		}
	})
}

// Helper function to check if string contains substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || s != "" && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
