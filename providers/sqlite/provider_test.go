package sqlite

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/sentinel"
)

// Test structs.
type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

type Order struct {
	ID     int    `db:"id"`
	UserID int    `db:"user_id"`
	Total  int    `db:"total"`
	Status string `db:"status"`
}

func setupTest(t *testing.T) {
	t.Helper()

	// Register test structs with sentinel
	sentinel.Inspect[User]()
	sentinel.Inspect[Order]()
}

func TestBasicSelect(t *testing.T) {
	setupTest(t)

	query := Select(astql.T("User")).
		Fields(astql.F("id"), astql.F("name")).
		Where(astql.C(astql.F("age"), types.GT, astql.P("minAge"))).
		OrderBy(astql.F("name"), types.ASC).
		Limit(10)

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expectedSQL := `SELECT "id", "name" FROM "User" WHERE "age" > ? ORDER BY "name" ASC LIMIT 10`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, result.SQL)
	}

	// Should have one parameter
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 parameter, got %d", len(result.RequiredParams))
	}
	if result.RequiredParams[0] != "minAge" {
		t.Errorf("Expected parameter 'minAge', got '%s'", result.RequiredParams[0])
	}
}

func TestInsertWithConflict(t *testing.T) {
	setupTest(t)

	t.Run("INSERT OR IGNORE", func(t *testing.T) {
		query := Insert(astql.T("User")).
			Values(map[types.Field]types.Param{
				astql.F("name"):  astql.P("userName"),
				astql.F("email"): astql.P("userEmail"),
			}).
			OnConflict().DoNothing()

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "INSERT OR IGNORE") {
			t.Errorf("Expected 'INSERT OR IGNORE' in SQL: %s", result.SQL)
		}
	})

	t.Run("ON CONFLICT DO NOTHING", func(t *testing.T) {
		query := Insert(astql.T("User")).
			Values(map[types.Field]types.Param{
				astql.F("name"):  astql.P("userName"),
				astql.F("email"): astql.P("userEmail"),
			}).
			OnConflict(astql.F("email")).DoNothing()

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("Expected 'ON CONFLICT' in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO NOTHING") {
			t.Errorf("Expected 'DO NOTHING' in SQL: %s", result.SQL)
		}
	})

	t.Run("ON CONFLICT DO UPDATE", func(t *testing.T) {
		query := Insert(astql.T("User")).
			Values(map[types.Field]types.Param{
				astql.F("name"):  astql.P("userName"),
				astql.F("email"): astql.P("userEmail"),
			}).
			OnConflict(astql.F("email")).
			DoUpdate().
			Set(astql.F("name"), astql.P("newName")).
			Build()

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DO UPDATE SET") {
			t.Errorf("Expected 'DO UPDATE SET' in SQL: %s", result.SQL)
		}
	})
}

func TestGroupByHaving(t *testing.T) {
	setupTest(t)

	query := Select(astql.T("Order")).
		Fields(
			astql.F("user_id"),
			astql.F("status"),
		).
		GroupBy(astql.F("user_id"), astql.F("status")).
		Having(astql.C(astql.F("user_id"), types.GT, astql.P("minCount"))).
		OrderBy(astql.F("user_id"), types.DESC)

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expectedParts := []string{
		"GROUP BY",
		"HAVING",
		"ORDER BY",
	}

	for _, part := range expectedParts {
		if !strings.Contains(result.SQL, part) {
			t.Errorf("Expected '%s' in SQL: %s", part, result.SQL)
		}
	}
}

func TestUpdate(t *testing.T) {
	setupTest(t)

	query := Update(astql.T("User")).
		Set(astql.F("name"), astql.P("newName")).
		Set(astql.F("email"), astql.P("newEmail")).
		Where(astql.C(astql.F("id"), types.EQ, astql.P("userId")))

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expectedSQL := `UPDATE "User" SET`
	if !strings.HasPrefix(result.SQL, expectedSQL) {
		t.Errorf("Expected SQL to start with:\n%s\nGot:\n%s", expectedSQL, result.SQL)
	}

	// Check all parameters are present (order may vary)
	expectedParams := []string{"newName", "newEmail", "userId"}
	if len(result.RequiredParams) != len(expectedParams) {
		t.Errorf("Expected %d parameters, got %d", len(expectedParams), len(result.RequiredParams))
	}
}

func TestDelete(t *testing.T) {
	setupTest(t)

	query := Delete(astql.T("User")).
		Where(astql.C(astql.F("age"), types.LT, astql.P("maxAge")))

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expectedSQL := `DELETE FROM "User" WHERE "age" < ?`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, result.SQL)
	}
}

func TestJoins(t *testing.T) {
	setupTest(t)

	t.Run("INNER JOIN", func(t *testing.T) {
		query := Select(astql.T("User", "u")).
			Fields(astql.F("id").WithTable("u"), astql.F("user_id").WithTable("o")).
			InnerJoin(
				astql.T("Order", "o"),
				CF(astql.F("id").WithTable("u"), types.EQ, astql.F("user_id").WithTable("o")),
			).
			Where(astql.C(astql.F("status").WithTable("o"), types.EQ, astql.P("status")))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "INNER JOIN") {
			t.Errorf("Expected INNER JOIN in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ON") {
			t.Errorf("Expected ON clause in SQL: %s", result.SQL)
		}
	})

	t.Run("LEFT JOIN", func(t *testing.T) {
		query := Select(astql.T("User", "u")).
			Fields(astql.F("name").WithTable("u")).
			LeftJoin(
				astql.T("Order", "o"),
				CF(astql.F("id").WithTable("u"), types.EQ, astql.F("user_id").WithTable("o")),
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "LEFT JOIN") {
			t.Errorf("Expected LEFT JOIN in SQL: %s", result.SQL)
		}
	})

	t.Run("Multiple JOINs", func(t *testing.T) {
		// Register a third table for testing
		type Product struct {
			ID int `db:"id"`
		}
		sentinel.Inspect[Product]()

		query := Select(astql.T("User", "u")).
			InnerJoin(
				astql.T("Order", "o"),
				CF(astql.F("id").WithTable("u"), types.EQ, astql.F("user_id").WithTable("o")),
			).
			InnerJoin(
				astql.T("Product", "p"),
				CF(astql.F("id").WithTable("p"), types.EQ, astql.F("id").WithTable("o")),
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		// Should have two JOINs
		joinCount := strings.Count(result.SQL, "INNER JOIN")
		if joinCount != 2 {
			t.Errorf("Expected 2 INNER JOINs, found %d in SQL: %s", joinCount, result.SQL)
		}
	})
}

func TestDistinct(t *testing.T) {
	setupTest(t)

	query := Select(astql.T("User")).
		Distinct().
		Fields(astql.F("name"))

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expectedSQL := `SELECT DISTINCT "name" FROM "User"`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, result.SQL)
	}
}

func TestPositionalParameters(t *testing.T) {
	setupTest(t)

	// Test that SQLite uses ? for all parameters
	query := Select(astql.T("User")).
		Where(astql.And(
			astql.C(astql.F("age"), types.GE, astql.P("minAge")),
			astql.C(astql.F("age"), types.LE, astql.P("maxAge")),
			astql.C(astql.F("status"), types.EQ, astql.P("status")),
		))

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Count question marks
	questionMarks := strings.Count(result.SQL, "?")
	if questionMarks != 3 {
		t.Errorf("Expected 3 question marks, found %d in SQL: %s", questionMarks, result.SQL)
	}

	// Verify no named parameters
	if strings.Contains(result.SQL, ":") {
		t.Errorf("Found named parameter marker ':' in SQL (should use '?'): %s", result.SQL)
	}

	// Check parameter order is preserved
	expectedParams := []string{"minAge", "maxAge", "status"}
	if len(result.RequiredParams) != len(expectedParams) {
		t.Errorf("Expected %d parameters, got %d", len(expectedParams), len(result.RequiredParams))
	}
	for i, param := range expectedParams {
		if result.RequiredParams[i] != param {
			t.Errorf("Parameter %d: expected '%s', got '%s'", i, param, result.RequiredParams[i])
		}
	}
}
