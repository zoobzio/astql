package astql_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/dbml"
)

func createExpressionsTestInstance(t *testing.T) *astql.ASTQL {
	t.Helper()

	project := dbml.NewProject("test")
	users := dbml.NewTable("users")
	users.AddColumn(dbml.NewColumn("id", "bigint"))
	users.AddColumn(dbml.NewColumn("age", "int"))
	users.AddColumn(dbml.NewColumn("active", "boolean"))
	project.AddTable(users)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

// Test valid alias.
func TestAs_ValidAlias(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Alias should be quoted
	expected := `SELECT SUM("age") AS "total_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test alias with underscores.
func TestAs_AliasWithUnderscores(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Avg(instance.F("age")), "avg_user_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT AVG("age") AS "avg_user_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test SQL injection attempt in alias - semicolon.
func TestAs_SQLInjection_Semicolon(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt in alias")
		} else {
			errMsg := r.(error).Error()
			if !strings.Contains(errMsg, "invalid alias") {
				t.Errorf("Expected 'invalid alias' error, got: %v", errMsg)
			}
		}
	}()

	// Should panic before rendering
	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total; DROP TABLE users--"))
}

// Test SQL injection attempt in alias - OR clause.
func TestAs_SQLInjection_OrClause(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt in alias")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total or 1=1"))
}

// Test SQL injection attempt in alias - comment.
func TestAs_SQLInjection_Comment(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt in alias")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total--"))
}

// Test SQL injection attempt in alias - quote.
func TestAs_SQLInjection_Quote(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt in alias")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total'"))
}

// Test SQL injection attempt in alias - spaces.
func TestAs_SQLInjection_Spaces(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt with spaces")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total age"))
}

// Test invalid alias - starts with number.
func TestAs_InvalidAlias_StartsWithNumber(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for alias starting with number")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "1total"))
}

// Test invalid alias - empty string.
func TestAs_InvalidAlias_Empty(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for empty alias")
		}
	}()

	astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), ""))
}

// Test CASE expression with valid alias.
func TestCaseBuilder_As_Valid(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	caseExpr := astql.Case().
		When(instance.C(instance.F("age"), "<", instance.P("young")), instance.P("child")).
		Else(instance.P("adult")).
		As("age_group").
		Build()

	result, err := astql.Select(instance.T("users")).
		SelectExpr(caseExpr).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Alias should be quoted
	if !strings.Contains(result.SQL, `AS "age_group"`) {
		t.Errorf("Expected quoted alias in SQL: %s", result.SQL)
	}
}

// Test CASE expression with SQL injection attempt.
func TestCaseBuilder_As_SQLInjection(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection in CASE alias")
		}
	}()

	astql.Case().
		When(instance.C(instance.F("age"), "<", instance.P("young")), instance.P("child")).
		As("group; DROP TABLE users--")
}

// Test COALESCE with valid alias.
func TestCoalesce_As_Valid(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(
			astql.Coalesce(instance.P("val1"), instance.P("val2")),
			"safe_value",
		)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	if !strings.Contains(result.SQL, `AS "safe_value"`) {
		t.Errorf("Expected quoted alias in SQL: %s", result.SQL)
	}
}

// Test multiple expressions with aliases.
func TestAs_MultipleAliases(t *testing.T) {
	instance := createExpressionsTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total_age")).
		SelectExpr(astql.As(astql.Avg(instance.F("age")), "avg_age")).
		SelectExpr(astql.As(astql.Max(instance.F("age")), "max_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// All aliases should be quoted
	expected := `SELECT SUM("age") AS "total_age", AVG("age") AS "avg_age", MAX("age") AS "max_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}
