package astql_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/dbml"
)

func createRenderTestInstance(t *testing.T) *astql.ASTQL {
	t.Helper()

	project := dbml.NewProject("test")

	users := dbml.NewTable("users")
	users.AddColumn(dbml.NewColumn("id", "bigint"))
	users.AddColumn(dbml.NewColumn("username", "varchar"))
	users.AddColumn(dbml.NewColumn("email", "varchar"))
	users.AddColumn(dbml.NewColumn("active", "boolean"))
	users.AddColumn(dbml.NewColumn("age", "int"))
	users.AddColumn(dbml.NewColumn("created_at", "timestamp"))
	project.AddTable(users)

	posts := dbml.NewTable("posts")
	posts.AddColumn(dbml.NewColumn("id", "bigint"))
	posts.AddColumn(dbml.NewColumn("user_id", "bigint"))
	posts.AddColumn(dbml.NewColumn("title", "varchar"))
	posts.AddColumn(dbml.NewColumn("content", "text"))
	posts.AddColumn(dbml.NewColumn("published", "boolean"))
	project.AddTable(posts)

	comments := dbml.NewTable("comments")
	comments.AddColumn(dbml.NewColumn("id", "bigint"))
	comments.AddColumn(dbml.NewColumn("post_id", "bigint"))
	comments.AddColumn(dbml.NewColumn("user_id", "bigint"))
	comments.AddColumn(dbml.NewColumn("body", "text"))
	project.AddTable(comments)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

// Test basic SELECT queries.
func TestRender_Select_AllFields(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 0 {
		t.Errorf("Expected 0 params, got %d", len(result.RequiredParams))
	}
}

func TestRender_Select_SpecificFields(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id"), instance.F("username"), instance.F("email")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id", "username", "email" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_WithAlias(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users", "u")).
		Fields(instance.F("id"), instance.F("username")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id", "username" FROM "users" u`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_WithWhere(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id"), instance.F("username")).
		Where(instance.C(instance.F("active"), "=", instance.P("is_active"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id", "username" FROM "users" WHERE "active" = :is_active`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "is_active" {
		t.Errorf("Expected params [is_active], got %v", result.RequiredParams)
	}
}

func TestRender_Select_MultipleWhere(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Where(instance.C(instance.F("active"), "=", instance.P("is_active"))).
		Where(instance.C(instance.F("age"), ">", instance.P("min_age"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Multiple Where() calls combine with AND
	expected := `SELECT "id" FROM "users" WHERE ("active" = :is_active AND "age" > :min_age)`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
}

func TestRender_Select_ComplexConditions(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Where(instance.And(
			instance.C(instance.F("active"), "=", instance.P("is_active")),
			instance.Or(
				instance.C(instance.F("age"), ">", instance.P("min_age")),
				instance.C(instance.F("age"), "<", instance.P("max_age")),
			),
		)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id" FROM "users" WHERE ("active" = :is_active AND ("age" > :min_age OR "age" < :max_age))`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got %d", len(result.RequiredParams))
	}
}

func TestRender_Select_OrderBy(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id"), instance.F("username")).
		OrderBy(instance.F("username"), "ASC").
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id", "username" FROM "users" ORDER BY "username" ASC`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_LimitOffset(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Limit(10).
		Offset(20).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id" FROM "users" LIMIT 10 OFFSET 20`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_Distinct(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username")).
		Distinct().
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT DISTINCT "username" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test JOIN queries.
func TestRender_Select_InnerJoin(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users", "u")).
		Fields(
			instance.WithTable(instance.F("id"), "u"),
			instance.WithTable(instance.F("username"), "u"),
			instance.WithTable(instance.F("title"), "p"),
		).
		InnerJoin(
			instance.T("posts", "p"),
			astql.CF(
				instance.WithTable(instance.F("id"), "u"),
				"=",
				instance.WithTable(instance.F("user_id"), "p"),
			),
		).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT u."id", u."username", p."title" FROM "users" u INNER JOIN "posts" p ON u."id" = p."user_id"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_LeftJoin(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users", "u")).
		Fields(instance.WithTable(instance.F("username"), "u")).
		LeftJoin(
			instance.T("posts", "p"),
			astql.CF(
				instance.WithTable(instance.F("id"), "u"),
				"=",
				instance.WithTable(instance.F("user_id"), "p"),
			),
		).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT u."username" FROM "users" u LEFT JOIN "posts" p ON u."id" = p."user_id"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_MultipleJoins(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users", "u")).
		Fields(
			instance.WithTable(instance.F("username"), "u"),
			instance.WithTable(instance.F("title"), "p"),
			instance.WithTable(instance.F("body"), "c"),
		).
		InnerJoin(
			instance.T("posts", "p"),
			astql.CF(
				instance.WithTable(instance.F("id"), "u"),
				"=",
				instance.WithTable(instance.F("user_id"), "p"),
			),
		).
		InnerJoin(
			instance.T("comments", "c"),
			astql.CF(
				instance.WithTable(instance.F("id"), "p"),
				"=",
				instance.WithTable(instance.F("post_id"), "c"),
			),
		).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT u."username", p."title", c."body" FROM "users" u INNER JOIN "posts" p ON u."id" = p."user_id" INNER JOIN "comments" c ON p."id" = c."post_id"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test INSERT queries.
func TestRender_Insert_Basic(t *testing.T) {
	instance := createRenderTestInstance(t)

	vm := instance.ValueMap()
	vm[instance.F("username")] = instance.P("username")
	vm[instance.F("email")] = instance.P("email")

	result, err := astql.Insert(instance.T("users")).
		Values(vm).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Fields are sorted alphabetically for deterministic output
	expected := `INSERT INTO "users" ("email", "username") VALUES (:email, :username)`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
}

func TestRender_Insert_WithReturning(t *testing.T) {
	instance := createRenderTestInstance(t)

	vm := instance.ValueMap()
	vm[instance.F("username")] = instance.P("username")

	result, err := astql.Insert(instance.T("users")).
		Values(vm).
		Returning(instance.F("id"), instance.F("created_at")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `INSERT INTO "users" ("username") VALUES (:username) RETURNING "id", "created_at"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Insert_OnConflictDoNothing(t *testing.T) {
	instance := createRenderTestInstance(t)

	vm := instance.ValueMap()
	vm[instance.F("username")] = instance.P("username")
	vm[instance.F("email")] = instance.P("email")

	result, err := astql.Insert(instance.T("users")).
		Values(vm).
		OnConflict(instance.F("email")).DoNothing().
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `INSERT INTO "users" ("email", "username") VALUES (:email, :username) ON CONFLICT ("email") DO NOTHING`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Insert_OnConflictDoUpdate(t *testing.T) {
	instance := createRenderTestInstance(t)

	vm := instance.ValueMap()
	vm[instance.F("username")] = instance.P("username")
	vm[instance.F("email")] = instance.P("email")

	result, err := astql.Insert(instance.T("users")).
		Values(vm).
		OnConflict(instance.F("email")).
		DoUpdate().
		Set(instance.F("username"), instance.P("new_username")).
		Build().
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `INSERT INTO "users" ("email", "username") VALUES (:email, :username) ON CONFLICT ("email") DO UPDATE SET "username" = :new_username`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test UPDATE queries.
func TestRender_Update_Basic(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Update(instance.T("users")).
		Set(instance.F("username"), instance.P("new_username")).
		Where(instance.C(instance.F("id"), "=", instance.P("user_id"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `UPDATE "users" SET "username" = :new_username WHERE "id" = :user_id`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
}

func TestRender_Update_MultipleFields(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Update(instance.T("users")).
		Set(instance.F("username"), instance.P("new_username")).
		Set(instance.F("email"), instance.P("new_email")).
		Set(instance.F("active"), instance.P("is_active")).
		Where(instance.C(instance.F("id"), "=", instance.P("user_id"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Fields are sorted alphabetically
	expected := `UPDATE "users" SET "active" = :is_active, "email" = :new_email, "username" = :new_username WHERE "id" = :user_id`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Update_WithReturning(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Update(instance.T("users")).
		Set(instance.F("active"), instance.P("is_active")).
		Where(instance.C(instance.F("id"), "=", instance.P("user_id"))).
		Returning(instance.F("id"), instance.F("active")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `UPDATE "users" SET "active" = :is_active WHERE "id" = :user_id RETURNING "id", "active"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test DELETE queries.
func TestRender_Delete_Basic(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Delete(instance.T("users")).
		Where(instance.C(instance.F("id"), "=", instance.P("user_id"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `DELETE FROM "users" WHERE "id" = :user_id`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
}

func TestRender_Delete_WithReturning(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Delete(instance.T("users")).
		Where(instance.C(instance.F("active"), "=", instance.P("is_active"))).
		Returning(instance.F("id"), instance.F("username")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `DELETE FROM "users" WHERE "active" = :is_active RETURNING "id", "username"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test COUNT queries.
func TestRender_Count_Basic(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Count(instance.T("users")).Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT COUNT(*) FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Count_WithWhere(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Count(instance.T("users")).
		Where(instance.C(instance.F("active"), "=", instance.P("is_active"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT COUNT(*) FROM "users" WHERE "active" = :is_active`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test aggregate functions.
func TestRender_Select_Aggregates(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.Sum(instance.F("age"))).
		SelectExpr(astql.Avg(instance.F("age"))).
		SelectExpr(astql.Min(instance.F("age"))).
		SelectExpr(astql.Max(instance.F("age"))).
		SelectExpr(astql.CountField(instance.F("id"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT SUM("age"), AVG("age"), MIN("age"), MAX("age"), COUNT("id") FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_AggregateWithAlias(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sum(instance.F("age")), "total_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT SUM("age") AS "total_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test GROUP BY and HAVING.
func TestRender_Select_GroupBy(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("active")).
		SelectExpr(astql.CountField(instance.F("id"))).
		GroupBy(instance.F("active")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "active", COUNT("id") FROM "users" GROUP BY "active"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_Select_GroupByHaving(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("active")).
		SelectExpr(astql.CountField(instance.F("id"))).
		GroupBy(instance.F("active")).
		Having(instance.C(instance.F("active"), "=", instance.P("is_active"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "active", COUNT("id") FROM "users" GROUP BY "active" HAVING "active" = :is_active`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test SQL injection prevention via schema validation.
func TestValidation_InvalidTable(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Try to use a table that doesn't exist in schema
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid table")
		}
	}()

	instance.T("malicious_table")
}

func TestValidation_InvalidField(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Try to use a field that doesn't exist in schema
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid field")
		}
	}()

	instance.F("malicious_field")
}

func TestValidation_SQLInjectionAttempt_Table(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Try SQL injection via table name - should fail at validation
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt")
		}
	}()

	instance.T("users; DROP TABLE users--")
}

func TestValidation_SQLInjectionAttempt_Field(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Try SQL injection via field name - should fail at validation
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt")
		}
	}()

	instance.F("id; DROP TABLE users--")
}

func TestValidation_SQLInjectionAttempt_Param(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Try SQL injection via parameter name - should fail at validation
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for SQL injection attempt")
		}
	}()

	instance.P("id'; DROP TABLE users--")
}

// Test special characters are properly quoted.
func TestRender_SpecialCharacters_TableName(t *testing.T) {
	project := dbml.NewProject("test")
	// Table name with reserved keyword
	selectTable := dbml.NewTable("select")
	selectTable.AddColumn(dbml.NewColumn("id", "bigint"))
	project.AddTable(selectTable)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	result, err := astql.Select(instance.T("select")).
		Fields(instance.F("id")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Reserved keyword should be quoted
	expected := `SELECT "id" FROM "select"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

func TestRender_SpecialCharacters_FieldName(t *testing.T) {
	project := dbml.NewProject("test")
	users := dbml.NewTable("users")
	// Field name with reserved keyword
	users.AddColumn(dbml.NewColumn("order", "varchar"))
	project.AddTable(users)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("order")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Reserved keyword should be quoted
	expected := `SELECT "order" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test parameter uniqueness.
func TestRender_DuplicateParameters(t *testing.T) {
	instance := createRenderTestInstance(t)

	// Use same parameter multiple times
	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Where(instance.And(
			instance.C(instance.F("age"), ">", instance.P("age_value")),
			instance.C(instance.F("age"), "<", instance.P("age_value")),
		)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Parameter should only appear once in RequiredParams list
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 unique param, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}

	// But should be used twice in SQL
	paramCount := strings.Count(result.SQL, ":age_value")
	if paramCount != 2 {
		t.Errorf("Expected parameter to be used 2 times in SQL, found %d", paramCount)
	}
}

// Test NULL conditions.
func TestRender_NullConditions(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Where(instance.Null(instance.F("email"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id" FROM "users" WHERE "email" IS NULL`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	// NULL condition shouldn't require parameters
	if len(result.RequiredParams) != 0 {
		t.Errorf("Expected 0 params for NULL condition, got %d", len(result.RequiredParams))
	}
}

func TestRender_NotNullConditions(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		Where(instance.NotNull(instance.F("email"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "id" FROM "users" WHERE "email" IS NOT NULL`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test complex real-world query.
func TestRender_ComplexQuery(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users", "u")).
		Fields(
			instance.WithTable(instance.F("username"), "u"),
			instance.WithTable(instance.F("email"), "u"),
		).
		SelectExpr(astql.As(astql.CountField(instance.WithTable(instance.F("id"), "p")), "post_count")).
		InnerJoin(
			instance.T("posts", "p"),
			astql.CF(
				instance.WithTable(instance.F("id"), "u"),
				"=",
				instance.WithTable(instance.F("user_id"), "p"),
			),
		).
		Where(instance.And(
			instance.C(instance.WithTable(instance.F("active"), "u"), "=", instance.P("is_active")),
			instance.C(instance.WithTable(instance.F("published"), "p"), "=", instance.P("is_published")),
		)).
		GroupBy(
			instance.WithTable(instance.F("id"), "u"),
			instance.WithTable(instance.F("username"), "u"),
			instance.WithTable(instance.F("email"), "u"),
		).
		OrderBy(instance.WithTable(instance.F("username"), "u"), "ASC").
		Limit(10).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT u."username", u."email", COUNT(p."id") AS "post_count" FROM "users" u INNER JOIN "posts" p ON u."id" = p."user_id" WHERE (u."active" = :is_active AND p."published" = :is_published) GROUP BY u."id", u."username", u."email" ORDER BY u."username" ASC LIMIT 10`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
}

// Test RIGHT JOIN.
func TestRender_Select_RightJoin(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username"), instance.F("title")).
		RightJoin(
			instance.T("posts"),
			astql.CF(instance.F("id"), "=", instance.F("user_id")),
		).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "username", "title" FROM "users" RIGHT JOIN "posts" ON "id" = "user_id"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test CROSS JOIN.
func TestRender_Select_CrossJoin(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username")).
		CrossJoin(instance.T("posts")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "username" FROM "users" CROSS JOIN "posts"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test COUNT(DISTINCT).
func TestRender_Select_CountDistinct(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.CountDistinct(instance.F("email"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT COUNT(DISTINCT "email") FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test CASE expression.
func TestRender_Select_CaseExpression(t *testing.T) {
	instance := createRenderTestInstance(t)

	caseExpr := astql.Case().
		When(instance.C(instance.F("age"), "<", instance.P("young_age")), instance.P("young")).
		When(instance.C(instance.F("age"), ">=", instance.P("old_age")), instance.P("old")).
		Else(instance.P("middle")).
		As("age_group").
		Build()

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username")).
		SelectExpr(caseExpr).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "username", CASE WHEN "age" < :young_age THEN :young WHEN "age" >= :old_age THEN :old ELSE :middle END AS "age_group" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	// Should have 5 parameters (young_age, young, old_age, old, middle)
	if len(result.RequiredParams) != 5 {
		t.Errorf("Expected 5 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

// Test CASE expression without ELSE.
func TestRender_Select_CaseExpression_NoElse(t *testing.T) {
	instance := createRenderTestInstance(t)

	caseExpr := astql.Case().
		When(instance.C(instance.F("active"), "=", instance.P("is_active")), instance.P("status_active")).
		Build()

	result, err := astql.Select(instance.T("users")).
		SelectExpr(caseExpr).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT CASE WHEN "active" = :is_active THEN :status_active END FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test COALESCE expression.
func TestRender_Select_Coalesce(t *testing.T) {
	instance := createRenderTestInstance(t)

	coalesceExpr := astql.As(
		astql.Coalesce(instance.P("email"), instance.P("username"), instance.P("default_value")),
		"contact",
	)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(coalesceExpr).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT COALESCE(:email, :username, :default_value) AS "contact" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got %d", len(result.RequiredParams))
	}
}

// Test NULLIF expression.
func TestRender_Select_NullIf(t *testing.T) {
	instance := createRenderTestInstance(t)

	nullifExpr := astql.As(
		astql.NullIf(instance.P("value"), instance.P("null_value")),
		"safe_value",
	)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(nullifExpr).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT NULLIF(:value, :null_value) AS "safe_value" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
}

// Test ROUND math function.
func TestRender_Select_Round(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Round(instance.F("age"), instance.P("precision")), "rounded_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT ROUND("age", :precision) AS "rounded_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
}

// Test ROUND without precision.
func TestRender_Select_Round_NoPrecision(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.Round(instance.F("age"))).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT ROUND("age") FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test FLOOR math function.
func TestRender_Select_Floor(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Floor(instance.F("age")), "floor_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT FLOOR("age") AS "floor_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test CEIL math function.
func TestRender_Select_Ceil(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Ceil(instance.F("age")), "ceil_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT CEIL("age") AS "ceil_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test ABS math function.
func TestRender_Select_Abs(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Abs(instance.F("age")), "abs_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT ABS("age") AS "abs_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test POWER math function.
func TestRender_Select_Power(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Power(instance.F("age"), instance.P("exponent")), "power_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT POWER("age", :exponent) AS "power_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
}

// Test SQRT math function.
func TestRender_Select_Sqrt(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		SelectExpr(astql.As(astql.Sqrt(instance.F("age")), "sqrt_age")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT SQRT("age") AS "sqrt_age" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test combining multiple math expressions.
func TestRender_Select_MultipleMathExpressions(t *testing.T) {
	instance := createRenderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username")).
		SelectExpr(astql.As(astql.Round(instance.F("age")), "rounded")).
		SelectExpr(astql.As(astql.Floor(instance.F("age")), "floored")).
		SelectExpr(astql.As(astql.Ceil(instance.F("age")), "ceiled")).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "username", ROUND("age") AS "rounded", FLOOR("age") AS "floored", CEIL("age") AS "ceiled" FROM "users"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}
