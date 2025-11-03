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

func createSubqueryTestInstance(t *testing.T) *astql.ASTQL {
	t.Helper()

	project := dbml.NewProject("test")

	users := dbml.NewTable("users")
	users.AddColumn(dbml.NewColumn("id", "bigint"))
	users.AddColumn(dbml.NewColumn("username", "varchar"))
	users.AddColumn(dbml.NewColumn("email", "varchar"))
	users.AddColumn(dbml.NewColumn("active", "boolean"))
	project.AddTable(users)

	posts := dbml.NewTable("posts")
	posts.AddColumn(dbml.NewColumn("id", "bigint"))
	posts.AddColumn(dbml.NewColumn("user_id", "bigint"))
	posts.AddColumn(dbml.NewColumn("title", "varchar"))
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

// Test basic IN subquery.
func TestSubquery_IN_Basic(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE id IN (SELECT user_id FROM posts WHERE published = :is_published)
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(instance.C(instance.F("published"), "=", instance.P("is_published"))),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "posts" WHERE "published" = :sq1_is_published)`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	// Should have the subquery parameter with prefix
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
	if !contains(result.RequiredParams, "sq1_is_published") {
		t.Errorf("Expected param 'sq1_is_published', got %v", result.RequiredParams)
	}
}

// Test NOT IN subquery.
func TestSubquery_NOT_IN(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM posts)
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.NotIn, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users" WHERE "id" NOT IN (SELECT "user_id" FROM "posts")`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test EXISTS subquery.
func TestSubquery_EXISTS(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)
	subquery := astql.Sub(
		astql.Select(instance.T("posts", "p")).
			Fields(instance.WithTable(instance.F("user_id"), "p")).
			Where(instance.C(
				instance.WithTable(instance.F("user_id"), "p"),
				"=",
				instance.P("user_id"),
			)),
	)

	result, err := astql.Select(instance.T("users", "u")).
		Where(astql.CSubExists(astql.EXISTS, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users" u WHERE EXISTS (SELECT p."user_id" FROM "posts" p WHERE p."user_id" = :sq1_user_id)`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test NOT EXISTS subquery.
func TestSubquery_NOT_EXISTS(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSubExists(astql.NotExists, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users" WHERE NOT EXISTS (SELECT "user_id" FROM "posts")`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}

// Test nested subqueries (depth 2).
func TestSubquery_Nested_Depth2(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE id IN (
	//   SELECT user_id FROM posts WHERE id IN (
	//     SELECT post_id FROM comments WHERE body = :search_term
	//   )
	// )
	innerSubquery := astql.Sub(
		astql.Select(instance.T("comments")).
			Fields(instance.F("post_id")).
			Where(instance.C(instance.F("body"), "=", instance.P("search_term"))),
	)

	middleSubquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(astql.CSub(instance.F("id"), astql.IN, innerSubquery)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, middleSubquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Parameters should be namespaced by depth: sq1_, sq2_
	expected := `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "posts" WHERE "id" IN (SELECT "post_id" FROM "comments" WHERE "body" = :sq2_search_term))`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	// Should have parameter with sq2_ prefix (depth 2)
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
	if !contains(result.RequiredParams, "sq2_search_term") {
		t.Errorf("Expected param 'sq2_search_term', got %v", result.RequiredParams)
	}
}

// Test nested subqueries (depth 3 - max depth).
func TestSubquery_Nested_MaxDepth(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// Build a query with max depth (3 levels of nesting)
	level3 := astql.Sub(
		astql.Select(instance.T("comments")).
			Fields(instance.F("post_id")).
			Where(instance.C(instance.F("body"), "=", instance.P("term"))),
	)

	level2 := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(astql.CSub(instance.F("id"), astql.IN, level3)),
	)

	level1 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), astql.IN, level2)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, level1)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Should succeed at max depth
	if !strings.Contains(result.SQL, "sq3_term") {
		t.Errorf("Expected parameter with sq3_ prefix at depth 3, got: %s", result.SQL)
	}
}

// Test subquery depth limit exceeded.
func TestSubquery_DepthLimitExceeded(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// Try to build a query that exceeds max depth (4 levels)
	level4 := astql.Sub(
		astql.Select(instance.T("comments")).
			Fields(instance.F("post_id")),
	)

	level3 := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(astql.CSub(instance.F("id"), astql.IN, level4)),
	)

	level2 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), astql.IN, level3)),
	)

	level1 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), astql.IN, level2)),
	)

	_, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, level1)).
		Render()

	// Should fail with depth exceeded error
	if err == nil {
		t.Fatal("Expected error for exceeding max subquery depth")
	}
	if !strings.Contains(err.Error(), "maximum subquery depth") {
		t.Errorf("Expected 'maximum subquery depth' error, got: %v", err)
	}
}

// Test subquery with multiple parameters.
func TestSubquery_MultipleParameters(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE id IN (
	//   SELECT user_id FROM posts WHERE published = :is_published AND title LIKE :title_pattern
	// )
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(instance.And(
				instance.C(instance.F("published"), "=", instance.P("is_published")),
				instance.C(instance.F("title"), "LIKE", instance.P("title_pattern")),
			)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// Should have both parameters with sq1_ prefix
	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
	if !contains(result.RequiredParams, "sq1_is_published") {
		t.Errorf("Missing param 'sq1_is_published': %v", result.RequiredParams)
	}
	if !contains(result.RequiredParams, "sq1_title_pattern") {
		t.Errorf("Missing param 'sq1_title_pattern': %v", result.RequiredParams)
	}
}

// Test mixing outer query params with subquery params.
func TestSubquery_MixedParameters(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users
	// WHERE active = :is_active
	// AND id IN (SELECT user_id FROM posts WHERE published = :is_published)
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(instance.C(instance.F("published"), "=", instance.P("is_published"))),
	)

	result, err := astql.Select(instance.T("users")).
		Where(instance.And(
			instance.C(instance.F("active"), "=", instance.P("is_active")),
			astql.CSub(instance.F("id"), astql.IN, subquery),
		)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT * FROM "users" WHERE ("active" = :is_active AND "id" IN (SELECT "user_id" FROM "posts" WHERE "published" = :sq1_is_published))`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}

	// Should have both outer and subquery params
	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d", len(result.RequiredParams))
	}
	if !contains(result.RequiredParams, "is_active") {
		t.Errorf("Missing outer param 'is_active': %v", result.RequiredParams)
	}
	if !contains(result.RequiredParams, "sq1_is_published") {
		t.Errorf("Missing subquery param 'sq1_is_published': %v", result.RequiredParams)
	}
}

// Test CSub with wrong operator (should panic).
func TestSubquery_CSub_WrongOperator(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for using = operator with CSub")
		}
	}()

	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")),
	)

	// Should panic - CSub only accepts IN/NOT IN
	astql.CSub(instance.F("id"), astql.EQ, subquery)
}

// Test CSubExists with wrong operator (should panic).
func TestSubquery_CSubExists_WrongOperator(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for using IN operator with CSubExists")
		}
	}()

	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")),
	)

	// Should panic - CSubExists only accepts EXISTS/NOT EXISTS
	astql.CSubExists(astql.IN, subquery)
}

// Test Sub with invalid builder (should panic).
func TestSubquery_Sub_InvalidBuilder(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for building subquery with invalid AST")
		}
	}()

	// Create an invalid builder (Fields on INSERT)
	invalidBuilder := astql.Insert(instance.T("users")).
		Fields(instance.F("id"))

	// Should panic when trying to build
	astql.Sub(invalidBuilder)
}

// Test subquery with complex conditions.
func TestSubquery_ComplexConditions(t *testing.T) {
	instance := createSubqueryTestInstance(t)

	// SELECT * FROM users WHERE id IN (
	//   SELECT user_id FROM posts
	//   WHERE (published = :is_published OR title LIKE :title_pattern)
	//   AND user_id != :excluded_user
	// )
	subquery := astql.Sub(
		astql.Select(instance.T("posts")).
			Fields(instance.F("user_id")).
			Where(instance.And(
				instance.Or(
					instance.C(instance.F("published"), "=", instance.P("is_published")),
					instance.C(instance.F("title"), "LIKE", instance.P("title_pattern")),
				),
				instance.C(instance.F("user_id"), "!=", instance.P("excluded_user")),
			)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), astql.IN, subquery)).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	// All subquery parameters should have sq1_ prefix
	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got %d", len(result.RequiredParams))
	}
	for _, param := range result.RequiredParams {
		if !strings.HasPrefix(param, "sq1_") {
			t.Errorf("Expected all params to have sq1_ prefix, got: %s", param)
		}
	}
}

// Helper function to check if slice contains string.
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
