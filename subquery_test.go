package astql_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/dbml"
)

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
		Where(astql.CSub(instance.F("id"), types.IN, subquery)).
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
		Where(astql.CSub(instance.F("id"), types.NotIn, subquery)).
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
		Where(astql.CSubExists(types.EXISTS, subquery)).
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
		Where(astql.CSubExists(types.NotExists, subquery)).
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
			Where(astql.CSub(instance.F("id"), types.IN, innerSubquery)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), types.IN, middleSubquery)).
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
			Where(astql.CSub(instance.F("id"), types.IN, level3)),
	)

	level1 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), types.IN, level2)),
	)

	result, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), types.IN, level1)).
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
			Where(astql.CSub(instance.F("id"), types.IN, level4)),
	)

	level2 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), types.IN, level3)),
	)

	level1 := astql.Sub(
		astql.Select(instance.T("users")).
			Fields(instance.F("id")).
			Where(astql.CSub(instance.F("id"), types.IN, level2)),
	)

	_, err := astql.Select(instance.T("users")).
		Where(astql.CSub(instance.F("id"), types.IN, level1)).
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
		Where(astql.CSub(instance.F("id"), types.IN, subquery)).
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
			astql.CSub(instance.F("id"), types.IN, subquery),
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
	astql.CSub(instance.F("id"), types.EQ, subquery)
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
	astql.CSubExists(types.IN, subquery)
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
		Where(astql.CSub(instance.F("id"), types.IN, subquery)).
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
