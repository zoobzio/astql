package astql_test

import (
	"fmt"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/dbml"
)

func createBuilderTestInstance(t *testing.T) *astql.ASTQL {
	t.Helper()

	project := dbml.NewProject("test")
	users := dbml.NewTable("users")
	users.AddColumn(dbml.NewColumn("id", "bigint"))
	users.AddColumn(dbml.NewColumn("username", "varchar"))
	users.AddColumn(dbml.NewColumn("email", "varchar"))
	users.AddColumn(dbml.NewColumn("age", "int"))
	project.AddTable(users)

	posts := dbml.NewTable("posts")
	posts.AddColumn(dbml.NewColumn("id", "bigint"))
	posts.AddColumn(dbml.NewColumn("user_id", "bigint"))
	posts.AddColumn(dbml.NewColumn("title", "varchar"))
	project.AddTable(posts)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

func TestSelect(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table)
	ast, err := builder.Build()

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Operation != types.OpSelect {
		t.Errorf("Expected SELECT operation, got %v", ast.Operation)
	}
	if ast.Target.Name != "users" {
		t.Errorf("Expected table 'users', got '%s'", ast.Target.Name)
	}
}

func TestInsert(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	values := map[types.Field]types.Param{
		instance.F("username"): instance.P("username"),
	}

	builder := astql.Insert(table).Values(values)
	ast, err := builder.Build()

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Operation != types.OpInsert {
		t.Errorf("Expected INSERT operation, got %v", ast.Operation)
	}
}

func TestUpdate(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Update(table)
	ast := builder.GetAST()

	if ast.Operation != types.OpUpdate {
		t.Errorf("Expected UPDATE operation, got %v", ast.Operation)
	}
	if ast.Updates == nil {
		t.Error("Expected Updates map to be initialized")
	}
}

func TestDelete(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Delete(table)
	ast, err := builder.Build()

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Operation != types.OpDelete {
		t.Errorf("Expected DELETE operation, got %v", ast.Operation)
	}
}

func TestCount(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Count(table)
	ast, err := builder.Build()

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Operation != types.OpCount {
		t.Errorf("Expected COUNT operation, got %v", ast.Operation)
	}
}

func TestFields(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).
		Fields(instance.F("id"), instance.F("username"))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(ast.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(ast.Fields))
	}
}

func TestFields_WrongOperation(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Insert(table).
		Fields(instance.F("id"))

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error when using Fields() with INSERT")
	}
}

func TestWhere(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	cond := instance.C(instance.F("id"), "=", instance.P("user_id"))
	builder := astql.Select(table).Where(cond)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.WhereClause == nil {
		t.Error("Expected WHERE clause to be set")
	}
}

func TestWhere_Multiple(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	cond1 := instance.C(instance.F("id"), "=", instance.P("user_id"))
	cond2 := instance.C(instance.F("username"), "=", instance.P("username"))

	builder := astql.Select(table).
		Where(cond1).
		Where(cond2)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Multiple Where() calls should combine with AND
	group, ok := ast.WhereClause.(types.ConditionGroup)
	if !ok {
		t.Fatal("Expected ConditionGroup for multiple WHERE clauses")
	}
	if group.Logic != types.AND {
		t.Errorf("Expected AND logic, got %v", group.Logic)
	}
	if len(group.Conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
	}
}

func TestWhereField(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).
		WhereField(instance.F("id"), "=", instance.P("user_id"))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.WhereClause == nil {
		t.Error("Expected WHERE clause to be set")
	}
}

func TestSet(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Update(table).
		Set(instance.F("username"), instance.P("new_username"))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(ast.Updates) != 1 {
		t.Errorf("Expected 1 update, got %d", len(ast.Updates))
	}
}

func TestSet_WrongOperation(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).
		Set(instance.F("username"), instance.P("new_username"))

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error when using Set() with SELECT")
	}
}

func TestValues(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	values := map[types.Field]types.Param{
		instance.F("username"): instance.P("username"),
		instance.F("email"):    instance.P("email"),
	}

	builder := astql.Insert(table).Values(values)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(ast.Values) != 1 {
		t.Errorf("Expected 1 value set, got %d", len(ast.Values))
	}
}

func TestValues_WrongOperation(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	values := map[types.Field]types.Param{
		instance.F("username"): instance.P("username"),
	}

	builder := astql.Select(table).Values(values)

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error when using Values() with SELECT")
	}
}

func TestOrderBy(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).
		OrderBy(instance.F("username"), types.ASC)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(ast.Ordering) != 1 {
		t.Errorf("Expected 1 ordering, got %d", len(ast.Ordering))
	}
	if ast.Ordering[0].Direction != types.ASC {
		t.Errorf("Expected ASC direction, got %v", ast.Ordering[0].Direction)
	}
}

func TestLimit(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).Limit(10)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Limit == nil {
		t.Fatal("Expected Limit to be set")
	}
	if *ast.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", *ast.Limit)
	}
}

func TestOffset(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table).Offset(20)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if ast.Offset == nil {
		t.Fatal("Expected Offset to be set")
	}
	if *ast.Offset != 20 {
		t.Errorf("Expected offset 20, got %d", *ast.Offset)
	}
}

func TestMustBuild_Success(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	builder := astql.Select(table)
	ast := builder.MustBuild()

	if ast == nil {
		t.Fatal("Expected AST, got nil")
	}
}

func TestMustBuild_Panics(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic from MustBuild")
		}
	}()

	// Create an invalid builder (Fields on INSERT)
	astql.Insert(table).
		Fields(instance.F("id")).
		MustBuild()
}

func TestErrorPropagation(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	// Create a builder with an operation error (Fields on INSERT)
	builder := astql.Insert(table).
		Fields(instance.F("id"))

	// Further method calls should not panic even with an error set
	builder.Where(instance.C(instance.F("id"), "=", instance.P("id")))
	builder.Limit(10)

	// Error should be returned on Build()
	_, err := builder.Build()
	if err == nil {
		t.Error("Expected error to be propagated")
	}
}

// Test GetError method.
func TestGetError(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	// Create a builder with an error
	builder := astql.Insert(table).Fields(instance.F("id"))

	// GetError should return the error
	err := builder.GetError()
	if err == nil {
		t.Error("Expected GetError to return error")
	}
	if err.Error() != "Fields() can only be used with SELECT queries" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// Test SetError method.
func TestSetError(t *testing.T) {
	instance := createBuilderTestInstance(t)
	table := instance.T("users")

	// Create a valid builder
	builder := astql.Select(table)

	// Manually set an error
	builder.SetError(fmt.Errorf("custom error"))

	// Error should be returned on Build()
	_, err := builder.Build()
	if err == nil {
		t.Error("Expected error to be returned")
	}
	if err.Error() != "custom error" {
		t.Errorf("Expected 'custom error', got: %v", err)
	}
}

// Test MustRender success case.
func TestMustRender_Success(t *testing.T) {
	instance := createBuilderTestInstance(t)

	result := astql.Select(instance.T("users")).
		Fields(instance.F("id")).
		MustRender()

	if result.SQL != `SELECT "id" FROM "users"` {
		t.Errorf("Expected SQL, got: %s", result.SQL)
	}
}

// Test MustRender panics on error.
func TestMustRender_Panics(t *testing.T) {
	instance := createBuilderTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MustRender to panic")
		}
	}()

	// Create invalid builder (Fields on INSERT)
	astql.Insert(instance.T("users")).
		Fields(instance.F("id")).
		MustRender()
}

// Test Join method (wrapper for InnerJoin).
func TestJoin(t *testing.T) {
	instance := createBuilderTestInstance(t)

	result, err := astql.Select(instance.T("users")).
		Fields(instance.F("username")).
		Join(
			instance.T("posts"),
			astql.CF(instance.F("id"), "=", instance.F("user_id")),
		).
		Render()
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	expected := `SELECT "username" FROM "users" INNER JOIN "posts" ON "id" = "user_id"`
	if result.SQL != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
	}
}
