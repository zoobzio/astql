package astql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestBuilderCreation(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")

	t.Run("Select builder", func(t *testing.T) {
		b := Select(table)
		if b.ast.Operation != types.OpSelect {
			t.Errorf("Expected OpSelect, got %s", b.ast.Operation)
		}
		if b.ast.Target.Name != "User" {
			t.Errorf("Expected target 'User', got '%s'", b.ast.Target.Name)
		}
	})

	t.Run("Insert builder", func(t *testing.T) {
		b := Insert(table)
		if b.ast.Operation != types.OpInsert {
			t.Errorf("Expected OpInsert, got %s", b.ast.Operation)
		}
		if b.ast.Target.Name != "User" {
			t.Errorf("Expected target 'User', got '%s'", b.ast.Target.Name)
		}
	})

	t.Run("Update builder", func(t *testing.T) {
		b := Update(table)
		if b.ast.Operation != types.OpUpdate {
			t.Errorf("Expected OpUpdate, got %s", b.ast.Operation)
		}
		if b.ast.Updates == nil {
			t.Error("Expected Updates map to be initialized")
		}
	})

	t.Run("Delete builder", func(t *testing.T) {
		b := Delete(table)
		if b.ast.Operation != types.OpDelete {
			t.Errorf("Expected OpDelete, got %s", b.ast.Operation)
		}
	})

	t.Run("Count builder", func(t *testing.T) {
		b := Count(table)
		if b.ast.Operation != types.OpCount {
			t.Errorf("Expected OpCount, got %s", b.ast.Operation)
		}
	})

}

func TestBuilderFields(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field1 := F("id")
	field2 := F("name")

	t.Run("Valid fields on SELECT", func(t *testing.T) {
		b := Select(table).Fields(field1, field2)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Fields) != 2 {
			t.Errorf("Expected 2 fields, got %d", len(b.ast.Fields))
		}
	})

	t.Run("Fields on non-SELECT", func(t *testing.T) {
		b := Update(table).Fields(field1)
		if b.err == nil {
			t.Error("Expected error for Fields on UPDATE")
		}
		if !strings.Contains(b.err.Error(), "can only be used with SELECT") {
			t.Errorf("Unexpected error message: %v", b.err)
		}
	})
}

func TestBuilderWhere(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field := F("id")
	param := P("user_id")

	t.Run("Single condition", func(t *testing.T) {
		b := Select(table).Where(C(field, EQ, param))
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if b.ast.WhereClause == nil {
			t.Error("Expected WhereClause to be set")
		}
	})

	t.Run("Multiple conditions combine with AND", func(t *testing.T) {
		field2 := F("status")
		param2 := P("status")

		b := Select(table).
			Where(C(field, EQ, param)).
			Where(C(field2, EQ, param2))

		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		// Should be an AND group
		if group, ok := b.ast.WhereClause.(types.ConditionGroup); ok {
			if group.Logic != types.AND {
				t.Errorf("Expected AND logic, got %s", group.Logic)
			}
			if len(group.Conditions) != 2 {
				t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
			}
		} else {
			t.Error("Expected WhereClause to be a ConditionGroup")
		}
	})

	t.Run("WhereField convenience method", func(t *testing.T) {
		b := Select(table).WhereField(field, EQ, param)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if b.ast.WhereClause == nil {
			t.Error("Expected WhereClause to be set")
		}
	})
}

func TestBuilderSet(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field := F("name")
	param := P("new_name")

	t.Run("Valid Set on UPDATE", func(t *testing.T) {
		b := Update(table).Set(field, param)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Updates) != 1 {
			t.Errorf("Expected 1 update, got %d", len(b.ast.Updates))
		}
		if b.ast.Updates[field].Name != "new_name" {
			t.Errorf("Expected param 'new_name', got '%s'", b.ast.Updates[field].Name)
		}
	})

	t.Run("Set on non-UPDATE", func(t *testing.T) {
		b := Select(table).Set(field, param)
		if b.err == nil {
			t.Error("Expected error for Set on SELECT")
		}
		if !strings.Contains(b.err.Error(), "can only be used with UPDATE") {
			t.Errorf("Unexpected error message: %v", b.err)
		}
	})

	t.Run("Multiple Sets", func(t *testing.T) {
		field2 := F("email")
		param2 := P("new_email")

		b := Update(table).Set(field, param).Set(field2, param2)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Updates) != 2 {
			t.Errorf("Expected 2 updates, got %d", len(b.ast.Updates))
		}
	})
}

func TestBuilderValues(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field1 := F("name")
	field2 := F("email")
	param1 := P("name")
	param2 := P("email")

	t.Run("Valid Values on INSERT", func(t *testing.T) {
		values := map[types.Field]types.Param{
			field1: param1,
			field2: param2,
		}
		b := Insert(table).Values(values)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Values) != 1 {
			t.Errorf("Expected 1 value set, got %d", len(b.ast.Values))
		}
		if len(b.ast.Values[0]) != 2 {
			t.Errorf("Expected 2 fields in value set, got %d", len(b.ast.Values[0]))
		}
	})

	t.Run("Values on non-INSERT", func(t *testing.T) {
		values := map[types.Field]types.Param{field1: param1}
		b := Select(table).Values(values)
		if b.err == nil {
			t.Error("Expected error for Values on SELECT")
		}
		if !strings.Contains(b.err.Error(), "can only be used with INSERT") {
			t.Errorf("Unexpected error message: %v", b.err)
		}
	})

	t.Run("Multiple Values", func(t *testing.T) {
		values1 := map[types.Field]types.Param{field1: param1}
		values2 := map[types.Field]types.Param{field1: param2}

		b := Insert(table).Values(values1).Values(values2)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Values) != 2 {
			t.Errorf("Expected 2 value sets, got %d", len(b.ast.Values))
		}
	})
}

func TestBuilderOrderBy(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field1 := F("created_at")
	field2 := F("name")

	t.Run("Single OrderBy", func(t *testing.T) {
		b := Select(table).OrderBy(field1, types.DESC)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Ordering) != 1 {
			t.Errorf("Expected 1 ordering, got %d", len(b.ast.Ordering))
		}
		if b.ast.Ordering[0].Field.Name != "created_at" {
			t.Errorf("Expected field 'created_at', got '%s'", b.ast.Ordering[0].Field.Name)
		}
		if b.ast.Ordering[0].Direction != types.DESC {
			t.Errorf("Expected DESC direction, got %s", b.ast.Ordering[0].Direction)
		}
	})

	t.Run("Multiple OrderBy", func(t *testing.T) {
		b := Select(table).OrderBy(field1, types.DESC).OrderBy(field2, types.ASC)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if len(b.ast.Ordering) != 2 {
			t.Errorf("Expected 2 orderings, got %d", len(b.ast.Ordering))
		}
	})
}

func TestBuilderLimitOffset(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")

	t.Run("Limit", func(t *testing.T) {
		b := Select(table).Limit(10)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if b.ast.Limit == nil || *b.ast.Limit != 10 {
			t.Error("Expected limit to be 10")
		}
	})

	t.Run("Offset", func(t *testing.T) {
		b := Select(table).Offset(20)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if b.ast.Offset == nil || *b.ast.Offset != 20 {
			t.Error("Expected offset to be 20")
		}
	})

	t.Run("Limit and Offset", func(t *testing.T) {
		b := Select(table).Limit(10).Offset(20)
		if b.err != nil {
			t.Errorf("Unexpected error: %v", b.err)
		}
		if b.ast.Limit == nil || *b.ast.Limit != 10 {
			t.Error("Expected limit to be 10")
		}
		if b.ast.Offset == nil || *b.ast.Offset != 20 {
			t.Error("Expected offset to be 20")
		}
	})
}

func TestBuilderBuild(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field := F("name")
	param := P("new_name")

	t.Run("Successful build", func(t *testing.T) {
		b := Update(table).Set(field, param)
		ast, err := b.Build()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if ast == nil {
			t.Error("Expected AST to be returned")
			return
		}
		if ast.Operation != types.OpUpdate {
			t.Errorf("Expected OpUpdate, got %s", ast.Operation)
		}
	})

	t.Run("Build with error", func(t *testing.T) {
		b := &Builder{err: fmt.Errorf("test error")}
		_, err := b.Build()
		if err == nil {
			t.Error("Expected error from Build")
		}
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got '%s'", err.Error())
		}
	})

	t.Run("Build with validation error", func(t *testing.T) {
		// Empty table name should fail validation
		b := &Builder{ast: &types.QueryAST{Operation: types.OpSelect}}
		_, err := b.Build()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "target table is required") {
			t.Errorf("Expected 'target table is required' error, got '%s'", err.Error())
		}
	})
}

func TestBuilderMustBuild(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field := F("name")
	param := P("new_name")

	t.Run("Successful MustBuild", func(t *testing.T) {
		b := Update(table).Set(field, param)
		ast := b.MustBuild()
		if ast == nil {
			t.Error("Expected AST to be returned")
			return
		}
		if ast.Operation != types.OpUpdate {
			t.Errorf("Expected OpUpdate, got %s", ast.Operation)
		}
	})

	t.Run("MustBuild with panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected MustBuild to panic")
			}
		}()

		b := &Builder{err: fmt.Errorf("test error")}
		b.MustBuild()
	})
}

func TestQueryASTValidate(t *testing.T) {
	// Register test structs
	SetupTest(t)

	field := F("name")
	param := P("value")

	t.Run("Empty target table", func(t *testing.T) {
		ast := &types.QueryAST{Operation: types.OpSelect}
		err := ast.Validate()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "target table is required") {
			t.Errorf("Expected 'target table is required' error, got '%s'", err.Error())
		}
	})

	t.Run("Valid SELECT", func(t *testing.T) {
		ast := &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
		}
		err := ast.Validate()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("INSERT without values", func(t *testing.T) {
		ast := &types.QueryAST{
			Operation: types.OpInsert,
			Target:    types.Table{Name: "users"},
		}
		err := ast.Validate()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "INSERT requires at least one value set") {
			t.Errorf("Expected 'INSERT requires at least one value set' error, got '%s'", err.Error())
		}
	})

	t.Run("UPDATE without updates", func(t *testing.T) {
		ast := &types.QueryAST{
			Operation: types.OpUpdate,
			Target:    types.Table{Name: "users"},
			Updates:   map[types.Field]types.Param{},
		}
		err := ast.Validate()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "UPDATE requires at least one field to update") {
			t.Errorf("Expected 'UPDATE requires at least one field to update' error, got '%s'", err.Error())
		}
	})

	t.Run("Unsupported operation", func(t *testing.T) {
		ast := &types.QueryAST{
			Operation: types.Operation("UNKNOWN"),
			Target:    types.Table{Name: "users"},
		}
		err := ast.Validate()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "unsupported operation") {
			t.Errorf("Expected 'unsupported operation' error, got '%s'", err.Error())
		}
	})

	t.Run("INSERT with mismatched value sets", func(t *testing.T) {
		field2 := F("email")
		ast := &types.QueryAST{
			Operation: types.OpInsert,
			Target:    types.Table{Name: "users"},
			Values: []map[types.Field]types.Param{
				{field: param},
				{field: param, field2: param},
			},
		}
		err := ast.Validate()
		if err == nil {
			t.Error("Expected validation error")
		}
		if !strings.Contains(err.Error(), "different number of fields") {
			t.Errorf("Expected 'different number of fields' error, got '%s'", err.Error())
		}
	})
}

func TestBuilderGettersSetters(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	b := Select(table)

	t.Run("GetAST", func(t *testing.T) {
		ast := b.GetAST()
		if ast == nil {
			t.Error("Expected AST to be returned")
			return
		}
		if ast.Operation != types.OpSelect {
			t.Errorf("Expected OpSelect, got %s", ast.Operation)
		}
	})

	t.Run("GetError", func(t *testing.T) {
		err := b.GetError()
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("SetError", func(t *testing.T) {
		testErr := fmt.Errorf("test error")
		b.SetError(testErr)
		err := b.GetError()
		if err == nil {
			t.Error("Expected error to be set")
		}
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got '%s'", err.Error())
		}
	})
}

func TestBuilderChainAfterError(t *testing.T) {
	// Register test structs
	SetupTest(t)

	table := T("User")
	field := F("id")
	param := P("user_id")

	// Create a builder with an error
	b := Select(table)
	b.SetError(fmt.Errorf("test error"))

	// All subsequent operations should be no-ops
	b = b.Fields(field).
		Where(C(field, EQ, param)).
		OrderBy(field, types.ASC).
		Limit(10).
		Offset(20)

	// Error should still be the original error
	if b.err.Error() != "test error" {
		t.Errorf("Expected error to remain 'test error', got '%s'", b.err.Error())
	}

	// AST should not have been modified
	if len(b.ast.Fields) != 0 {
		t.Error("Expected Fields to remain empty after error")
	}
	if b.ast.WhereClause != nil {
		t.Error("Expected WhereClause to remain nil after error")
	}
	if b.ast.Limit != nil {
		t.Error("Expected Limit to remain nil after error")
	}
}
