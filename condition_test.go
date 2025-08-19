package astql

import (
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestCondition(t *testing.T) {
	// Register test structs
	SetupTest(t)

	field := F("name") // Use registered field
	param := P("test_param")

	t.Run("Basic condition creation", func(t *testing.T) {
		cond := C(field, types.EQ, param)

		if cond.Field.Name != "name" {
			t.Errorf("Expected field name 'name', got '%s'", cond.Field.Name)
		}
		if cond.Operator != types.EQ {
			t.Errorf("Expected operator EQ, got %s", cond.Operator)
		}
		if cond.Value.Name != "test_param" {
			t.Errorf("Expected param name 'test_param', got '%s'", cond.Value.Name)
		}
	})

	t.Run("Null condition", func(t *testing.T) {
		cond := Null(field)

		if cond.Field.Name != "name" {
			t.Errorf("Expected field name 'name', got '%s'", cond.Field.Name)
		}
		if cond.Operator != types.IsNull {
			t.Errorf("Expected operator IsNull, got %s", cond.Operator)
		}
		// Value should be placeholder
		if cond.Value.Name != "_null_" {
			t.Errorf("Expected placeholder '_null_', got '%s'", cond.Value.Name)
		}
	})

	t.Run("NotNull condition", func(t *testing.T) {
		cond := NotNull(field)

		if cond.Operator != types.IsNotNull {
			t.Errorf("Expected operator IsNotNull, got %s", cond.Operator)
		}
		// Value should be placeholder
		if cond.Value.Name != "_null_" {
			t.Errorf("Expected placeholder '_null_', got '%s'", cond.Value.Name)
		}
	})
}

func TestConditionGroup(t *testing.T) {
	// Register test structs
	SetupTest(t)

	field1 := F("id") // Use registered fields
	field2 := F("name")
	param1 := P("param1")
	param2 := P("param2")

	cond1 := C(field1, types.EQ, param1)
	cond2 := C(field2, types.GT, param2)

	t.Run("AND group", func(t *testing.T) {
		group := And(cond1, cond2)

		if group.Logic != types.AND {
			t.Errorf("Expected AND logic, got %s", group.Logic)
		}
		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
		}
	})

	t.Run("OR group", func(t *testing.T) {
		group := Or(cond1, cond2)

		if group.Logic != types.OR {
			t.Errorf("Expected OR logic, got %s", group.Logic)
		}
		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
		}
	})

	t.Run("Nested groups", func(t *testing.T) {
		group1 := And(cond1, cond2)
		cond3 := C(field1, types.LT, param1)
		nestedGroup := Or(group1, cond3)

		if nestedGroup.Logic != types.OR {
			t.Errorf("Expected OR logic, got %s", nestedGroup.Logic)
		}
		if len(nestedGroup.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(nestedGroup.Conditions))
		}
	})

	t.Run("Empty groups panic", func(t *testing.T) {
		// Test And() with no conditions panics
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected And() with no conditions to panic")
				}
			}()
			And()
		}()

		// Test Or() with no conditions panics
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected Or() with no conditions to panic")
				}
			}()
			Or()
		}()
	})
}

func TestTryConditionFunctions(t *testing.T) {
	// Register test structs
	SetupTest(t)

	field := F("name")
	param := P("test_param")

	t.Run("TryC", func(t *testing.T) {
		cond, err := TryC(field, types.EQ, param)
		if err != nil {
			t.Errorf("TryC() unexpected error: %v", err)
		}
		if cond.Field.Name != "name" {
			t.Errorf("Expected field name 'name', got '%s'", cond.Field.Name)
		}
	})

	t.Run("TryNull", func(t *testing.T) {
		cond, err := TryNull(field)
		if err != nil {
			t.Errorf("TryNull() unexpected error: %v", err)
		}
		if cond.Operator != types.IsNull {
			t.Errorf("Expected operator IsNull, got %s", cond.Operator)
		}
	})

	t.Run("TryNotNull", func(t *testing.T) {
		cond, err := TryNotNull(field)
		if err != nil {
			t.Errorf("TryNotNull() unexpected error: %v", err)
		}
		if cond.Operator != types.IsNotNull {
			t.Errorf("Expected operator IsNotNull, got %s", cond.Operator)
		}
	})

	t.Run("TryAnd with conditions", func(t *testing.T) {
		cond1 := C(field, types.EQ, param)
		cond2 := C(field, types.NE, param)
		group, err := TryAnd(cond1, cond2)
		if err != nil {
			t.Errorf("TryAnd() unexpected error: %v", err)
		}
		if group.Logic != types.AND {
			t.Errorf("Expected AND logic, got %s", group.Logic)
		}
		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
		}
	})

	t.Run("TryAnd with no conditions", func(t *testing.T) {
		_, err := TryAnd()
		if err == nil {
			t.Error("Expected TryAnd() with no conditions to return error")
		}
		if err.Error() != "AND requires at least one condition" {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})

	t.Run("TryOr with conditions", func(t *testing.T) {
		cond1 := C(field, types.EQ, param)
		cond2 := C(field, types.NE, param)
		group, err := TryOr(cond1, cond2)
		if err != nil {
			t.Errorf("TryOr() unexpected error: %v", err)
		}
		if group.Logic != types.OR {
			t.Errorf("Expected OR logic, got %s", group.Logic)
		}
		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
		}
	})

	t.Run("TryOr with no conditions", func(t *testing.T) {
		_, err := TryOr()
		if err == nil {
			t.Error("Expected TryOr() with no conditions to return error")
		}
		if err.Error() != "OR requires at least one condition" {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})
}

func TestConditionItemInterface(t *testing.T) {
	// Register test structs
	SetupTest(t)

	// Test that Condition implements ConditionItem
	var _ types.ConditionItem = types.Condition{}

	// Test that ConditionGroup implements ConditionItem
	var _ types.ConditionItem = types.ConditionGroup{}

	// Test actual interface usage
	field := F("id") // Use registered field
	param := P("test")

	var items []types.ConditionItem
	items = append(items, C(field, types.EQ, param))
	items = append(items, And(C(field, types.GT, param), C(field, types.LT, param)))

	if len(items) != 2 {
		t.Errorf("Expected 2 ConditionItems, got %d", len(items))
	}
}

func TestLogicOperatorConstants(t *testing.T) {
	if types.AND != "AND" {
		t.Errorf("Expected AND to be 'AND', got '%s'", types.AND)
	}
	if types.OR != "OR" {
		t.Errorf("Expected OR to be 'OR', got '%s'", types.OR)
	}
}
