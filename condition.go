package astql

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// TryC creates a simple condition, returning an error if invalid.
func TryC(f types.Field, op types.Operator, v types.Param) (types.Condition, error) {
	// No validation needed - just creates the struct
	return types.Condition{
		Field:    f,
		Operator: op,
		Value:    v,
	}, nil
}

// C creates a simple condition.
func C(f types.Field, op types.Operator, v types.Param) types.Condition {
	c, _ := TryC(f, op, v) //nolint:errcheck // Cannot fail
	return c
}

// TryNull creates an IS NULL condition, returning an error if invalid.
func TryNull(f types.Field) (types.Condition, error) {
	return types.Condition{
		Field:    f,
		Operator: types.IsNull,
		Value:    types.Param{Name: "_null_"}, // Placeholder, not rendered
	}, nil
}

// Null creates an IS NULL condition.
func Null(f types.Field) types.Condition {
	c, _ := TryNull(f) //nolint:errcheck // Cannot fail
	return c
}

// TryNotNull creates an IS NOT NULL condition, returning an error if invalid.
func TryNotNull(f types.Field) (types.Condition, error) {
	return types.Condition{
		Field:    f,
		Operator: types.IsNotNull,
		Value:    types.Param{Name: "_null_"}, // Placeholder, not rendered
	}, nil
}

// NotNull creates an IS NOT NULL condition.
func NotNull(f types.Field) types.Condition {
	c, _ := TryNotNull(f) //nolint:errcheck // Cannot fail
	return c
}

// TryAnd creates a ConditionGroup with AND logic, returning an error if invalid.
func TryAnd(conditions ...types.ConditionItem) (types.ConditionGroup, error) {
	if len(conditions) == 0 {
		return types.ConditionGroup{}, fmt.Errorf("AND requires at least one condition")
	}
	return types.ConditionGroup{
		Logic:      types.AND,
		Conditions: conditions,
	}, nil
}

// And creates a ConditionGroup with AND logic.
func And(conditions ...types.ConditionItem) types.ConditionGroup {
	g, err := TryAnd(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}

// TryOr creates a ConditionGroup with OR logic, returning an error if invalid.
func TryOr(conditions ...types.ConditionItem) (types.ConditionGroup, error) {
	if len(conditions) == 0 {
		return types.ConditionGroup{}, fmt.Errorf("OR requires at least one condition")
	}
	return types.ConditionGroup{
		Logic:      types.OR,
		Conditions: conditions,
	}, nil
}

// Or creates a ConditionGroup with OR logic.
func Or(conditions ...types.ConditionItem) types.ConditionGroup {
	g, err := TryOr(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}
