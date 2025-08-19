package types

// Condition represents a simple condition.
// Values are always parameters, never literals.
// This is exported from the internal package so providers can use it,
// but external users cannot import this package.
type Condition struct {
	Field    Field
	Operator Operator
	Value    Param
}

// ConditionItem represents either a single condition or a group of conditions.
type ConditionItem interface {
	IsConditionItem()
}

// LogicOperator represents how conditions are combined.
type LogicOperator string

const (
	AND LogicOperator = "AND"
	OR  LogicOperator = "OR"
)

// ConditionGroup represents grouped conditions with AND/OR logic.
type ConditionGroup struct {
	Logic      LogicOperator
	Conditions []ConditionItem
}

// Implement ConditionItem interface.
func (Condition) IsConditionItem()      {}
func (ConditionGroup) IsConditionItem() {}
