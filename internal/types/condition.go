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

// AggregateCondition represents a HAVING condition on an aggregate function.
// Used for conditions like: HAVING COUNT(*) > :min_count or HAVING SUM("amount") >= :threshold
//
// Examples:
//
//	HAVING COUNT(*) > 10:
//	  AggregateCondition{Func: AggCountField, Field: nil, Operator: GT, Value: param}
//
//	HAVING SUM("amount") >= :threshold:
//	  AggregateCondition{Func: AggSum, Field: &amountField, Operator: GE, Value: param}
type AggregateCondition struct {
	Func     AggregateFunc // SUM, AVG, MIN, MAX, COUNT, COUNT_DISTINCT
	Field    *Field        // nil for COUNT(*), otherwise the field to aggregate
	Operator Operator
	Value    Param
}

// Example: field BETWEEN :low AND :high.
type BetweenCondition struct {
	Field   Field
	Low     Param
	High    Param
	Negated bool // true for NOT BETWEEN
}

// Implement ConditionItem interface.
func (Condition) IsConditionItem()          {}
func (ConditionGroup) IsConditionItem()     {}
func (AggregateCondition) IsConditionItem() {}
func (BetweenCondition) IsConditionItem()   {}
