package astql

import "fmt"

// Operator represents query comparison operators.
type Operator string

const (
	// Basic comparison operators.
	EQ Operator = "="
	NE Operator = "!="
	GT Operator = ">"
	GE Operator = ">="
	LT Operator = "<"
	LE Operator = "<="

	// Extended operators.
	IN        Operator = "IN"
	NotIn     Operator = "NOT IN"
	LIKE      Operator = "LIKE"
	NotLike   Operator = "NOT LIKE"
	IsNull    Operator = "IS NULL"
	IsNotNull Operator = "IS NOT NULL"
	EXISTS    Operator = "EXISTS"
	NotExists Operator = "NOT EXISTS"
)

// Operation represents the type of query operation.
type Operation string

const (
	OpSelect   Operation = "SELECT"
	OpInsert   Operation = "INSERT"
	OpUpdate   Operation = "UPDATE"
	OpDelete   Operation = "DELETE"
	OpCount    Operation = "COUNT"
	OpListen   Operation = "LISTEN"
	OpNotify   Operation = "NOTIFY"
	OpUnlisten Operation = "UNLISTEN"
)

// Direction represents sort direction.
type Direction string

const (
	ASC  Direction = "ASC"
	DESC Direction = "DESC"
)

// Table represents a validated table reference.
type Table struct {
	Name  string
	Alias string
}

// Field represents a validated field reference.
type Field struct {
	Name  string // The field name (required)
	Table string // Optional table/alias prefix
}

// Values are always parameters, never literals.
type Condition struct {
	Field    Field
	Operator Operator
	Value    Param
}

// ConditionItem represents either a single condition or a group of conditions.
type ConditionItem interface {
	isConditionItem()
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

// FieldComparison represents a comparison between two fields.
type FieldComparison struct {
	LeftField  Field
	Operator   Operator
	RightField Field
}

// SubqueryCondition represents a condition that uses a subquery.
type SubqueryCondition struct {
	Subquery Subquery
	Field    *Field
	Operator Operator
}

// Subquery represents a nested query.
type Subquery struct {
	AST interface{} // Can be *QueryAST or *PostgresAST
}

// Implement ConditionItem interface.
func (Condition) isConditionItem()         {}
func (ConditionGroup) isConditionItem()    {}
func (FieldComparison) isConditionItem()   {}
func (SubqueryCondition) isConditionItem() {}

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	Field     Field
	Direction Direction
}

// QueryAST represents the core abstract syntax tree for queries.
//
//nolint:govet // fieldalignment: Logical grouping is preferred over memory optimization
type QueryAST struct {
	Operation     Operation
	Target        Table
	Fields        []Field
	WhereClause   ConditionItem
	Ordering      []OrderBy
	Limit         *int
	Offset        *int
	Updates       map[Field]Param   // For UPDATE operations
	Values        []map[Field]Param // For INSERT operations
	NotifyPayload *Param            // For NOTIFY operations
}

// TryT creates a validated table reference, returning an error if invalid.
func TryT(name string, alias ...string) (Table, error) {
	// Validate table name against Sentinel registry
	if err := ValidateTable(name); err != nil {
		return Table{}, fmt.Errorf("invalid table: %w", err)
	}

	table := Table{Name: name}
	if len(alias) > 0 {
		// Enforce single lowercase letter for aliases
		if !isValidTableAlias(alias[0]) {
			return Table{}, fmt.Errorf("table alias must be single lowercase letter (a-z), got: %s", alias[0])
		}
		table.Alias = alias[0]
	}
	return table, nil
}

// T creates a validated table reference.
func T(name string, alias ...string) Table {
	table, err := TryT(name, alias...)
	if err != nil {
		panic(err)
	}
	return table
}

// isValidTableAlias checks if a string is a valid single-letter table alias.
func isValidTableAlias(alias string) bool {
	return len(alias) == 1 && alias[0] >= 'a' && alias[0] <= 'z'
}

// TryF creates a validated field reference, returning an error if invalid.
func TryF(name string) (Field, error) {
	// Validate field name against Sentinel registry
	if err := ValidateField(name); err != nil {
		return Field{}, fmt.Errorf("invalid field: %w", err)
	}

	return Field{Name: name}, nil
}

// F creates a validated field reference.
func F(name string) Field {
	field, err := TryF(name)
	if err != nil {
		panic(err)
	}
	return field
}

// WithTable sets the table/alias prefix for a field.
func (f Field) WithTable(tableOrAlias string) Field {
	// Must be either:
	// 1. A single lowercase letter (table alias), OR
	// 2. A registered table name
	if isValidTableAlias(tableOrAlias) {
		// It's a valid single-letter alias
		f.Table = tableOrAlias
	} else if err := ValidateTable(tableOrAlias); err == nil {
		// It's a valid table name
		f.Table = tableOrAlias
	} else {
		panic(fmt.Errorf("WithTable requires single-letter alias (a-z) or valid table name, got: %s", tableOrAlias))
	}
	return f
}

// C creates a simple condition.
func C(field Field, op Operator, value Param) Condition {
	// For IsNull and IsNotNull, the value param is ignored
	// but we still need to pass something, so we'll create a dummy param
	if op == IsNull || op == IsNotNull {
		// Use a special placeholder that won't be rendered
		value = Param{Type: ParamNamed, Name: "_null_"}
	}

	return Condition{
		Field:    field,
		Operator: op,
		Value:    value,
	}
}

// And creates a ConditionGroup with AND logic.
func And(conditions ...ConditionItem) ConditionGroup {
	return ConditionGroup{
		Logic:      AND,
		Conditions: conditions,
	}
}

// Or creates a ConditionGroup with OR logic.
func Or(conditions ...ConditionItem) ConditionGroup {
	return ConditionGroup{
		Logic:      OR,
		Conditions: conditions,
	}
}

// TODO: Move to postgres package.
func CF(left Field, op Operator, right Field) FieldComparison {
	return FieldComparison{
		LeftField:  left,
		Operator:   op,
		RightField: right,
	}
}

// TODO: Move to postgres package.
func CSub(field Field, op Operator, subquery Subquery) SubqueryCondition {
	// Validate operator is appropriate for subqueries
	switch op {
	case IN, NotIn:
		// Valid operators that require a field
	default:
		panic(fmt.Errorf("operator %s cannot be used with CSub - use CSubExists for EXISTS/NOT EXISTS", op))
	}

	return SubqueryCondition{
		Field:    &field,
		Operator: op,
		Subquery: subquery,
	}
}

// TODO: Move to postgres package.
func CSubExists(op Operator, subquery Subquery) SubqueryCondition {
	// Validate operator
	switch op {
	case EXISTS, NotExists:
		// Valid operators
	default:
		panic(fmt.Errorf("CSubExists only accepts EXISTS or NOT EXISTS, got %s", op))
	}

	return SubqueryCondition{
		Field:    nil,
		Operator: op,
		Subquery: subquery,
	}
}

// TODO: Move to postgres package.
func Sub(builder *Builder) Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return Subquery{AST: ast}
}
