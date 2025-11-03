package types

import "fmt"

// Operation represents the type of query operation.
type Operation string

const (
	OpSelect Operation = "SELECT"
	OpInsert Operation = "INSERT"
	OpUpdate Operation = "UPDATE"
	OpDelete Operation = "DELETE"
	OpCount  Operation = "COUNT"
)

// Direction represents sort direction.
type Direction string

const (
	ASC  Direction = "ASC"
	DESC Direction = "DESC"
)

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	Field     Field
	Direction Direction
}

// JoinType represents the type of SQL join.
type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	CrossJoin JoinType = "CROSS JOIN"
)

// Join represents a SQL JOIN clause.
type Join struct {
	On    ConditionItem
	Table Table
	Type  JoinType
}

// ConflictAction represents what to do on conflict.
type ConflictAction string

const (
	DoNothing ConflictAction = "DO NOTHING"
	DoUpdate  ConflictAction = "DO UPDATE"
)

// ConflictClause represents PostgreSQL's ON CONFLICT clause.
type ConflictClause struct {
	Updates map[Field]Param
	Action  ConflictAction
	Columns []Field
}

// AggregateFunc represents SQL aggregate functions.
type AggregateFunc string

const (
	AggSum AggregateFunc = "SUM"
	AggAvg AggregateFunc = "AVG"
	AggMin AggregateFunc = "MIN"
	AggMax AggregateFunc = "MAX"
	// Note: COUNT is already an operation, but can also be an aggregate on fields.
	AggCountField    AggregateFunc = "COUNT"
	AggCountDistinct AggregateFunc = "COUNT_DISTINCT"
)

// FieldExpression represents a field with optional aggregate function or SQL expression.
type FieldExpression struct {
	Field     Field
	Aggregate AggregateFunc
	Case      *CaseExpression     // For CASE expressions in SELECT
	Coalesce  *CoalesceExpression // For COALESCE expressions
	NullIf    *NullIfExpression   // For NULLIF expressions
	Math      *MathExpression     // For math functions
	Alias     string
}

// CaseExpression represents a SQL CASE expression.
type CaseExpression struct {
	ElseValue   *Param
	Alias       string
	WhenClauses []WhenClause
}

// WhenClause represents a single WHEN...THEN clause.
type WhenClause struct {
	Condition ConditionItem
	Result    Param
}

// CoalesceExpression represents a COALESCE function call.
type CoalesceExpression struct {
	Alias  string
	Values []Param
}

// NullIfExpression represents a NULLIF function call.
type NullIfExpression struct {
	Alias  string
	Value1 Param
	Value2 Param
}

// MathFunc represents SQL math functions.
type MathFunc string

const (
	MathRound MathFunc = "ROUND"
	MathFloor MathFunc = "FLOOR"
	MathCeil  MathFunc = "CEIL"
	MathAbs   MathFunc = "ABS"
	MathPower MathFunc = "POWER"
	MathSqrt  MathFunc = "SQRT"
)

// MathExpression represents a math function call.
type MathExpression struct {
	Function  MathFunc
	Field     Field
	Precision *Param // Optional, for ROUND
	Exponent  *Param // Optional, for POWER
	Alias     string
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
	AST *AST
}

// Constants for query complexity limits to prevent DoS attacks.
const (
	MaxSubqueryDepth  = 3   // Prevent DoS via deep nesting
	MaxJoinCount      = 10  // Maximum number of JOINs per query
	MaxConditionDepth = 5   // Maximum nesting depth of condition groups
	MaxFieldCount     = 100 // Maximum number of fields in SELECT
)

// Implement ConditionItem interface for new condition types.
func (FieldComparison) IsConditionItem()   {}
func (SubqueryCondition) IsConditionItem() {}

// AST represents the abstract syntax tree for PostgreSQL queries.
// This is exported from the internal package so the base package can use it,
// but external users cannot import this package.
//
//nolint:govet // fieldalignment: Logical grouping is preferred over memory optimization
type AST struct {
	Operation        Operation
	Target           Table
	Fields           []Field
	WhereClause      ConditionItem
	Ordering         []OrderBy
	Limit            *int
	Offset           *int
	Updates          map[Field]Param   // For UPDATE operations
	Values           []map[Field]Param // For INSERT operations
	OnConflict       *ConflictClause   // PostgreSQL ON CONFLICT
	Joins            []Join            // JOIN clauses
	GroupBy          []Field           // GROUP BY fields
	Having           []Condition       // HAVING conditions
	FieldExpressions []FieldExpression // Field expressions (aggregates, CASE, etc)
	Returning        []Field           // RETURNING fields (PostgreSQL)
	Distinct         bool              // DISTINCT flag
}

// Validate performs basic validation on the AST.
func (ast *AST) Validate() error {
	if ast.Target.Name == "" {
		return fmt.Errorf("target table is required")
	}

	// Check complexity limits
	if len(ast.Joins) > MaxJoinCount {
		return fmt.Errorf("too many JOINs: %d (max %d)", len(ast.Joins), MaxJoinCount)
	}

	totalFields := len(ast.Fields) + len(ast.FieldExpressions)
	if totalFields > MaxFieldCount {
		return fmt.Errorf("too many fields: %d (max %d)", totalFields, MaxFieldCount)
	}

	// Validate condition depth
	if ast.WhereClause != nil {
		if err := validateConditionDepth(ast.WhereClause, 0); err != nil {
			return err
		}
	}

	switch ast.Operation {
	case OpSelect:
		// Fields are optional (defaults to *)
		// Can have JOINs, GROUP BY, HAVING, DISTINCT
	case OpInsert:
		if len(ast.Values) == 0 {
			return fmt.Errorf("INSERT requires at least one value set")
		}
		// Ensure all value sets have the same fields
		if len(ast.Values) > 1 {
			firstKeys := make(map[Field]bool)
			for k := range ast.Values[0] {
				firstKeys[k] = true
			}
			for i, valueSet := range ast.Values[1:] {
				if len(valueSet) != len(firstKeys) {
					return fmt.Errorf("value set %d has different number of fields", i+1)
				}
				for k := range valueSet {
					if !firstKeys[k] {
						return fmt.Errorf("value set %d has different fields", i+1)
					}
				}
			}
		}
		// ON CONFLICT only makes sense with INSERT
		if ast.OnConflict != nil && len(ast.OnConflict.Columns) == 0 {
			return fmt.Errorf("ON CONFLICT requires at least one column")
		}
	case OpUpdate:
		if len(ast.Updates) == 0 {
			return fmt.Errorf("UPDATE requires at least one field to update")
		}
		// UPDATE can have RETURNING but not SELECT features
		if ast.Distinct || len(ast.Joins) > 0 || len(ast.GroupBy) > 0 {
			return fmt.Errorf("UPDATE cannot have SELECT features like DISTINCT, JOIN, or GROUP BY")
		}
	case OpDelete:
		// DELETE can have RETURNING but not SELECT features
		if ast.Distinct || len(ast.Joins) > 0 || len(ast.GroupBy) > 0 {
			return fmt.Errorf("DELETE cannot have SELECT features like DISTINCT, JOIN, or GROUP BY")
		}
	case OpCount:
		// COUNT can have JOINs and WHERE but no fields
		// COUNT can have JOINs
	default:
		return fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	// HAVING requires GROUP BY
	if len(ast.Having) > 0 && len(ast.GroupBy) == 0 {
		return fmt.Errorf("HAVING requires GROUP BY")
	}

	return nil
}

// validateConditionDepth checks the nesting depth of condition groups.
func validateConditionDepth(cond ConditionItem, depth int) error {
	if depth > MaxConditionDepth {
		return fmt.Errorf("condition nesting too deep: %d (max %d)", depth, MaxConditionDepth)
	}

	switch c := cond.(type) {
	case ConditionGroup:
		for _, subCond := range c.Conditions {
			if err := validateConditionDepth(subCond, depth+1); err != nil {
				return err
			}
		}
	case Condition, FieldComparison, SubqueryCondition:
		// Leaf nodes, no further depth
	}

	return nil
}
