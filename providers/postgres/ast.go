package postgres

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// JoinType represents the type of SQL join.
type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
)

// Join represents a SQL JOIN clause.
type Join struct {
	On    types.ConditionItem
	Table types.Table
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
	Updates map[types.Field]types.Param
	Action  ConflictAction
	Columns []types.Field
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
	Field     types.Field
	Aggregate AggregateFunc
	Case      *CaseExpression     // For CASE expressions in SELECT
	Coalesce  *CoalesceExpression // For COALESCE expressions
	NullIf    *NullIfExpression   // For NULLIF expressions
	Math      *MathExpression     // For math functions
	Alias     string
}

// AST extends QueryAST with PostgreSQL-specific features.
type AST struct {
	*types.QueryAST
	OnConflict       *ConflictClause
	Joins            []Join
	GroupBy          []types.Field
	Having           []types.Condition
	FieldExpressions []FieldExpression
	Returning        []types.Field
	Distinct         bool
}

// NewAST creates a new PostgreSQL AST from a base QueryAST.
func NewAST(base *types.QueryAST) *AST {
	return &AST{
		QueryAST: base,
	}
}

// Validate extends base validation with PostgreSQL-specific rules.
func (ast *AST) Validate() error {
	// Custom validation for PostgreSQL-specific operations first
	switch ast.Operation {
	case types.OpNotify:
		// PostgreSQL NOTIFY is valid with or without payload
		// No additional validation needed

	case types.OpListen, types.OpUnlisten:
		// These operations don't need payload validation

	default:
		// For other operations, use base validation
		if err := ast.QueryAST.Validate(); err != nil {
			return err
		}
	}

	// PostgreSQL-specific validation
	switch ast.Operation {
	case types.OpSelect:
		// If using GROUP BY, all non-aggregate fields in SELECT must be in GROUP BY
		// (This is a simplified check - real implementation would be more thorough)
		// Having requires GROUP BY which is already validated elsewhere

	case types.OpInsert:
		// ON CONFLICT only makes sense with INSERT
		if ast.OnConflict != nil && len(ast.OnConflict.Columns) == 0 {
			return fmt.Errorf("ON CONFLICT requires at least one column")
		}

	case types.OpUpdate, types.OpDelete:
		// These operations can have RETURNING but not other SELECT features
		if ast.Distinct || len(ast.Joins) > 0 || len(ast.GroupBy) > 0 {
			return fmt.Errorf("%s cannot have SELECT features like DISTINCT, JOIN, or GROUP BY", ast.Operation)
		}
	}

	return nil
}
