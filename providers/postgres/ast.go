package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
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
	On    astql.ConditionItem
	Table astql.Table
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
	Updates map[astql.Field]astql.Param
	Action  ConflictAction
	Columns []astql.Field
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
	Field     astql.Field
	Aggregate AggregateFunc
	Case      *CaseExpression     // For CASE expressions in SELECT
	Coalesce  *CoalesceExpression // For COALESCE expressions
	NullIf    *NullIfExpression   // For NULLIF expressions
	Math      *MathExpression     // For math functions
	Alias     string
}

// AST extends QueryAST with PostgreSQL-specific features.
type AST struct {
	*astql.QueryAST
	OnConflict       *ConflictClause
	Joins            []Join
	GroupBy          []astql.Field
	Having           []astql.Condition
	FieldExpressions []FieldExpression
	Returning        []astql.Field
	Distinct         bool
}

// NewAST creates a new PostgreSQL AST from a base QueryAST.
func NewAST(base *astql.QueryAST) *AST {
	return &AST{
		QueryAST: base,
	}
}

// Validate extends base validation with PostgreSQL-specific rules.
func (ast *AST) Validate() error {
	// First run base validation
	if err := ast.QueryAST.Validate(); err != nil {
		return err
	}

	// PostgreSQL-specific validation
	switch ast.Operation {
	case astql.OpSelect:
		// If using GROUP BY, all non-aggregate fields in SELECT must be in GROUP BY
		// (This is a simplified check - real implementation would be more thorough)
		// Having requires GROUP BY which is already validated elsewhere

	case astql.OpInsert:
		// ON CONFLICT only makes sense with INSERT
		if ast.OnConflict != nil && len(ast.OnConflict.Columns) == 0 {
			return fmt.Errorf("ON CONFLICT requires at least one column")
		}

	case astql.OpUpdate, astql.OpDelete:
		// These operations can have RETURNING but not other SELECT features
		if ast.Distinct || len(ast.Joins) > 0 || len(ast.GroupBy) > 0 {
			return fmt.Errorf("%s cannot have SELECT features like DISTINCT, JOIN, or GROUP BY", ast.Operation)
		}
	}

	return nil
}
