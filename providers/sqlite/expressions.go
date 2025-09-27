package sqlite

import (
	"github.com/zoobzio/astql/internal/types"
)

// CaseExpression represents a CASE expression in SELECT.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type CaseExpression struct {
	WhenClauses []WhenClause
	ElseClause  *types.Param
	Alias       string
}

// WhenClause represents a WHEN condition THEN result clause.
type WhenClause struct {
	Condition types.ConditionItem
	Result    types.Param
}

// CoalesceExpression represents a COALESCE expression.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type CoalesceExpression struct {
	Values []types.Param
	Alias  string
}

// NullIfExpression represents a NULLIF expression.
type NullIfExpression struct {
	Value1 types.Param
	Value2 types.Param
	Alias  string
}

// SubqueryCondition represents a condition that uses a subquery.
type SubqueryCondition struct {
	Subquery Subquery
	Field    *types.Field
	Operator types.Operator
}

// Subquery represents a nested query.
type Subquery struct {
	AST interface{} // Can be *AST or base *types.QueryAST
}

// Implement ConditionItem interface.
func (SubqueryCondition) IsConditionItem() {}
