package postgres

import (
	"github.com/zoobzio/astql/internal/types"
)

// CaseExpression represents a SQL CASE expression.
type CaseExpression struct {
	ElseValue   *types.Param
	Alias       string
	WhenClauses []WhenClause
}

// WhenClause represents a single WHEN...THEN clause.
type WhenClause struct {
	Condition types.ConditionItem // Reuses existing validation!
	Result    types.Param         // Parameter reference only
}

// CaseBuilder provides fluent API for building CASE expressions.
type CaseBuilder struct {
	expr *CaseExpression
}

// Case creates a new CASE expression builder.
func Case() *CaseBuilder {
	return &CaseBuilder{
		expr: &CaseExpression{},
	}
}

// When adds a WHEN...THEN clause.
func (b *CaseBuilder) When(condition types.ConditionItem, result types.Param) *CaseBuilder {
	b.expr.WhenClauses = append(b.expr.WhenClauses, WhenClause{
		Condition: condition,
		Result:    result,
	})
	return b
}

// Else sets the ELSE clause.
func (b *CaseBuilder) Else(result types.Param) *CaseBuilder {
	b.expr.ElseValue = &result
	return b
}

// Build returns the CaseExpression.
func (b *CaseBuilder) Build() CaseExpression {
	return *b.expr
}

// As adds an alias to the CASE expression (for SELECT clauses).
func (b *CaseBuilder) As(alias string) *CaseBuilder {
	b.expr.Alias = alias
	return b
}

// CaseWithClauses convenience function to build a CASE expression with WhenClauses.
func CaseWithClauses(whenClauses ...WhenClause) CaseExpression {
	return CaseExpression{
		WhenClauses: whenClauses,
	}
}

// Else adds an else clause to a CaseExpression.
func (c CaseExpression) Else(value types.Param) CaseExpression {
	c.ElseValue = &value
	return c
}
