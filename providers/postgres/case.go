package postgres

import "github.com/zoobzio/astql"

// CaseExpression represents a SQL CASE expression.
type CaseExpression struct {
	ElseValue   *astql.Param
	Alias       string
	WhenClauses []WhenClause
}

// WhenClause represents a single WHEN...THEN clause.
type WhenClause struct {
	Condition astql.ConditionItem // Reuses existing validation!
	Result    astql.Param         // Parameter reference only
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
func (b *CaseBuilder) When(condition astql.ConditionItem, result astql.Param) *CaseBuilder {
	b.expr.WhenClauses = append(b.expr.WhenClauses, WhenClause{
		Condition: condition,
		Result:    result,
	})
	return b
}

// Else sets the ELSE clause.
func (b *CaseBuilder) Else(result astql.Param) *CaseBuilder {
	b.expr.ElseValue = &result
	return b
}

// Build returns the CaseExpression.
func (b *CaseBuilder) Build() CaseExpression {
	return *b.expr
}

// As adds an alias to the CASE expression (for SELECT clauses).
func (b *CaseBuilder) As(alias string) *CaseBuilder {
	// Validate alias against registered field aliases
	if err := astql.ValidateFieldAlias(alias); err != nil {
		panic(err) // Consistent with other validation failures
	}
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
func (c CaseExpression) Else(value astql.Param) CaseExpression {
	c.ElseValue = &value
	return c
}
