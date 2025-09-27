package sqlite

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// Aggregate Functions

// Sum creates a SUM aggregate expression.
func Sum(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggSum,
	}
}

// Avg creates an AVG aggregate expression.
func Avg(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggAvg,
	}
}

// Min creates a MIN aggregate expression.
func Min(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggMin,
	}
}

// Max creates a MAX aggregate expression.
func Max(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggMax,
	}
}

// CountField creates a COUNT aggregate expression for a specific field.
func CountField(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggCount,
	}
}

// CountDistinct creates a COUNT DISTINCT aggregate expression.
func CountDistinct(field types.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggCountDistinct,
	}
}

// As adds an alias to a FieldExpression.
func (fe FieldExpression) As(alias string) FieldExpression {
	fe.Alias = alias
	return fe
}

// CASE Expression

// CaseBuilder helps construct CASE expressions.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type CaseBuilder struct {
	whenClauses []WhenClause
	elseClause  *types.Param
}

// Case creates a new CASE expression builder.
func Case() *CaseBuilder {
	return &CaseBuilder{}
}

// When adds a WHEN condition THEN result clause.
func (cb *CaseBuilder) When(condition types.ConditionItem, result types.Param) *CaseBuilder {
	cb.whenClauses = append(cb.whenClauses, WhenClause{
		Condition: condition,
		Result:    result,
	})
	return cb
}

// Else adds an ELSE clause.
func (cb *CaseBuilder) Else(result types.Param) *CaseBuilder {
	cb.elseClause = &result
	return cb
}

// End finalizes the CASE expression.
func (cb *CaseBuilder) End() CaseExpression {
	if len(cb.whenClauses) == 0 {
		panic(fmt.Errorf("CASE expression requires at least one WHEN clause"))
	}
	return CaseExpression{
		WhenClauses: cb.whenClauses,
		ElseClause:  cb.elseClause,
	}
}

// As adds an alias to a CaseExpression.
func (ce CaseExpression) As(alias string) CaseExpression {
	ce.Alias = alias
	return ce
}

// COALESCE Function

// Coalesce creates a COALESCE expression.
func Coalesce(values ...types.Param) CoalesceExpression {
	if len(values) < 2 {
		panic(fmt.Errorf("COALESCE requires at least 2 arguments"))
	}
	return CoalesceExpression{
		Values: values,
	}
}

// As adds an alias to a CoalesceExpression.
func (ce CoalesceExpression) As(alias string) CoalesceExpression {
	ce.Alias = alias
	return ce
}

// NULLIF Function

// NullIf creates a NULLIF expression.
func NullIf(value1, value2 types.Param) NullIfExpression {
	return NullIfExpression{
		Value1: value1,
		Value2: value2,
	}
}

// As adds an alias to a NullIfExpression.
func (ne NullIfExpression) As(alias string) NullIfExpression {
	ne.Alias = alias
	return ne
}

// Subquery Support

// Sub creates a subquery from a builder.
func Sub(builder *Builder) Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return Subquery{AST: ast}
}

// SubBase creates a subquery from a base builder.
func SubBase(builder *astql.Builder) Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return Subquery{AST: ast}
}

// CSub creates a subquery condition with a field (for IN/NOT IN).
func CSub(field types.Field, op types.Operator, subquery Subquery) SubqueryCondition {
	// Validate operator is appropriate for subqueries
	switch op {
	case types.IN, types.NotIn:
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

// CSubExists creates an EXISTS/NOT EXISTS subquery condition.
func CSubExists(op types.Operator, subquery Subquery) SubqueryCondition {
	// Validate operator
	switch op {
	case types.EXISTS, types.NotExists:
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
