package astql

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// Helper functions for creating field expressions.

// Sum creates a SUM aggregate expression.
func Sum(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggSum,
	}
}

// Avg creates an AVG aggregate expression.
func Avg(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggAvg,
	}
}

// Min creates a MIN aggregate expression.
func Min(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggMin,
	}
}

// Max creates a MAX aggregate expression.
func Max(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggMax,
	}
}

// CountField creates a COUNT aggregate expression for a specific field.
func CountField(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggCountField,
	}
}

// CountDistinct creates a COUNT(DISTINCT) aggregate expression.
func CountDistinct(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Field:     field,
		Aggregate: types.AggCountDistinct,
	}
}

// CF creates a field comparison condition.
func CF(left types.Field, op types.Operator, right types.Field) types.FieldComparison {
	return types.FieldComparison{
		LeftField:  left,
		Operator:   op,
		RightField: right,
	}
}

// CSub creates a subquery condition with a field.
func CSub(field types.Field, op types.Operator, subquery types.Subquery) types.SubqueryCondition {
	// Validate operator is appropriate for subqueries
	switch op {
	case types.IN, types.NotIn:
		// Valid operators that require a field
	default:
		panic(fmt.Errorf("operator %s cannot be used with CSub - use CSubExists for EXISTS/NOT EXISTS", op))
	}

	return types.SubqueryCondition{
		Field:    &field,
		Operator: op,
		Subquery: subquery,
	}
}

// CSubExists creates an EXISTS/NOT EXISTS subquery condition.
func CSubExists(op types.Operator, subquery types.Subquery) types.SubqueryCondition {
	// Validate operator
	switch op {
	case types.EXISTS, types.NotExists:
		// Valid operators
	default:
		panic(fmt.Errorf("CSubExists only accepts EXISTS or NOT EXISTS, got %s", op))
	}

	return types.SubqueryCondition{
		Field:    nil,
		Operator: op,
		Subquery: subquery,
	}
}

// Sub creates a subquery from a builder.
func Sub(builder *Builder) types.Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return types.Subquery{AST: ast}
}

// Case creates a new CASE expression builder.
func Case() *CaseBuilder {
	return &CaseBuilder{
		expr: &types.CaseExpression{},
	}
}

// CaseBuilder provides fluent API for building CASE expressions.
type CaseBuilder struct {
	expr *types.CaseExpression
}

// When adds a WHEN...THEN clause.
func (cb *CaseBuilder) When(condition types.ConditionItem, result types.Param) *CaseBuilder {
	cb.expr.WhenClauses = append(cb.expr.WhenClauses, types.WhenClause{
		Condition: condition,
		Result:    result,
	})
	return cb
}

// Else sets the ELSE clause.
func (cb *CaseBuilder) Else(result types.Param) *CaseBuilder {
	cb.expr.ElseValue = &result
	return cb
}

// As adds an alias to the CASE expression.
func (cb *CaseBuilder) As(alias string) *CaseBuilder {
	if !isValidSQLIdentifier(alias) {
		panic(fmt.Errorf("invalid alias '%s': must be alphanumeric/underscore, start with letter/underscore, and contain no SQL keywords", alias))
	}
	cb.expr.Alias = alias
	return cb
}

// Build returns the CaseExpression wrapped in a FieldExpression.
func (cb *CaseBuilder) Build() types.FieldExpression {
	return types.FieldExpression{
		Case:  cb.expr,
		Alias: cb.expr.Alias,
	}
}

// Coalesce creates a COALESCE expression that returns the first non-null value.
func Coalesce(values ...types.Param) types.FieldExpression {
	if len(values) < 2 {
		panic("COALESCE requires at least 2 values")
	}
	return types.FieldExpression{
		Coalesce: &types.CoalesceExpression{Values: values},
	}
}

// NullIf creates a NULLIF expression that returns NULL if two values are equal.
func NullIf(value1, value2 types.Param) types.FieldExpression {
	return types.FieldExpression{
		NullIf: &types.NullIfExpression{
			Value1: value1,
			Value2: value2,
		},
	}
}

// Round creates a ROUND math expression.
func Round(field types.Field, precision ...types.Param) types.FieldExpression {
	expr := types.MathExpression{
		Function: types.MathRound,
		Field:    field,
	}
	if len(precision) > 0 {
		expr.Precision = &precision[0]
	}
	return types.FieldExpression{
		Math: &expr,
	}
}

// Floor creates a FLOOR math expression.
func Floor(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Math: &types.MathExpression{
			Function: types.MathFloor,
			Field:    field,
		},
	}
}

// Ceil creates a CEIL math expression.
func Ceil(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Math: &types.MathExpression{
			Function: types.MathCeil,
			Field:    field,
		},
	}
}

// Abs creates an ABS math expression.
func Abs(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Math: &types.MathExpression{
			Function: types.MathAbs,
			Field:    field,
		},
	}
}

// Power creates a POWER math expression.
func Power(field types.Field, exponent types.Param) types.FieldExpression {
	return types.FieldExpression{
		Math: &types.MathExpression{
			Function: types.MathPower,
			Field:    field,
			Exponent: &exponent,
		},
	}
}

// Sqrt creates a SQRT math expression.
func Sqrt(field types.Field) types.FieldExpression {
	return types.FieldExpression{
		Math: &types.MathExpression{
			Function: types.MathSqrt,
			Field:    field,
		},
	}
}

// As adds an alias to a field expression.
func As(expr types.FieldExpression, alias string) types.FieldExpression {
	if !isValidSQLIdentifier(alias) {
		panic(fmt.Errorf("invalid alias '%s': must be alphanumeric/underscore, start with letter/underscore, and contain no SQL keywords", alias))
	}
	expr.Alias = alias
	return expr
}
