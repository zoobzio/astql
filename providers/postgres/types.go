package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// FieldComparison represents a comparison between two fields.
type FieldComparison struct {
	LeftField  types.Field
	Operator   types.Operator
	RightField types.Field
}

// SubqueryCondition represents a condition that uses a subquery.
type SubqueryCondition struct {
	Subquery Subquery
	Field    *types.Field
	Operator types.Operator
}

// Subquery represents a nested query.
type Subquery struct {
	AST interface{} // Can be *QueryAST or *PostgresAST
}

// Implement ConditionItem interface.
func (FieldComparison) IsConditionItem()   {}
func (SubqueryCondition) IsConditionItem() {}

// CF creates a field comparison condition.
func CF(left types.Field, op types.Operator, right types.Field) FieldComparison {
	return FieldComparison{
		LeftField:  left,
		Operator:   op,
		RightField: right,
	}
}

// CSub creates a subquery condition with a field.
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

// Sub creates a subquery from a builder.
func Sub(builder *astql.Builder) Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return Subquery{AST: ast}
}

// SubPostgres creates a subquery from a PostgreSQL builder.
func SubPostgres(builder *Builder) Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return Subquery{AST: ast}
}

// Constants for subquery handling.
const (
	MaxSubqueryDepth = 3 // Prevent DoS via deep nesting
)

// renderContext tracks rendering state for parameter namespacing and depth limiting.
type renderContext struct {
	usedParams    map[string]bool
	paramCallback func(types.Param) string
	paramPrefix   string
	depth         int
}

// newRenderContext creates a new render context.
func newRenderContext(paramCallback func(types.Param) string) *renderContext {
	return &renderContext{
		depth:         0,
		paramPrefix:   "",
		usedParams:    make(map[string]bool),
		paramCallback: paramCallback,
	}
}

// withSubquery creates a child context for rendering a subquery.
func (ctx *renderContext) withSubquery() (*renderContext, error) {
	if ctx.depth >= MaxSubqueryDepth {
		return nil, fmt.Errorf("maximum subquery depth (%d) exceeded", MaxSubqueryDepth)
	}

	return &renderContext{
		depth:         ctx.depth + 1,
		paramPrefix:   fmt.Sprintf("sq%d_", ctx.depth+1),
		usedParams:    ctx.usedParams, // Share the same map
		paramCallback: ctx.paramCallback,
	}, nil
}

// addParam adds a parameter with proper namespacing.
func (ctx *renderContext) addParam(param types.Param) string {
	// Apply prefix for subqueries
	if ctx.paramPrefix != "" {
		param = astql.P(ctx.paramPrefix + param.Name)
	}
	return ctx.paramCallback(param)
}
