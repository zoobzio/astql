package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// Constants for subquery handling.
const (
	MaxSubqueryDepth = 3 // Prevent DoS via deep nesting
)

// Sub creates a subquery from a regular AST builder.
func Sub(builder *astql.Builder) astql.Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return astql.Subquery{AST: ast}
}

// SubPG creates a subquery from a PostgreSQL builder.
func SubPG(builder *Builder) astql.Subquery {
	ast, err := builder.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build subquery: %w", err))
	}
	return astql.Subquery{AST: ast}
}

// renderContext tracks rendering state for parameter namespacing and depth limiting.
type renderContext struct {
	usedParams    map[string]bool
	paramCallback func(astql.Param) string
	paramPrefix   string
	depth         int
}

// newRenderContext creates a new render context.
func newRenderContext(paramCallback func(astql.Param) string) *renderContext {
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
func (ctx *renderContext) addParam(param astql.Param) string {
	// Apply prefix for subqueries
	if ctx.paramPrefix != "" && param.Type == astql.ParamNamed {
		param = astql.P(ctx.paramPrefix + param.Name)
	}
	return ctx.paramCallback(param)
}
