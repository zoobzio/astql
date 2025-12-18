package astql

import (
	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/astql/pkg/postgres"
)

// defaultRenderer is the PostgreSQL renderer used for backward compatibility.
var defaultRenderer = postgres.New()

// Render converts an AST to a QueryResult with SQL and parameters.
// This uses the default PostgreSQL renderer for backward compatibility.
func Render(ast *types.AST) (*QueryResult, error) {
	return defaultRenderer.Render(ast)
}

// RenderCompound converts a CompoundQuery to a QueryResult with SQL and parameters.
// Parameters are namespaced per sub-query (q0_, q1_, etc.) to prevent collisions.
// This uses the default PostgreSQL renderer for backward compatibility.
func RenderCompound(query *types.CompoundQuery) (*QueryResult, error) {
	return defaultRenderer.RenderCompound(query)
}
