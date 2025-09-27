package astql

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// Builder provides a fluent API for constructing queries.
type Builder struct {
	ast *types.QueryAST
	err error
}

// GetAST returns the internal AST (for use by provider packages).
func (b *Builder) GetAST() *types.QueryAST {
	return b.ast
}

// GetError returns the internal error (for use by provider packages).
func (b *Builder) GetError() error {
	return b.err
}

// SetError sets the internal error (for use by provider packages).
func (b *Builder) SetError(err error) {
	b.err = err
}

// Select creates a new SELECT query builder.
func Select(t types.Table) *Builder {
	return &Builder{
		ast: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    t,
		},
	}
}

// Insert creates a new INSERT query builder.
func Insert(t types.Table) *Builder {
	return &Builder{
		ast: &types.QueryAST{
			Operation: types.OpInsert,
			Target:    t,
		},
	}
}

// Update creates a new UPDATE query builder.
func Update(t types.Table) *Builder {
	return &Builder{
		ast: &types.QueryAST{
			Operation: types.OpUpdate,
			Target:    t,
			Updates:   make(map[types.Field]types.Param),
		},
	}
}

// Delete creates a new DELETE query builder.
func Delete(t types.Table) *Builder {
	return &Builder{
		ast: &types.QueryAST{
			Operation: types.OpDelete,
			Target:    t,
		},
	}
}

// Count creates a new COUNT query builder.
func Count(t types.Table) *Builder {
	return &Builder{
		ast: &types.QueryAST{
			Operation: types.OpCount,
			Target:    t,
		},
	}
}

// Fields sets the fields to select.
func (b *Builder) Fields(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect {
		b.err = fmt.Errorf("Fields() can only be used with SELECT queries")
		return b
	}
	b.ast.Fields = fields
	return b
}

// Where sets or adds conditions.
func (b *Builder) Where(condition types.ConditionItem) *Builder {
	if b.err != nil {
		return b
	}

	if b.ast.WhereClause == nil {
		b.ast.WhereClause = condition
	} else {
		// If there's already a where clause, combine with AND
		b.ast.WhereClause = And(b.ast.WhereClause, condition)
	}

	return b
}

// WhereField is a convenience method for simple field conditions.
func (b *Builder) WhereField(f types.Field, op types.Operator, p types.Param) *Builder {
	return b.Where(C(f, op, p))
}

// Set adds a field update for UPDATE queries.
func (b *Builder) Set(f types.Field, p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpUpdate {
		b.err = fmt.Errorf("Set() can only be used with UPDATE queries")
		return b
	}
	if b.ast.Updates == nil {
		b.ast.Updates = make(map[types.Field]types.Param)
	}
	b.ast.Updates[f] = p
	return b
}

// Values adds a value set for INSERT queries.
func (b *Builder) Values(values map[types.Field]types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpInsert {
		b.err = fmt.Errorf("Values() can only be used with INSERT queries")
		return b
	}
	if b.ast.Values == nil {
		b.ast.Values = []map[types.Field]types.Param{}
	}
	b.ast.Values = append(b.ast.Values, values)
	return b
}

// OrderBy adds ordering.
func (b *Builder) OrderBy(f types.Field, direction types.Direction) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Ordering == nil {
		b.ast.Ordering = []types.OrderBy{}
	}
	b.ast.Ordering = append(b.ast.Ordering, types.OrderBy{
		Field:     f,
		Direction: direction,
	})
	return b
}

// Limit sets the limit.
func (b *Builder) Limit(limit int) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.Limit = &limit
	return b
}

// Offset sets the offset.
func (b *Builder) Offset(offset int) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.Offset = &offset
	return b
}

// Build returns the constructed AST or an error.
func (b *Builder) Build() (*types.QueryAST, error) {
	if b.err != nil {
		return nil, b.err
	}

	// Validate the AST
	if err := b.ast.Validate(); err != nil {
		return nil, err
	}

	return b.ast, nil
}

// MustBuild returns the AST or panics on error.
func (b *Builder) MustBuild() *types.QueryAST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}
