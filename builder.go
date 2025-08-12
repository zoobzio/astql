package astql

import "fmt"

// Builder provides a fluent API for constructing queries.
type Builder struct {
	ast *QueryAST
	err error
}

// GetAST returns the internal AST (for use by provider packages).
func (b *Builder) GetAST() *QueryAST {
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
func Select(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpSelect,
			Target:    table,
		},
	}
}

// Insert creates a new INSERT query builder.
func Insert(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpInsert,
			Target:    table,
		},
	}
}

// Update creates a new UPDATE query builder.
func Update(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpUpdate,
			Target:    table,
			Updates:   make(map[Field]Param),
		},
	}
}

// Delete creates a new DELETE query builder.
func Delete(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpDelete,
			Target:    table,
		},
	}
}

// Count creates a new COUNT query builder.
func Count(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpCount,
			Target:    table,
		},
	}
}

// Listen creates a new LISTEN query builder.
// The channel name is derived from the table name.
func Listen(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpListen,
			Target:    table,
		},
	}
}

// Notify creates a new NOTIFY query builder.
// The channel name is derived from the table name.
func Notify(table Table, payload Param) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation:     OpNotify,
			Target:        table,
			NotifyPayload: &payload,
		},
	}
}

// Unlisten creates a new UNLISTEN query builder.
// The channel name is derived from the table name.
func Unlisten(table Table) *Builder {
	return &Builder{
		ast: &QueryAST{
			Operation: OpUnlisten,
			Target:    table,
		},
	}
}

// Fields sets the fields to select.
func (b *Builder) Fields(fields ...Field) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != OpSelect {
		b.err = fmt.Errorf("Fields() can only be used with SELECT queries")
		return b
	}
	b.ast.Fields = fields
	return b
}

// Where sets or adds conditions.
func (b *Builder) Where(condition ConditionItem) *Builder {
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
func (b *Builder) WhereField(field Field, op Operator, param Param) *Builder {
	return b.Where(C(field, op, param))
}

// Set adds a field update for UPDATE queries.
func (b *Builder) Set(field Field, param Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != OpUpdate {
		b.err = fmt.Errorf("Set() can only be used with UPDATE queries")
		return b
	}
	b.ast.Updates[field] = param
	return b
}

// Values adds a value set for INSERT queries.
func (b *Builder) Values(values map[Field]Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != OpInsert {
		b.err = fmt.Errorf("Values() can only be used with INSERT queries")
		return b
	}
	if b.ast.Values == nil {
		b.ast.Values = []map[Field]Param{}
	}
	b.ast.Values = append(b.ast.Values, values)
	return b
}

// OrderBy adds ordering.
func (b *Builder) OrderBy(field Field, direction Direction) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Ordering == nil {
		b.ast.Ordering = []OrderBy{}
	}
	b.ast.Ordering = append(b.ast.Ordering, OrderBy{
		Field:     field,
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
func (b *Builder) Build() (*QueryAST, error) {
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
func (b *Builder) MustBuild() *QueryAST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Validate performs basic validation on the AST.
func (ast *QueryAST) Validate() error {
	if ast.Target.Name == "" {
		return fmt.Errorf("target table is required")
	}

	switch ast.Operation {
	case OpSelect:
		// Fields are optional (defaults to *)
	case OpInsert:
		if len(ast.Values) == 0 {
			return fmt.Errorf("INSERT requires at least one value set")
		}
		// Ensure all value sets have the same fields
		if len(ast.Values) > 1 {
			firstKeys := make(map[Field]bool)
			for k := range ast.Values[0] {
				firstKeys[k] = true
			}
			for i, valueSet := range ast.Values[1:] {
				if len(valueSet) != len(firstKeys) {
					return fmt.Errorf("value set %d has different number of fields", i+1)
				}
				for k := range valueSet {
					if !firstKeys[k] {
						return fmt.Errorf("value set %d has different fields", i+1)
					}
				}
			}
		}
	case OpUpdate:
		if len(ast.Updates) == 0 {
			return fmt.Errorf("UPDATE requires at least one field to update")
		}
	case OpDelete:
		// No additional validation needed
	case OpCount:
		// No additional validation needed - COUNT can have WHERE but no fields
	case OpListen, OpUnlisten:
		// No additional validation needed - just need table name
	case OpNotify:
		if ast.NotifyPayload == nil {
			return fmt.Errorf("NOTIFY requires a payload parameter")
		}
	default:
		return fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	return nil
}
