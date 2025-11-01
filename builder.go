package astql

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// and creates an AND condition group (internal helper for builder).
func and(conditions ...types.ConditionItem) types.ConditionGroup {
	return types.ConditionGroup{
		Logic:      types.AND,
		Conditions: conditions,
	}
}

// c creates a simple condition (internal helper for builder).
func c(f types.Field, op types.Operator, p types.Param) types.Condition {
	return types.Condition{
		Field:    f,
		Operator: op,
		Value:    p,
	}
}

// Builder provides a fluent API for constructing queries.
type Builder struct {
	ast *types.AST
	err error
}

// GetAST returns the internal AST.
func (b *Builder) GetAST() *types.AST {
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
		ast: &types.AST{
			Operation: types.OpSelect,
			Target:    t,
		},
	}
}

// Insert creates a new INSERT query builder.
func Insert(t types.Table) *Builder {
	return &Builder{
		ast: &types.AST{
			Operation: types.OpInsert,
			Target:    t,
		},
	}
}

// Update creates a new UPDATE query builder.
func Update(t types.Table) *Builder {
	return &Builder{
		ast: &types.AST{
			Operation: types.OpUpdate,
			Target:    t,
			Updates:   make(map[types.Field]types.Param),
		},
	}
}

// Delete creates a new DELETE query builder.
func Delete(t types.Table) *Builder {
	return &Builder{
		ast: &types.AST{
			Operation: types.OpDelete,
			Target:    t,
		},
	}
}

// Count creates a new COUNT query builder.
func Count(t types.Table) *Builder {
	return &Builder{
		ast: &types.AST{
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
		b.ast.WhereClause = and(b.ast.WhereClause, condition)
	}

	return b
}

// WhereField is a convenience method for simple field conditions.
func (b *Builder) WhereField(f types.Field, op types.Operator, p types.Param) *Builder {
	return b.Where(c(f, op, p))
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

// Value adds a single field-value pair for INSERT queries.
// Multiple calls to Value() build up a single row to insert.
// Call NextRow() to finalize the current row and start a new one.
func (b *Builder) Value(f types.Field, p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpInsert {
		b.err = fmt.Errorf("Value() can only be used with INSERT queries")
		return b
	}
	if b.ast.Values == nil {
		b.ast.Values = []map[types.Field]types.Param{}
	}
	// If there are no value sets yet, create the first one
	if len(b.ast.Values) == 0 {
		b.ast.Values = append(b.ast.Values, make(map[types.Field]types.Param))
	}
	// Add to the last value set
	lastIdx := len(b.ast.Values) - 1
	b.ast.Values[lastIdx][f] = p
	return b
}

// NextRow finalizes the current row and starts a new one for INSERT queries.
func (b *Builder) NextRow() *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpInsert {
		b.err = fmt.Errorf("NextRow() can only be used with INSERT queries")
		return b
	}
	if b.ast.Values == nil {
		b.ast.Values = []map[types.Field]types.Param{}
	}
	// Add a new empty map for the next row
	b.ast.Values = append(b.ast.Values, make(map[types.Field]types.Param))
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
func (b *Builder) Build() (*types.AST, error) {
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
func (b *Builder) MustBuild() *types.AST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Render builds the AST and renders it to SQL.
func (b *Builder) Render() (*QueryResult, error) {
	ast, err := b.Build()
	if err != nil {
		return nil, err
	}
	return Render(ast)
}

// MustRender builds and renders the AST or panics on error.
func (b *Builder) MustRender() *QueryResult {
	result, err := b.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Distinct sets the DISTINCT flag for SELECT queries.
func (b *Builder) Distinct() *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect {
		b.err = fmt.Errorf("DISTINCT can only be used with SELECT queries")
		return b
	}
	b.ast.Distinct = true
	return b
}

// Join adds an INNER JOIN.
func (b *Builder) Join(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(types.InnerJoin, table, on)
}

// InnerJoin adds an INNER JOIN.
func (b *Builder) InnerJoin(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(types.InnerJoin, table, on)
}

// LeftJoin adds a LEFT JOIN.
func (b *Builder) LeftJoin(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(types.LeftJoin, table, on)
}

// RightJoin adds a RIGHT JOIN.
func (b *Builder) RightJoin(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(types.RightJoin, table, on)
}

// CrossJoin adds a CROSS JOIN (no ON clause needed).
func (b *Builder) CrossJoin(table types.Table) *Builder {
	return b.addJoin(types.CrossJoin, table, nil)
}

// addJoin is a helper to add joins.
func (b *Builder) addJoin(joinType types.JoinType, table types.Table, on types.ConditionItem) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect && b.ast.Operation != types.OpCount {
		b.err = fmt.Errorf("JOIN can only be used with SELECT or COUNT queries")
		return b
	}
	if joinType == types.CrossJoin && on != nil {
		b.err = fmt.Errorf("CROSS JOIN cannot have ON clause")
		return b
	}
	if joinType != types.CrossJoin && on == nil {
		b.err = fmt.Errorf("%s requires ON clause", joinType)
		return b
	}

	join := types.Join{
		Type:  joinType,
		Table: table,
		On:    on,
	}

	b.ast.Joins = append(b.ast.Joins, join)
	return b
}

// GroupBy adds GROUP BY fields.
func (b *Builder) GroupBy(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect {
		b.err = fmt.Errorf("GROUP BY can only be used with SELECT queries")
		return b
	}
	b.ast.GroupBy = append(b.ast.GroupBy, fields...)
	return b
}

// Having adds HAVING conditions.
func (b *Builder) Having(conditions ...types.Condition) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect {
		b.err = fmt.Errorf("HAVING can only be used with SELECT queries")
		return b
	}
	if len(b.ast.GroupBy) == 0 {
		b.err = fmt.Errorf("HAVING requires GROUP BY")
		return b
	}
	b.ast.Having = append(b.ast.Having, conditions...)
	return b
}

// Returning adds RETURNING fields for INSERT/UPDATE/DELETE.
func (b *Builder) Returning(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	switch b.ast.Operation {
	case types.OpInsert, types.OpUpdate, types.OpDelete:
		b.ast.Returning = append(b.ast.Returning, fields...)
	default:
		b.err = fmt.Errorf("RETURNING can only be used with INSERT, UPDATE, or DELETE")
	}
	return b
}

// OnConflict adds ON CONFLICT clause for INSERT.
func (b *Builder) OnConflict(columns ...types.Field) *ConflictBuilder {
	if b.err != nil {
		return &ConflictBuilder{builder: b, err: b.err}
	}
	if b.ast.Operation != types.OpInsert {
		err := fmt.Errorf("ON CONFLICT can only be used with INSERT")
		b.err = err
		return &ConflictBuilder{builder: b, err: err}
	}

	b.ast.OnConflict = &types.ConflictClause{
		Columns: columns,
	}

	return &ConflictBuilder{builder: b}
}

// ConflictBuilder handles ON CONFLICT actions.
type ConflictBuilder struct {
	builder *Builder
	err     error
}

// DoNothing sets the conflict action to DO NOTHING.
func (cb *ConflictBuilder) DoNothing() *Builder {
	if cb.err != nil {
		return cb.builder
	}
	cb.builder.ast.OnConflict.Action = types.DoNothing
	return cb.builder
}

// DoUpdate sets the conflict action to DO UPDATE.
func (cb *ConflictBuilder) DoUpdate() *UpdateBuilder {
	if cb.err != nil {
		return &UpdateBuilder{builder: cb.builder, err: cb.err}
	}
	cb.builder.ast.OnConflict.Action = types.DoUpdate
	cb.builder.ast.OnConflict.Updates = make(map[types.Field]types.Param)
	return &UpdateBuilder{
		builder: cb.builder,
		updates: cb.builder.ast.OnConflict.Updates,
	}
}

// UpdateBuilder handles DO UPDATE SET clause construction.
type UpdateBuilder struct {
	builder *Builder
	updates map[types.Field]types.Param
	err     error
}

// Set adds a field to update on conflict.
func (ub *UpdateBuilder) Set(field types.Field, param types.Param) *UpdateBuilder {
	if ub.err != nil {
		return ub
	}
	ub.updates[field] = param
	return ub
}

// Build finalizes the update and returns the builder.
func (ub *UpdateBuilder) Build() *Builder {
	return ub.builder
}

// SelectExpr adds a field expression (aggregate, case, etc) to SELECT.
func (b *Builder) SelectExpr(expr types.FieldExpression) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSelect {
		b.err = fmt.Errorf("SelectExpr can only be used with SELECT queries")
		return b
	}
	b.ast.FieldExpressions = append(b.ast.FieldExpressions, expr)
	return b
}
