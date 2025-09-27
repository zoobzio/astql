package sqlite

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// Builder provides a fluent API for constructing SQLite queries.
type Builder struct {
	*astql.Builder
	sqliteAst *AST
}

// Select creates a new SELECT query builder for SQLite.
func Select(t types.Table) *Builder {
	base := astql.Select(t)
	return &Builder{
		Builder: base,
		sqliteAst: &AST{
			QueryAST: base.GetAST(),
		},
	}
}

// Insert creates a new INSERT query builder for SQLite.
func Insert(t types.Table) *Builder {
	base := astql.Insert(t)
	return &Builder{
		Builder: base,
		sqliteAst: &AST{
			QueryAST: base.GetAST(),
		},
	}
}

// Update creates a new UPDATE query builder for SQLite.
func Update(t types.Table) *Builder {
	base := astql.Update(t)
	return &Builder{
		Builder: base,
		sqliteAst: &AST{
			QueryAST: base.GetAST(),
		},
	}
}

// Delete creates a new DELETE query builder for SQLite.
func Delete(t types.Table) *Builder {
	base := astql.Delete(t)
	return &Builder{
		Builder: base,
		sqliteAst: &AST{
			QueryAST: base.GetAST(),
		},
	}
}

// Count creates a new COUNT query builder for SQLite.
func Count(t types.Table) *Builder {
	base := astql.Count(t)
	return &Builder{
		Builder: base,
		sqliteAst: &AST{
			QueryAST: base.GetAST(),
		},
	}
}

// Distinct adds DISTINCT to SELECT.
func (b *Builder) Distinct() *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpSelect {
		b.SetError(fmt.Errorf("DISTINCT can only be used with SELECT"))
		return b
	}
	b.sqliteAst.Distinct = true
	return b
}

// Join adds a generic JOIN with specified type.
func (b *Builder) Join(joinType string, table types.Table, on types.ConditionItem) *Builder {
	// Map string join type to JoinType
	var jt JoinType
	switch strings.ToUpper(joinType) {
	case "INNER":
		jt = InnerJoin
	case "LEFT":
		jt = LeftJoin
	case "RIGHT":
		// SQLite doesn't support RIGHT JOIN
		b.SetError(fmt.Errorf("SQLite does not support RIGHT JOIN"))
		return b
	case "CROSS":
		jt = CrossJoin
	default:
		b.SetError(fmt.Errorf("unsupported join type: %s", joinType))
		return b
	}
	return b.addJoin(jt, table, on)
}

// InnerJoin adds an INNER JOIN.
func (b *Builder) InnerJoin(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(InnerJoin, table, on)
}

// LeftJoin adds a LEFT JOIN.
func (b *Builder) LeftJoin(table types.Table, on types.ConditionItem) *Builder {
	return b.addJoin(LeftJoin, table, on)
}

// CrossJoin adds a CROSS JOIN (no ON clause).
func (b *Builder) CrossJoin(table types.Table) *Builder {
	return b.addJoin(CrossJoin, table, nil)
}

// addJoin is a helper to add joins.
func (b *Builder) addJoin(joinType JoinType, table types.Table, on types.ConditionItem) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpSelect && b.sqliteAst.Operation != types.OpCount {
		b.SetError(fmt.Errorf("JOINs can only be used with SELECT or COUNT"))
		return b
	}
	if joinType != CrossJoin && on == nil {
		b.SetError(fmt.Errorf("%s requires ON clause", joinType))
		return b
	}
	if joinType == CrossJoin && on != nil {
		b.SetError(fmt.Errorf("CROSS JOIN cannot have ON clause"))
		return b
	}

	b.sqliteAst.Joins = append(b.sqliteAst.Joins, Join{
		Type:  joinType,
		Table: table,
		On:    on,
	})
	return b
}

// GroupBy adds GROUP BY fields (for aggregations).
func (b *Builder) GroupBy(fields ...types.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpSelect {
		b.SetError(fmt.Errorf("GROUP BY can only be used with SELECT queries"))
		return b
	}
	b.sqliteAst.GroupBy = append(b.sqliteAst.GroupBy, fields...)
	return b
}

// Having adds HAVING conditions (requires GROUP BY).
func (b *Builder) Having(conditions ...types.Condition) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpSelect {
		b.SetError(fmt.Errorf("HAVING can only be used with SELECT queries"))
		return b
	}
	if len(b.sqliteAst.GroupBy) == 0 {
		b.SetError(fmt.Errorf("HAVING requires GROUP BY"))
		return b
	}
	b.sqliteAst.Having = append(b.sqliteAst.Having, conditions...)
	return b
}

// OnConflict adds ON CONFLICT clause for UPSERT operations.
func (b *Builder) OnConflict(columns ...types.Field) *ConflictBuilder {
	if b.GetError() != nil {
		return &ConflictBuilder{builder: b, err: b.GetError()}
	}
	if b.sqliteAst.Operation != types.OpInsert {
		err := fmt.Errorf("ON CONFLICT can only be used with INSERT")
		b.SetError(err)
		return &ConflictBuilder{builder: b, err: err}
	}

	return &ConflictBuilder{
		builder: b,
		clause: &ConflictClause{
			Columns: columns,
		},
	}
}

// ConflictBuilder handles ON CONFLICT clause construction.
type ConflictBuilder struct {
	builder *Builder
	clause  *ConflictClause
	err     error
}

// DoNothing sets the conflict action to DO NOTHING.
func (cb *ConflictBuilder) DoNothing() *Builder {
	if cb.err != nil {
		return cb.builder
	}
	cb.clause.Action = ConflictDoNothing
	cb.builder.sqliteAst.OnConflict = cb.clause
	return cb.builder
}

// DoUpdate sets the conflict action to DO UPDATE.
func (cb *ConflictBuilder) DoUpdate() *UpdateBuilder {
	if cb.err != nil {
		return &UpdateBuilder{builder: cb.builder, err: cb.err}
	}
	cb.clause.Action = ConflictDoUpdate
	cb.clause.Updates = make(map[types.Field]types.Param)
	return &UpdateBuilder{
		builder: cb.builder,
		clause:  cb.clause,
	}
}

// UpdateBuilder handles DO UPDATE SET clause construction.
type UpdateBuilder struct {
	builder *Builder
	clause  *ConflictClause
	err     error
}

// Set adds a field to update on conflict.
func (ub *UpdateBuilder) Set(field types.Field, param types.Param) *UpdateBuilder {
	if ub.err != nil {
		return ub
	}
	ub.clause.Updates[field] = param
	return ub
}

// Build finalizes the ON CONFLICT DO UPDATE clause.
func (ub *UpdateBuilder) Build() *Builder {
	if ub.err != nil {
		return ub.builder
	}
	ub.builder.sqliteAst.OnConflict = ub.clause
	return ub.builder
}

// Fields adds fields to SELECT.
func (b *Builder) Fields(fields ...types.Field) *Builder {
	b.Builder.Fields(fields...)
	return b
}

// Where adds WHERE conditions.
func (b *Builder) Where(cond types.ConditionItem) *Builder {
	b.Builder.Where(cond)
	return b
}

// OrderBy adds ORDER BY clause.
func (b *Builder) OrderBy(field types.Field, direction types.Direction) *Builder {
	b.Builder.OrderBy(field, direction)
	return b
}

// Limit adds LIMIT clause.
func (b *Builder) Limit(limit int) *Builder {
	b.Builder.Limit(limit)
	return b
}

// Values adds values for INSERT.
func (b *Builder) Values(values map[types.Field]types.Param) *Builder {
	b.Builder.Values(values)
	return b
}

// Set adds a field to update (for UPDATE queries).
func (b *Builder) Set(field types.Field, param types.Param) *Builder {
	// Delegate to the base builder
	b.Builder.Set(field, param)
	return b
}

// Build validates and returns the final AST.
func (b *Builder) Build() (*AST, error) {
	if b.GetError() != nil {
		return nil, b.GetError()
	}

	// Validate the AST
	if err := b.sqliteAst.Validate(); err != nil {
		return nil, err
	}

	return b.sqliteAst, nil
}

// Returning adds RETURNING clause (SQLite 3.35.0+).
func (b *Builder) Returning(fields ...types.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpInsert &&
		b.sqliteAst.Operation != types.OpUpdate &&
		b.sqliteAst.Operation != types.OpDelete {
		b.SetError(fmt.Errorf("RETURNING can only be used with INSERT, UPDATE, or DELETE"))
		return b
	}
	b.sqliteAst.Returning = append(b.sqliteAst.Returning, fields...)
	return b
}

// FieldExpressions adds field expressions to SELECT.
func (b *Builder) FieldExpressions(exprs ...FieldExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.sqliteAst.Operation != types.OpSelect {
		b.SetError(fmt.Errorf("Field expressions can only be used with SELECT"))
		return b
	}
	b.sqliteAst.FieldExpressions = append(b.sqliteAst.FieldExpressions, exprs...)
	return b
}
