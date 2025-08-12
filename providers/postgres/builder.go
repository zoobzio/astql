package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// Builder provides a fluent API for building PostgreSQL queries.
type Builder struct {
	*astql.Builder
	pgAst *AST
}

// Select creates a new PostgreSQL SELECT query builder.
func Select(table astql.Table) *Builder {
	builder := astql.Select(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Insert creates a new PostgreSQL INSERT query builder.
func Insert(table astql.Table) *Builder {
	builder := astql.Insert(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Update creates a new PostgreSQL UPDATE query builder.
func Update(table astql.Table) *Builder {
	builder := astql.Update(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Delete creates a new PostgreSQL DELETE query builder.
func Delete(table astql.Table) *Builder {
	builder := astql.Delete(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Count creates a new PostgreSQL COUNT query builder.
func Count(table astql.Table) *Builder {
	builder := astql.Count(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Listen creates a new PostgreSQL LISTEN query builder.
func Listen(table astql.Table) *Builder {
	builder := astql.Listen(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Notify creates a new PostgreSQL NOTIFY query builder.
func Notify(table astql.Table, payload astql.Param) *Builder {
	builder := astql.Notify(table, payload)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Unlisten creates a new PostgreSQL UNLISTEN query builder.
func Unlisten(table astql.Table) *Builder {
	builder := astql.Unlisten(table)
	pgAst := NewAST(builder.GetAST())
	return &Builder{
		Builder: builder,
		pgAst:   pgAst,
	}
}

// Distinct sets the DISTINCT flag for SELECT queries.
func (b *Builder) Distinct() *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("DISTINCT can only be used with SELECT queries"))
		return b
	}
	b.pgAst.Distinct = true
	return b
}

// Join adds an INNER JOIN.
func (b *Builder) Join(table astql.Table, on astql.ConditionItem) *Builder {
	return b.addJoin(InnerJoin, table, on)
}

// InnerJoin adds an INNER JOIN.
func (b *Builder) InnerJoin(table astql.Table, on astql.ConditionItem) *Builder {
	return b.addJoin(InnerJoin, table, on)
}

// LeftJoin adds a LEFT JOIN.
func (b *Builder) LeftJoin(table astql.Table, on astql.ConditionItem) *Builder {
	return b.addJoin(LeftJoin, table, on)
}

// RightJoin adds a RIGHT JOIN.
func (b *Builder) RightJoin(table astql.Table, on astql.ConditionItem) *Builder {
	return b.addJoin(RightJoin, table, on)
}

// addJoin is a helper to add joins.
func (b *Builder) addJoin(joinType JoinType, table astql.Table, on astql.ConditionItem) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect && b.pgAst.Operation != astql.OpCount {
		b.SetError(fmt.Errorf("JOIN can only be used with SELECT or COUNT queries"))
		return b
	}

	join := Join{
		Type:  joinType,
		Table: table,
		On:    on,
	}

	b.pgAst.Joins = append(b.pgAst.Joins, join)
	return b
}

// GroupBy adds GROUP BY fields.
func (b *Builder) GroupBy(fields ...astql.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("GROUP BY can only be used with SELECT queries"))
		return b
	}
	b.pgAst.GroupBy = append(b.pgAst.GroupBy, fields...)
	return b
}

// Having adds HAVING conditions.
func (b *Builder) Having(conditions ...astql.Condition) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("HAVING can only be used with SELECT queries"))
		return b
	}
	if len(b.pgAst.GroupBy) == 0 {
		b.SetError(fmt.Errorf("HAVING requires GROUP BY"))
		return b
	}
	b.pgAst.Having = append(b.pgAst.Having, conditions...)
	return b
}

// Returning adds RETURNING fields for INSERT/UPDATE/DELETE.
func (b *Builder) Returning(fields ...astql.Field) *Builder {
	if b.GetError() != nil {
		return b
	}
	switch b.pgAst.Operation {
	case astql.OpInsert, astql.OpUpdate, astql.OpDelete:
		b.pgAst.Returning = append(b.pgAst.Returning, fields...)
	default:
		b.SetError(fmt.Errorf("RETURNING can only be used with INSERT, UPDATE, or DELETE"))
	}
	return b
}

// OnConflict adds ON CONFLICT clause for INSERT.
func (b *Builder) OnConflict(columns ...astql.Field) *ConflictBuilder {
	if b.GetError() != nil {
		return &ConflictBuilder{builder: b, err: b.GetError()}
	}
	if b.pgAst.Operation != astql.OpInsert {
		err := fmt.Errorf("ON CONFLICT can only be used with INSERT")
		b.SetError(err)
		return &ConflictBuilder{builder: b, err: err}
	}

	b.pgAst.OnConflict = &ConflictClause{
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
	cb.builder.pgAst.OnConflict.Action = DoNothing
	return cb.builder
}

// DoUpdate sets the conflict action to DO UPDATE.
func (cb *ConflictBuilder) DoUpdate(updates map[astql.Field]astql.Param) *Builder {
	if cb.err != nil {
		return cb.builder
	}
	cb.builder.pgAst.OnConflict.Action = DoUpdate
	cb.builder.pgAst.OnConflict.Updates = updates
	return cb.builder
}

// SelectExpr adds a field expression (aggregate, case, etc) to SELECT.
func (b *Builder) SelectExpr(expr FieldExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("SelectExpr can only be used with SELECT queries"))
		return b
	}
	b.pgAst.FieldExpressions = append(b.pgAst.FieldExpressions, expr)
	return b
}

// SelectCoalesce adds a COALESCE expression to SELECT.
func (b *Builder) SelectCoalesce(expr CoalesceExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("SelectCoalesce can only be used with SELECT queries"))
		return b
	}

	fieldExpr := FieldExpression{
		Coalesce: &expr,
		Alias:    expr.Alias,
	}

	b.pgAst.FieldExpressions = append(b.pgAst.FieldExpressions, fieldExpr)
	return b
}

// SelectNullIf adds a NULLIF expression to SELECT.
func (b *Builder) SelectNullIf(expr NullIfExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("SelectNullIf can only be used with SELECT queries"))
		return b
	}

	fieldExpr := FieldExpression{
		NullIf: &expr,
		Alias:  expr.Alias,
	}

	b.pgAst.FieldExpressions = append(b.pgAst.FieldExpressions, fieldExpr)
	return b
}

// SelectCase adds a CASE expression to SELECT.
func (b *Builder) SelectCase(expr CaseExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("SelectCase can only be used with SELECT queries"))
		return b
	}

	// If the CASE expression already has an alias from As(), use it
	// Otherwise, the alias will be empty
	fieldExpr := FieldExpression{
		Case:  &expr,
		Alias: expr.Alias,
	}

	b.pgAst.FieldExpressions = append(b.pgAst.FieldExpressions, fieldExpr)
	return b
}

// SelectMath adds a math expression to SELECT.
func (b *Builder) SelectMath(expr MathExpression) *Builder {
	if b.GetError() != nil {
		return b
	}
	if b.pgAst.Operation != astql.OpSelect {
		b.SetError(fmt.Errorf("SelectMath can only be used with SELECT queries"))
		return b
	}

	fieldExpr := FieldExpression{
		Math:  &expr,
		Alias: expr.Alias,
	}

	b.pgAst.FieldExpressions = append(b.pgAst.FieldExpressions, fieldExpr)
	return b
}

// Build returns the PostgreSQL AST or an error.
func (b *Builder) Build() (*AST, error) {
	if b.GetError() != nil {
		return nil, b.GetError()
	}

	// Sync base AST with PostgreSQL AST
	b.pgAst.QueryAST = b.GetAST()

	// Validate the PostgreSQL AST
	if err := b.pgAst.Validate(); err != nil {
		return nil, err
	}

	return b.pgAst, nil
}

// MustBuild returns the PostgreSQL AST or panics on error.
func (b *Builder) MustBuild() *AST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Override base builder methods to return PostgresBuilder

// Fields overrides to return PostgresBuilder.
func (b *Builder) Fields(fields ...astql.Field) *Builder {
	b.Builder.Fields(fields...)
	return b
}

// Where overrides to return PostgresBuilder.
func (b *Builder) Where(condition astql.ConditionItem) *Builder {
	b.Builder.Where(condition)
	return b
}

// WhereField overrides to return PostgresBuilder.
func (b *Builder) WhereField(field astql.Field, op astql.Operator, param astql.Param) *Builder {
	b.Builder.WhereField(field, op, param)
	return b
}

// Set overrides to return PostgresBuilder.
func (b *Builder) Set(field astql.Field, param astql.Param) *Builder {
	b.Builder.Set(field, param)
	return b
}

// Values overrides to return PostgresBuilder.
func (b *Builder) Values(values map[astql.Field]astql.Param) *Builder {
	b.Builder.Values(values)
	return b
}

// OrderBy overrides to return PostgresBuilder.
func (b *Builder) OrderBy(field astql.Field, dir astql.Direction) *Builder {
	b.Builder.OrderBy(field, dir)
	return b
}

// Limit overrides to return PostgresBuilder.
func (b *Builder) Limit(limit int) *Builder {
	b.Builder.Limit(limit)
	return b
}

// Offset overrides to return PostgresBuilder.
func (b *Builder) Offset(offset int) *Builder {
	b.Builder.Offset(offset)
	return b
}

// Helper functions for creating field expressions

// Sum creates a SUM aggregate expression.
func Sum(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggSum,
	}
}

// Avg creates an AVG aggregate expression.
func Avg(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggAvg,
	}
}

// Min creates a MIN aggregate expression.
func Min(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggMin,
	}
}

// Max creates a MAX aggregate expression.
func Max(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggMax,
	}
}

// CountField creates a COUNT aggregate expression for a specific field.
func CountField(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggCountField,
	}
}

// CountDistinct creates a COUNT(DISTINCT) aggregate expression.
func CountDistinct(field astql.Field) FieldExpression {
	return FieldExpression{
		Field:     field,
		Aggregate: AggCountDistinct,
	}
}

// As adds an alias to a field expression.
func (expr FieldExpression) As(alias string) FieldExpression {
	// Validate alias against registered field aliases
	if err := astql.ValidateFieldAlias(alias); err != nil {
		panic(fmt.Errorf("invalid field alias: %w", err))
	}
	expr.Alias = alias
	return expr
}
