package redis

import (
	"github.com/zoobzio/astql"
)

// Builder provides a fluent API for building Redis queries.
type Builder struct {
	*astql.Builder
	redisAst *AST
}

// Select creates a new Redis SELECT query builder.
func Select(table astql.Table) *Builder {
	builder := astql.Select(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Insert creates a new Redis INSERT query builder.
func Insert(table astql.Table) *Builder {
	builder := astql.Insert(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Update creates a new Redis UPDATE query builder.
func Update(table astql.Table) *Builder {
	builder := astql.Update(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Delete creates a new Redis DELETE query builder.
func Delete(table astql.Table) *Builder {
	builder := astql.Delete(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Count creates a new Redis COUNT query builder.
func Count(table astql.Table) *Builder {
	builder := astql.Count(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Listen creates a new Redis LISTEN query builder (SUBSCRIBE).
func Listen(table astql.Table) *Builder {
	builder := astql.Listen(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Notify creates a new Redis NOTIFY query builder (PUBLISH).
func Notify(table astql.Table, payload astql.Param) *Builder {
	builder := astql.Notify(table, payload)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// Unlisten creates a new Redis UNLISTEN query builder (UNSUBSCRIBE).
func Unlisten(table astql.Table) *Builder {
	builder := astql.Unlisten(table)
	redisAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		redisAst: redisAst,
	}
}

// WithTTL sets the TTL parameter for the operation (Redis-specific).
// The parameter should contain the TTL value in seconds.
func (b *Builder) WithTTL(ttlParam astql.Param) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.redisAst.TTLParam = &ttlParam
	return b
}

// WithScore sets the score parameter for sorted set operations (Redis-specific).
// The parameter should contain the score value.
func (b *Builder) WithScore(scoreParam astql.Param) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.redisAst.ScoreParam = &scoreParam
	return b
}

// Build returns the Redis AST or an error.
func (b *Builder) Build() (*AST, error) {
	if b.GetError() != nil {
		return nil, b.GetError()
	}

	// Sync base AST with Redis AST
	b.redisAst.QueryAST = b.GetAST()

	// Validate the Redis AST
	if err := b.redisAst.Validate(); err != nil {
		return nil, err
	}

	return b.redisAst, nil
}

// MustBuild returns the Redis AST or panics on error.
func (b *Builder) MustBuild() *AST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Override base builder methods to return Redis Builder

// Fields overrides to return Redis Builder.
func (b *Builder) Fields(fields ...astql.Field) *Builder {
	b.Builder.Fields(fields...)
	return b
}

// Where overrides to return Redis Builder.
func (b *Builder) Where(condition astql.ConditionItem) *Builder {
	b.Builder.Where(condition)
	return b
}

// WhereField overrides to return Redis Builder.
func (b *Builder) WhereField(field astql.Field, op astql.Operator, param astql.Param) *Builder {
	b.Builder.WhereField(field, op, param)
	return b
}

// Set overrides to return Redis Builder.
func (b *Builder) Set(field astql.Field, param astql.Param) *Builder {
	b.Builder.Set(field, param)
	return b
}

// Values overrides to return Redis Builder.
func (b *Builder) Values(values map[astql.Field]astql.Param) *Builder {
	b.Builder.Values(values)
	return b
}

// OrderBy overrides to return Redis Builder.
func (b *Builder) OrderBy(field astql.Field, dir astql.Direction) *Builder {
	b.Builder.OrderBy(field, dir)
	return b
}

// Limit overrides to return Redis Builder.
func (b *Builder) Limit(limit int) *Builder {
	b.Builder.Limit(limit)
	return b
}

// Offset overrides to return Redis Builder.
func (b *Builder) Offset(offset int) *Builder {
	b.Builder.Offset(offset)
	return b
}
