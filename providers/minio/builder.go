package minio

import (
	"github.com/zoobzio/astql"
)

// Builder extends the base Builder with MinIO-specific methods.
type Builder struct {
	*astql.Builder
	minioAst *AST
}

// Build creates the final AST.
func (b *Builder) Build() (*AST, error) {
	if b.GetError() != nil {
		return nil, b.GetError()
	}

	baseAST := b.GetAST()
	b.minioAst.QueryAST = baseAST
	return b.minioAst, nil
}

// WithContent sets the content parameter for object upload operations.
// The parameter should contain the object data.
func (b *Builder) WithContent(contentParam astql.Param) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.minioAst.ContentParam = &contentParam
	return b
}

// WithEventTypes sets the event types for LISTEN operations.
// Only pre-defined EventType constants are accepted.
func (b *Builder) WithEventTypes(events ...EventType) *Builder {
	if b.GetError() != nil {
		return b
	}
	b.minioAst.EventTypes = events
	return b
}

// Select creates a SELECT query for listing objects.
func Select(table astql.Table) *Builder {
	builder := astql.Select(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Insert creates an INSERT query for uploading objects.
func Insert(table astql.Table) *Builder {
	builder := astql.Insert(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Update creates an UPDATE query for replacing objects.
func Update(table astql.Table) *Builder {
	builder := astql.Update(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Delete creates a DELETE query for removing objects.
func Delete(table astql.Table) *Builder {
	builder := astql.Delete(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Count creates a COUNT query for counting objects.
func Count(table astql.Table) *Builder {
	builder := astql.Count(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Listen creates a LISTEN query for bucket notifications.
func Listen(table astql.Table) *Builder {
	builder := astql.Listen(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Unlisten creates an UNLISTEN query to stop notifications.
func Unlisten(table astql.Table) *Builder {
	builder := astql.Unlisten(table)
	minioAst := NewAST(builder.GetAST())
	return &Builder{
		Builder:  builder,
		minioAst: minioAst,
	}
}

// Values overrides to return MinIO Builder.
func (b *Builder) Values(values map[astql.Field]astql.Param) *Builder {
	b.Builder.Values(values)
	return b
}

// Where overrides to return MinIO Builder.
func (b *Builder) Where(condition astql.ConditionItem) *Builder {
	b.Builder.Where(condition)
	return b
}

// Set overrides to return MinIO Builder.
func (b *Builder) Set(field astql.Field, value astql.Param) *Builder {
	b.Builder.Set(field, value)
	return b
}

// Fields overrides to return MinIO Builder.
func (b *Builder) Fields(fields ...astql.Field) *Builder {
	b.Builder.Fields(fields...)
	return b
}

// Limit overrides to return MinIO Builder.
func (b *Builder) Limit(limit int) *Builder {
	b.Builder.Limit(limit)
	return b
}

// Offset overrides to return MinIO Builder.
func (b *Builder) Offset(offset int) *Builder {
	b.Builder.Offset(offset)
	return b
}
