package redis

import (
	"github.com/zoobzio/astql"
)

// Type represents the data type in Redis.
type Type string

// Redis data types.
const (
	TypeString Type = "STRING"
	TypeHash   Type = "HASH"
	TypeList   Type = "LIST"
	TypeSet    Type = "SET"
	TypeZSet   Type = "ZSET"
	TypeStream Type = "STREAM"
)

// TableConfig defines how an ASTQL table maps to Redis keys.
type TableConfig struct {
	// KeyPattern defines how to construct Redis keys.
	// Use {field} placeholders, e.g., "users:{id}" or "session:{session_id}"
	KeyPattern string

	// DataType specifies the Redis data type for this table.
	DataType Type

	// IDField specifies which field contains the key identifier.
	IDField string

	// Reserved for future use - TTL and Score are now handled via builder methods
}

// AST extends the base QueryAST with Redis-specific features.
type AST struct {
	*astql.QueryAST

	// Redis-specific fields
	TableConfig *TableConfig
	TTLParam    *astql.Param // Parameter containing TTL value
	ScoreParam  *astql.Param // Parameter containing score value
}

// NewAST creates a Redis AST from a base QueryAST.
func NewAST(baseAST *astql.QueryAST) *AST {
	return &AST{
		QueryAST: baseAST,
	}
}

// Validate checks if the AST is valid for Redis operations.
func (ast *AST) Validate() error {
	// First validate the base AST
	if err := ast.QueryAST.Validate(); err != nil {
		return err
	}

	// Add Redis-specific validation here
	// For example, check if operations are supported for the data type

	return nil
}
