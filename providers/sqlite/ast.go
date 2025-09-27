package sqlite

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// AggregateFunc represents an aggregate function.
type AggregateFunc string

const (
	AggSum           AggregateFunc = "SUM"
	AggAvg           AggregateFunc = "AVG"
	AggMin           AggregateFunc = "MIN"
	AggMax           AggregateFunc = "MAX"
	AggCount         AggregateFunc = "COUNT"
	AggCountDistinct AggregateFunc = "COUNT_DISTINCT"
)

// FieldExpression represents a field with optional aggregate function or expression.
type FieldExpression struct {
	Field     types.Field
	Aggregate AggregateFunc
	Case      *CaseExpression     // For CASE expressions
	Coalesce  *CoalesceExpression // For COALESCE expressions
	NullIf    *NullIfExpression   // For NULLIF expressions
	Alias     string
}

// AST represents a SQLite-specific query AST.
// SQLite is simpler than PostgreSQL - no schemas, users, etc.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type AST struct {
	*types.QueryAST

	// SQLite-specific features
	// Note: OR IGNORE/OR REPLACE are now handled via OnConflict
	IfNotExists bool // For CREATE TABLE IF NOT EXISTS
	IfExists    bool // For DROP TABLE IF EXISTS

	// JOIN support (SQLite doesn't support RIGHT JOIN)
	Joins []Join // JOIN clauses

	// GROUP BY support (same as PostgreSQL)
	GroupBy []types.Field     // GROUP BY fields
	Having  []types.Condition // HAVING conditions

	// SQLite UPSERT support
	OnConflict *ConflictClause // ON CONFLICT clause

	// DISTINCT support
	Distinct bool // SELECT DISTINCT

	// Advanced features
	FieldExpressions []FieldExpression // Fields with expressions/aggregates
	Returning        []types.Field     // RETURNING clause (SQLite 3.35.0+)
}

// ConflictClause represents SQLite's ON CONFLICT clause.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type ConflictClause struct {
	Columns []types.Field
	Action  ConflictAction
	Updates map[types.Field]types.Param // For DO UPDATE SET
}

// ConflictAction represents the action to take on conflict.
type ConflictAction string

const (
	ConflictDoNothing ConflictAction = "DO NOTHING"
	ConflictDoUpdate  ConflictAction = "DO UPDATE"
)

// JoinType represents the type of SQL JOIN.
// Note: SQLite doesn't support RIGHT JOIN.
type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	CrossJoin JoinType = "CROSS JOIN"
)

// Join represents a SQL JOIN clause.
//
//nolint:govet // struct field ordering is optimized for clarity, not memory alignment
type Join struct {
	Type  JoinType
	Table types.Table
	On    types.ConditionItem // nil for CROSS JOIN
}

// Validate performs SQLite-specific validation.
func (ast *AST) Validate() error {
	// First validate the base AST
	if err := ast.QueryAST.Validate(); err != nil {
		return err
	}

	// SQLite-specific validations
	switch ast.Operation {
	case types.OpSelect:
		// HAVING requires GROUP BY
		if len(ast.Having) > 0 && len(ast.GroupBy) == 0 {
			return fmt.Errorf("HAVING requires GROUP BY")
		}

	case types.OpInsert:
		// OnConflict validation handled by base AST validation

	case types.OpUpdate, types.OpDelete:
		// SQLite doesn't support RETURNING for UPDATE/DELETE
		// (only supports RETURNING for INSERT in newer versions)

	default:
		// Other operations don't have SQLite-specific validations
	}

	return nil
}
