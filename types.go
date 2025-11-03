package astql

import "github.com/zoobzio/astql/internal/types"

// AST represents the abstract syntax tree for a query.
// This is re-exported from internal/types for use by consumers.
type AST = types.AST

// QueryResult contains the rendered SQL and required parameters.
type QueryResult struct {
	SQL            string
	RequiredParams []string
}

// Operation represents the type of query operation.
type Operation = types.Operation

// Re-export operation constants for public API.
const (
	OpSelect = types.OpSelect
	OpInsert = types.OpInsert
	OpUpdate = types.OpUpdate
	OpDelete = types.OpDelete
	OpCount  = types.OpCount
)

// Direction represents sort direction.
type Direction = types.Direction

// Re-export direction constants for public API.
const (
	ASC  = types.ASC
	DESC = types.DESC
)

// Operator represents SQL comparison operators.
type Operator = types.Operator

// Re-export operator constants for public API.
const (
	// Basic comparison operators.
	EQ = types.EQ
	NE = types.NE
	GT = types.GT
	GE = types.GE
	LT = types.LT
	LE = types.LE

	// Extended operators.
	IN        = types.IN
	NotIn     = types.NotIn
	LIKE      = types.LIKE
	NotLike   = types.NotLike
	IsNull    = types.IsNull
	IsNotNull = types.IsNotNull
	EXISTS    = types.EXISTS
	NotExists = types.NotExists
)

// ConditionItem represents either a single condition or a group of conditions.
type ConditionItem = types.ConditionItem
