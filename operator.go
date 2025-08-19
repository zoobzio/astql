package astql

import "github.com/zoobzio/astql/internal/types"

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
