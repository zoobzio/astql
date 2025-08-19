package postgres

import (
	"github.com/zoobzio/astql/internal/types"
)

// MathFunc represents SQL math functions.
type MathFunc string

const (
	MathRound MathFunc = "ROUND"
	MathFloor MathFunc = "FLOOR"
	MathCeil  MathFunc = "CEIL"
	MathAbs   MathFunc = "ABS"
	MathPower MathFunc = "POWER"
	MathSqrt  MathFunc = "SQRT"
)

// MathExpression represents a math function call.
type MathExpression struct {
	Function  MathFunc
	Field     types.Field
	Precision *types.Param // Optional, for ROUND
	Exponent  *types.Param // Optional, for POWER
	Alias     string
}

// Round creates a ROUND math expression.
func Round(field types.Field, precision ...types.Param) MathExpression {
	expr := MathExpression{
		Function: MathRound,
		Field:    field,
	}
	if len(precision) > 0 {
		expr.Precision = &precision[0]
	}
	return expr
}

// Floor creates a FLOOR math expression.
func Floor(field types.Field) MathExpression {
	return MathExpression{
		Function: MathFloor,
		Field:    field,
	}
}

// Ceil creates a CEIL math expression.
func Ceil(field types.Field) MathExpression {
	return MathExpression{
		Function: MathCeil,
		Field:    field,
	}
}

// Abs creates an ABS math expression.
func Abs(field types.Field) MathExpression {
	return MathExpression{
		Function: MathAbs,
		Field:    field,
	}
}

// Power creates a POWER math expression.
func Power(field types.Field, exponent types.Param) MathExpression {
	return MathExpression{
		Function: MathPower,
		Field:    field,
		Exponent: &exponent,
	}
}

// Sqrt creates a SQRT math expression.
func Sqrt(field types.Field) MathExpression {
	return MathExpression{
		Function: MathSqrt,
		Field:    field,
	}
}

// As adds an alias to a math expression.
func (expr MathExpression) As(alias string) MathExpression {
	expr.Alias = alias
	return expr
}
