package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
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
	Field     astql.Field
	Precision *astql.Param // Optional, for ROUND
	Exponent  *astql.Param // Optional, for POWER
	Alias     string
}

// Round creates a ROUND math expression.
func Round(field astql.Field, precision ...astql.Param) MathExpression {
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
func Floor(field astql.Field) MathExpression {
	return MathExpression{
		Function: MathFloor,
		Field:    field,
	}
}

// Ceil creates a CEIL math expression.
func Ceil(field astql.Field) MathExpression {
	return MathExpression{
		Function: MathCeil,
		Field:    field,
	}
}

// Abs creates an ABS math expression.
func Abs(field astql.Field) MathExpression {
	return MathExpression{
		Function: MathAbs,
		Field:    field,
	}
}

// Power creates a POWER math expression.
func Power(field astql.Field, exponent astql.Param) MathExpression {
	return MathExpression{
		Function: MathPower,
		Field:    field,
		Exponent: &exponent,
	}
}

// Sqrt creates a SQRT math expression.
func Sqrt(field astql.Field) MathExpression {
	return MathExpression{
		Function: MathSqrt,
		Field:    field,
	}
}

// As adds an alias to a math expression.
func (expr MathExpression) As(alias string) MathExpression {
	// Validate alias against registered field aliases
	if err := astql.ValidateFieldAlias(alias); err != nil {
		panic(fmt.Errorf("invalid field alias: %w", err))
	}
	expr.Alias = alias
	return expr
}
