package postgres

import (
	"github.com/zoobzio/astql/internal/types"
)

// CoalesceExpression represents a COALESCE function call.
type CoalesceExpression struct {
	Alias  string
	Values []types.Param
}

// NullIfExpression represents a NULLIF function call.
type NullIfExpression struct {
	Alias  string
	Value1 types.Param
	Value2 types.Param
}

// Coalesce creates a COALESCE expression that returns the first non-null value.
func Coalesce(values ...types.Param) CoalesceExpression {
	if len(values) < 2 {
		panic("COALESCE requires at least 2 values")
	}
	return CoalesceExpression{Values: values}
}

// As adds an alias to the COALESCE expression.
func (c CoalesceExpression) As(alias string) CoalesceExpression {
	c.Alias = alias
	return c
}

// NullIf creates a NULLIF expression that returns NULL if two values are equal.
func NullIf(value1, value2 types.Param) NullIfExpression {
	return NullIfExpression{
		Value1: value1,
		Value2: value2,
	}
}

// As adds an alias to the NULLIF expression.
func (n NullIfExpression) As(alias string) NullIfExpression {
	n.Alias = alias
	return n
}
