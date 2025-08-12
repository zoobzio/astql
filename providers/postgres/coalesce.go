package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// CoalesceExpression represents a COALESCE function call.
type CoalesceExpression struct {
	Alias  string
	Values []astql.Param
}

// NullIfExpression represents a NULLIF function call.
type NullIfExpression struct {
	Alias  string
	Value1 astql.Param
	Value2 astql.Param
}

// Coalesce creates a COALESCE expression that returns the first non-null value.
func Coalesce(values ...astql.Param) CoalesceExpression {
	if len(values) < 2 {
		panic("COALESCE requires at least 2 values")
	}
	return CoalesceExpression{Values: values}
}

// As adds an alias to the COALESCE expression.
func (c CoalesceExpression) As(alias string) CoalesceExpression {
	// Validate alias against registered field aliases
	if err := astql.ValidateFieldAlias(alias); err != nil {
		panic(fmt.Errorf("invalid field alias: %w", err))
	}
	c.Alias = alias
	return c
}

// NullIf creates a NULLIF expression that returns NULL if two values are equal.
func NullIf(value1, value2 astql.Param) NullIfExpression {
	return NullIfExpression{
		Value1: value1,
		Value2: value2,
	}
}

// As adds an alias to the NULLIF expression.
func (n NullIfExpression) As(alias string) NullIfExpression {
	// Validate alias against registered field aliases
	if err := astql.ValidateFieldAlias(alias); err != nil {
		panic(fmt.Errorf("invalid field alias: %w", err))
	}
	n.Alias = alias
	return n
}
