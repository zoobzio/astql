package types

import "fmt"

// Operation represents the type of query operation.
type Operation string

const (
	OpSelect Operation = "SELECT"
	OpInsert Operation = "INSERT"
	OpUpdate Operation = "UPDATE"
	OpDelete Operation = "DELETE"
	OpCount  Operation = "COUNT"
)

// Direction represents sort direction.
type Direction string

const (
	ASC  Direction = "ASC"
	DESC Direction = "DESC"
)

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	Field     Field
	Direction Direction
}

// QueryAST represents the core abstract syntax tree for queries.
// This is exported from the internal package so providers can use it,
// but external users cannot import this package.
//
//nolint:govet // fieldalignment: Logical grouping is preferred over memory optimization
type QueryAST struct {
	Operation   Operation
	Target      Table
	Fields      []Field
	WhereClause ConditionItem
	Ordering    []OrderBy
	Limit       *int
	Offset      *int
	Updates     map[Field]Param   // For UPDATE operations
	Values      []map[Field]Param // For INSERT operations
}

// Validate performs basic validation on the AST.
func (ast *QueryAST) Validate() error {
	if ast.Target.Name == "" {
		return fmt.Errorf("target table is required")
	}

	switch ast.Operation {
	case OpSelect:
		// Fields are optional (defaults to *)
	case OpInsert:
		if len(ast.Values) == 0 {
			return fmt.Errorf("INSERT requires at least one value set")
		}
		// Ensure all value sets have the same fields
		if len(ast.Values) > 1 {
			firstKeys := make(map[Field]bool)
			for k := range ast.Values[0] {
				firstKeys[k] = true
			}
			for i, valueSet := range ast.Values[1:] {
				if len(valueSet) != len(firstKeys) {
					return fmt.Errorf("value set %d has different number of fields", i+1)
				}
				for k := range valueSet {
					if !firstKeys[k] {
						return fmt.Errorf("value set %d has different fields", i+1)
					}
				}
			}
		}
	case OpUpdate:
		if len(ast.Updates) == 0 {
			return fmt.Errorf("UPDATE requires at least one field to update")
		}
	case OpDelete:
		// No additional validation needed
	case OpCount:
		// No additional validation needed - COUNT can have WHERE but no fields
	default:
		return fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	return nil
}
