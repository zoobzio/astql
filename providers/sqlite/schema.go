package sqlite

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// QuerySchema represents a query that can be serialized from YAML/JSON.
// Simplified version of PostgreSQL schema - no JOINs or complex expressions yet.
type QuerySchema struct {
	// Core fields
	Operation string            `json:"operation" yaml:"operation"`
	Table     string            `json:"table" yaml:"table"`
	Alias     string            `json:"alias,omitempty" yaml:"alias,omitempty"`
	Fields    []string          `json:"fields,omitempty" yaml:"fields,omitempty"`
	Where     *ConditionSchema  `json:"where,omitempty" yaml:"where,omitempty"`
	GroupBy   []string          `json:"group_by,omitempty" yaml:"group_by,omitempty"`
	Having    []ConditionSchema `json:"having,omitempty" yaml:"having,omitempty"`
	OrderBy   []OrderSchema     `json:"order_by,omitempty" yaml:"order_by,omitempty"`
	Limit     *int              `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset    *int              `json:"offset,omitempty" yaml:"offset,omitempty"`

	// INSERT specific
	Values []map[string]string `json:"values,omitempty" yaml:"values,omitempty"`

	// UPDATE specific
	Updates map[string]string `json:"updates,omitempty" yaml:"updates,omitempty"`

	// UPSERT support
	OnConflict *ConflictSchema `json:"on_conflict,omitempty" yaml:"on_conflict,omitempty"`
}

// ConditionSchema represents a condition in declarative form.
type ConditionSchema struct {
	// Simple condition
	Field    string `json:"field,omitempty" yaml:"field,omitempty"`
	Operator string `json:"operator,omitempty" yaml:"operator,omitempty"`
	Param    string `json:"param,omitempty" yaml:"param,omitempty"`

	// Grouped conditions
	Logic      string            `json:"logic,omitempty" yaml:"logic,omitempty"`
	Conditions []ConditionSchema `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// OrderSchema represents ordering in declarative form.
type OrderSchema struct {
	Field     string `json:"field" yaml:"field"`
	Direction string `json:"direction,omitempty" yaml:"direction,omitempty"`
}

// ConflictSchema represents ON CONFLICT clause in declarative form.
type ConflictSchema struct {
	Columns []string          `json:"columns,omitempty" yaml:"columns,omitempty"`
	Action  string            `json:"action" yaml:"action"` // "nothing" or "update"
	Updates map[string]string `json:"updates,omitempty" yaml:"updates,omitempty"`
}

// BuildFromSchema builds a SQLite AST from a declarative schema.
func BuildFromSchema(schema *QuerySchema) (*AST, error) {
	// Validate operation
	op := strings.ToUpper(schema.Operation)
	if err := validateOperation(op); err != nil {
		return nil, err
	}

	// Create table
	table, err := astql.TryT(schema.Table)
	if err != nil {
		return nil, fmt.Errorf("invalid table: %w", err)
	}
	// In astql, aliases are provided at table creation
	if schema.Alias != "" {
		table, err = astql.TryT(schema.Table, schema.Alias)
		if err != nil {
			return nil, fmt.Errorf("invalid table with alias: %w", err)
		}
	}

	// Create base builder based on operation
	var builder *Builder
	switch op {
	case "SELECT":
		builder = Select(table)
	case "INSERT":
		builder = Insert(table)
	case "UPDATE":
		builder = Update(table)
	case "DELETE":
		builder = Delete(table)
	case "COUNT":
		builder = Count(table)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", op)
	}

	// Add fields for SELECT
	if op == "SELECT" && len(schema.Fields) > 0 {
		fields := make([]types.Field, len(schema.Fields))
		for i, fieldName := range schema.Fields {
			field, err := astql.TryF(fieldName)
			if err != nil {
				return nil, fmt.Errorf("invalid field: %w", err)
			}
			fields[i] = field
		}
		builder = builder.Fields(fields...)
	}

	// Add WHERE clause
	if schema.Where != nil {
		cond, err := buildCondition(schema.Where)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE condition: %w", err)
		}
		builder = builder.Where(cond)
	}

	// Add GROUP BY
	if len(schema.GroupBy) > 0 {
		groupFields := make([]types.Field, len(schema.GroupBy))
		for i, fieldName := range schema.GroupBy {
			field, err := astql.TryF(fieldName)
			if err != nil {
				return nil, fmt.Errorf("invalid group by field: %w", err)
			}
			groupFields[i] = field
		}
		builder = builder.GroupBy(groupFields...)
	}

	// Add HAVING
	if len(schema.Having) > 0 {
		havingConditions := make([]types.Condition, 0, len(schema.Having))
		for _, havingSchema := range schema.Having {
			// Simple conditions only for HAVING
			if havingSchema.Field == "" || havingSchema.Operator == "" || havingSchema.Param == "" {
				return nil, fmt.Errorf("HAVING requires simple conditions")
			}

			field, err := astql.TryF(havingSchema.Field)
			if err != nil {
				return nil, fmt.Errorf("invalid having field: %w", err)
			}

			op, err := parseOperator(havingSchema.Operator)
			if err != nil {
				return nil, fmt.Errorf("invalid having operator: %w", err)
			}

			param, err := astql.TryP(havingSchema.Param)
			if err != nil {
				return nil, fmt.Errorf("invalid having parameter: %w", err)
			}

			havingConditions = append(havingConditions, astql.C(field, op, param))
		}
		builder = builder.Having(havingConditions...)
	}

	// Add ORDER BY
	if len(schema.OrderBy) > 0 {
		for _, order := range schema.OrderBy {
			field, err := astql.TryF(order.Field)
			if err != nil {
				return nil, fmt.Errorf("invalid order field: %w", err)
			}

			direction := types.ASC
			if strings.EqualFold(order.Direction, "DESC") {
				direction = types.DESC
			}

			builder = builder.OrderBy(field, direction)
		}
	}

	// Add LIMIT
	if schema.Limit != nil {
		builder = builder.Limit(*schema.Limit)
	}

	// Add OFFSET
	if schema.Offset != nil {
		builder = builder.Offset(*schema.Offset)
	}

	// Handle INSERT specific
	if op == "INSERT" {
		if len(schema.Values) == 0 {
			return nil, fmt.Errorf("INSERT requires values")
		}

		for _, valueMap := range schema.Values {
			values := make(map[types.Field]types.Param)
			for fieldName, paramName := range valueMap {
				field, err := astql.TryF(fieldName)
				if err != nil {
					return nil, fmt.Errorf("invalid value field: %w", err)
				}
				param, err := astql.TryP(paramName)
				if err != nil {
					return nil, fmt.Errorf("invalid value parameter: %w", err)
				}
				values[field] = param
			}
			builder = builder.Values(values)
		}

		// Handle conflict resolution
		if schema.OnConflict != nil {
			// Handle ON CONFLICT
			columns := make([]types.Field, len(schema.OnConflict.Columns))
			for i, colName := range schema.OnConflict.Columns {
				col, err := astql.TryF(colName)
				if err != nil {
					return nil, fmt.Errorf("invalid conflict column: %w", err)
				}
				columns[i] = col
			}

			conflictBuilder := builder.OnConflict(columns...)

			if schema.OnConflict.Action == "nothing" {
				builder = conflictBuilder.DoNothing()
			} else if schema.OnConflict.Action == "update" {
				updateBuilder := conflictBuilder.DoUpdate()
				for fieldName, paramName := range schema.OnConflict.Updates {
					field, err := astql.TryF(fieldName)
					if err != nil {
						return nil, fmt.Errorf("invalid conflict update field: %w", err)
					}
					param, err := astql.TryP(paramName)
					if err != nil {
						return nil, fmt.Errorf("invalid conflict update parameter: %w", err)
					}
					updateBuilder = updateBuilder.Set(field, param)
				}
				builder = updateBuilder.Build()
			}
		}
	}

	// Handle UPDATE specific
	if op == "UPDATE" {
		if len(schema.Updates) == 0 {
			return nil, fmt.Errorf("UPDATE requires fields to update")
		}

		for fieldName, paramName := range schema.Updates {
			field, err := astql.TryF(fieldName)
			if err != nil {
				return nil, fmt.Errorf("invalid update field: %w", err)
			}
			param, err := astql.TryP(paramName)
			if err != nil {
				return nil, fmt.Errorf("invalid update parameter: %w", err)
			}
			builder = builder.Set(field, param)
		}
	}

	// Build the AST
	return builder.Build()
}

// buildCondition recursively builds conditions from schema.
func buildCondition(schema *ConditionSchema) (types.ConditionItem, error) {
	// Grouped condition
	if schema.Logic != "" {
		if len(schema.Conditions) == 0 {
			return nil, fmt.Errorf("grouped condition requires conditions")
		}

		conditions := make([]types.ConditionItem, len(schema.Conditions))
		for i, condSchema := range schema.Conditions {
			cond, err := buildCondition(&condSchema)
			if err != nil {
				return nil, err
			}
			conditions[i] = cond
		}

		switch strings.ToUpper(schema.Logic) {
		case "AND":
			return astql.And(conditions...), nil
		case "OR":
			return astql.Or(conditions...), nil
		default:
			return nil, fmt.Errorf("invalid logic operator: %s", schema.Logic)
		}
	}

	// Simple condition
	if schema.Field == "" || schema.Operator == "" {
		return nil, fmt.Errorf("condition requires field and operator")
	}

	field, err := astql.TryF(schema.Field)
	if err != nil {
		return nil, fmt.Errorf("invalid field: %w", err)
	}

	op, err := parseOperator(schema.Operator)
	if err != nil {
		return nil, fmt.Errorf("invalid operator: %w", err)
	}

	// Handle NULL operators specially - they don't need params
	if op == types.IsNull || op == types.IsNotNull {
		if schema.Param != "" {
			return nil, fmt.Errorf("%s operator does not take a parameter", op)
		}
		if op == types.IsNull {
			return astql.Null(field), nil
		}
		return astql.NotNull(field), nil
	}

	// Regular operators require a param
	if schema.Param == "" {
		return nil, fmt.Errorf("operator %s requires a parameter", op)
	}

	param, err := astql.TryP(schema.Param)
	if err != nil {
		return nil, fmt.Errorf("invalid parameter: %w", err)
	}

	return astql.C(field, op, param), nil
}

// validateOperation checks if the operation is allowed.
func validateOperation(op string) error {
	allowed := map[string]bool{
		"SELECT": true,
		"INSERT": true,
		"UPDATE": true,
		"DELETE": true,
		"COUNT":  true,
	}

	if !allowed[op] {
		return fmt.Errorf("operation '%s' not allowed", op)
	}

	return nil
}

// Add Offset method to Builder.
func (b *Builder) Offset(offset int) *Builder {
	b.Builder.Offset(offset)
	return b
}

// parseOperator converts an operator string to an Operator type.
func parseOperator(opStr string) (types.Operator, error) {
	// Normalize the operator
	opStr = strings.TrimSpace(strings.ToUpper(opStr))

	// Map string to operator
	operatorMap := map[string]types.Operator{
		"=":           types.EQ,
		"!=":          types.NE,
		"<>":          types.NE, // SQL alternative
		">":           types.GT,
		">=":          types.GE,
		"<":           types.LT,
		"<=":          types.LE,
		"IN":          types.IN,
		"NOT IN":      types.NotIn,
		"LIKE":        types.LIKE,
		"NOT LIKE":    types.NotLike,
		"IS NULL":     types.IsNull,
		"IS NOT NULL": types.IsNotNull,
		"EXISTS":      types.EXISTS,
		"NOT EXISTS":  types.NotExists,
	}

	op, ok := operatorMap[opStr]
	if !ok {
		return "", fmt.Errorf("operator '%s' not allowed", opStr)
	}

	return op, nil
}
