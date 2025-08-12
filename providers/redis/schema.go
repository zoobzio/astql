package redis

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// QuerySchema represents a Redis query in declarative form.
//
//nolint:govet // field alignment not critical for schema definitions
type QuerySchema struct {
	Values        []map[string]string `json:"values,omitempty" yaml:"values,omitempty"`
	Fields        []string            `json:"fields,omitempty" yaml:"fields,omitempty"`
	Updates       map[string]string   `json:"updates,omitempty" yaml:"updates,omitempty"`
	Operation     string              `json:"operation" yaml:"operation"`
	Table         string              `json:"table" yaml:"table"`
	NotifyPayload string              `json:"notify_payload,omitempty" yaml:"notify_payload,omitempty"`
	Where         *ConditionSchema    `json:"where,omitempty" yaml:"where,omitempty"`
	Limit         *int                `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset        *int                `json:"offset,omitempty" yaml:"offset,omitempty"`
	// Redis-specific fields
	TTLParam   string `json:"ttl_param,omitempty" yaml:"ttl_param,omitempty"`     // Parameter name for TTL
	ScoreParam string `json:"score_param,omitempty" yaml:"score_param,omitempty"` // Parameter name for score
}

// ConditionSchema represents a condition in declarative form.
type ConditionSchema struct {
	Field    string `json:"field" yaml:"field"`
	Operator string `json:"operator" yaml:"operator"`
	Param    string `json:"param" yaml:"param"`
}

// BuildFromSchema converts a QuerySchema to a Redis AST.
func BuildFromSchema(schema *QuerySchema) (*AST, error) {
	if schema.Operation == "" {
		return nil, fmt.Errorf("operation is required")
	}
	if schema.Table == "" {
		return nil, fmt.Errorf("table is required")
	}

	// Create table with validation
	table, err := astql.TryT(schema.Table)
	if err != nil {
		return nil, err
	}

	// Start building based on operation
	var builder *Builder
	switch schema.Operation {
	case "SELECT", "select":
		builder = Select(table)
	case "INSERT", "insert":
		builder = Insert(table)
	case "UPDATE", "update":
		builder = Update(table)
	case "DELETE", "delete":
		builder = Delete(table)
	case "COUNT", "count":
		builder = Count(table)
	case "LISTEN", "listen":
		builder = Listen(table)
	case "NOTIFY", "notify":
		if schema.NotifyPayload == "" {
			return nil, fmt.Errorf("NOTIFY requires a payload parameter name")
		}
		builder = Notify(table, astql.P(schema.NotifyPayload))
	case "UNLISTEN", "unlisten":
		builder = Unlisten(table)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", schema.Operation)
	}

	// Add fields for SELECT
	if schema.Operation == "SELECT" || schema.Operation == "select" {
		if len(schema.Fields) > 0 {
			fields := make([]astql.Field, len(schema.Fields))
			for i, f := range schema.Fields {
				field, err := astql.TryF(f)
				if err != nil {
					return nil, fmt.Errorf("invalid field '%s': %w", f, err)
				}
				fields[i] = field
			}
			builder = builder.Fields(fields...)
		}
	}

	// Add WHERE clause
	if schema.Where != nil {
		condition, err := buildConditionFromSchema(schema.Where)
		if err != nil {
			return nil, fmt.Errorf("invalid where clause: %w", err)
		}
		builder = builder.Where(condition)
	}

	// Add LIMIT/OFFSET
	if schema.Limit != nil {
		builder = builder.Limit(*schema.Limit)
	}
	if schema.Offset != nil {
		builder = builder.Offset(*schema.Offset)
	}

	// Handle operation-specific fields
	switch schema.Operation {
	case "UPDATE", "update":
		if len(schema.Updates) == 0 {
			return nil, fmt.Errorf("UPDATE requires at least one field to update")
		}
		for field, param := range schema.Updates {
			f, err := astql.TryF(field)
			if err != nil {
				return nil, fmt.Errorf("invalid update field '%s': %w", field, err)
			}
			builder = builder.Set(f, astql.P(param))
		}

	case "INSERT", "insert":
		if len(schema.Values) == 0 {
			return nil, fmt.Errorf("INSERT requires at least one value set")
		}
		for _, valueSet := range schema.Values {
			values := make(map[astql.Field]astql.Param)
			for field, param := range valueSet {
				f, err := astql.TryF(field)
				if err != nil {
					return nil, fmt.Errorf("invalid insert field '%s': %w", field, err)
				}
				values[f] = astql.P(param)
			}
			builder = builder.Values(values)
		}
	}

	// Add Redis-specific features
	if schema.TTLParam != "" {
		builder = builder.WithTTL(astql.P(schema.TTLParam))
	}
	if schema.ScoreParam != "" {
		builder = builder.WithScore(astql.P(schema.ScoreParam))
	}

	return builder.Build()
}

// buildConditionFromSchema converts a ConditionSchema to a Condition.
func buildConditionFromSchema(schema *ConditionSchema) (astql.ConditionItem, error) {
	if schema.Field == "" {
		return nil, fmt.Errorf("field is required for condition")
	}
	if schema.Operator == "" {
		return nil, fmt.Errorf("operator is required for condition")
	}
	if schema.Param == "" {
		return nil, fmt.Errorf("param is required for condition")
	}

	field, err := astql.TryF(schema.Field)
	if err != nil {
		return nil, fmt.Errorf("invalid condition field '%s': %w", schema.Field, err)
	}

	// Convert operator string to Operator type
	op, err := parseOperator(schema.Operator)
	if err != nil {
		return nil, err
	}

	return astql.C(field, op, astql.P(schema.Param)), nil
}

// parseOperator converts an operator string to an Operator type.
func parseOperator(opStr string) (astql.Operator, error) {
	switch opStr {
	case "=", "==", "EQ":
		return astql.EQ, nil
	case "!=", "<>", "NE":
		return astql.NE, nil
	case ">", "GT":
		return astql.GT, nil
	case ">=", "GE":
		return astql.GE, nil
	case "<", "LT":
		return astql.LT, nil
	case "<=", "LE":
		return astql.LE, nil
	default:
		return "", fmt.Errorf("unsupported operator for Redis: %s", opStr)
	}
}
