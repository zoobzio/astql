package minio

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// QuerySchema represents a MinIO query in declarative form.
type QuerySchema struct {
	Values    []map[string]string `json:"values,omitempty" yaml:"values,omitempty"`
	Fields    []string            `json:"fields,omitempty" yaml:"fields,omitempty"`
	Updates   map[string]string   `json:"updates,omitempty" yaml:"updates,omitempty"`
	Operation string              `json:"operation" yaml:"operation"`
	Table     string              `json:"table" yaml:"table"`
	Where     *ConditionSchema    `json:"where,omitempty" yaml:"where,omitempty"`
	Limit     *int                `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset    *int                `json:"offset,omitempty" yaml:"offset,omitempty"`
	// MinIO-specific fields
	ContentParam string   `json:"content_param,omitempty" yaml:"content_param,omitempty"`
	EventTypes   []string `json:"event_types,omitempty" yaml:"event_types,omitempty"`
}

// ConditionSchema represents a condition in declarative form.
type ConditionSchema struct {
	Field    string `json:"field" yaml:"field"`
	Operator string `json:"operator" yaml:"operator"`
	Param    string `json:"param" yaml:"param"`
}

// BuildFromSchema converts a QuerySchema to a MinIO AST.
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

	// Add MinIO-specific features
	if schema.ContentParam != "" {
		builder = builder.WithContent(astql.P(schema.ContentParam))
	}

	if len(schema.EventTypes) > 0 && (schema.Operation == "LISTEN" || schema.Operation == "listen") {
		// Convert string event types to EventType constants
		events := make([]EventType, 0, len(schema.EventTypes))
		for _, evt := range schema.EventTypes {
			eventType, err := parseEventType(evt)
			if err != nil {
				return nil, fmt.Errorf("invalid event type '%s': %w", evt, err)
			}
			events = append(events, eventType)
		}
		builder = builder.WithEventTypes(events...)
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
	case "LIKE", "like":
		return astql.LIKE, nil
	default:
		return "", fmt.Errorf("MinIO only supports = and LIKE operators")
	}
}

// parseEventType converts a string to EventType constant.
func parseEventType(evt string) (EventType, error) {
	// Map of allowed event type strings to constants
	eventMap := map[string]EventType{
		"s3:ObjectCreated:*":                       EventObjectCreatedAll,
		"s3:ObjectCreated:Put":                     EventObjectCreatedPut,
		"s3:ObjectCreated:Post":                    EventObjectCreatedPost,
		"s3:ObjectCreated:Copy":                    EventObjectCreatedCopy,
		"s3:ObjectCreated:CompleteMultipartUpload": EventObjectCreatedMultipart,
		"s3:ObjectRemoved:*":                       EventObjectRemovedAll,
		"s3:ObjectRemoved:Delete":                  EventObjectRemovedDelete,
		"s3:ObjectRemoved:DeleteMarkerCreated":     EventObjectRemovedDeleteMarkerCreated,
		"s3:ObjectAccessed:*":                      EventObjectAccessedAll,
		"s3:ObjectAccessed:Get":                    EventObjectAccessedGet,
		"s3:ObjectAccessed:Head":                   EventObjectAccessedHead,
	}

	if eventType, ok := eventMap[evt]; ok {
		return eventType, nil
	}

	return "", fmt.Errorf("unknown event type - must use predefined constants")
}
