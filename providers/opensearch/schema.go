package opensearch

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// SchemaLoader loads OpenSearch queries from schema definitions.
type SchemaLoader struct {
	provider *Provider
}

// NewSchemaLoader creates a new schema loader.
func NewSchemaLoader(provider *Provider) *SchemaLoader {
	return &SchemaLoader{provider: provider}
}

// LoadQuery converts a schema definition to an executable query.
func (sl *SchemaLoader) LoadQuery(schema map[string]interface{}) (*AST, error) {
	// Extract operation
	operation, err := extractOperation(schema)
	if err != nil {
		return nil, err
	}

	// Extract table
	table, err := extractTable(schema)
	if err != nil {
		return nil, err
	}

	// Build query based on operation
	var builder *Builder

	switch operation {
	case "search", "select":
		builder = Select(table)
	case "insert", "index":
		builder = Insert(table)
	case "update":
		builder = Update(table)
	case "delete":
		builder = Delete(table)
	case "count":
		builder = Count(table)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", operation)
	}

	// Apply query components
	if err := sl.applyQueryComponents(builder, schema); err != nil {
		return nil, err
	}

	return builder.Build()
}

// applyQueryComponents applies various query components from schema.
func (sl *SchemaLoader) applyQueryComponents(builder *Builder, schema map[string]interface{}) error {
	// WHERE clause
	if where, exists := schema["where"]; exists {
		condition, err := sl.parseCondition(where)
		if err != nil {
			return fmt.Errorf("invalid where clause: %w", err)
		}
		builder.Where(condition)
	}

	// VALUES for INSERT
	if values, exists := schema["values"]; exists {
		valueMap, err := sl.parseValues(values)
		if err != nil {
			return fmt.Errorf("invalid values: %w", err)
		}
		builder.Values(valueMap)
	}

	// SET for UPDATE
	if set, exists := schema["set"]; exists {
		setMap, ok := set.(map[string]interface{})
		if !ok {
			return fmt.Errorf("set must be an object")
		}
		for fieldStr, paramStr := range setMap {
			param, ok := paramStr.(string)
			if !ok {
				return fmt.Errorf("set values must be parameter names")
			}
			builder.Set(astql.F(fieldStr), astql.P(param))
		}
	}

	// FIELDS selection
	if fields, exists := schema["fields"]; exists {
		fieldList, err := sl.parseFields(fields)
		if err != nil {
			return fmt.Errorf("invalid fields: %w", err)
		}
		builder.Fields(fieldList...)
	}

	// ORDER BY
	if orderBy, exists := schema["order_by"]; exists {
		if err := sl.parseOrderBy(builder, orderBy); err != nil {
			return fmt.Errorf("invalid order_by: %w", err)
		}
	}

	// LIMIT
	if limit, exists := schema["limit"]; exists {
		limitInt, ok := toInt(limit)
		if !ok {
			return fmt.Errorf("limit must be a number")
		}
		builder.Limit(limitInt)
	}

	// OFFSET
	if offset, exists := schema["offset"]; exists {
		offsetInt, ok := toInt(offset)
		if !ok {
			return fmt.Errorf("offset must be a number")
		}
		builder.Offset(offsetInt)
	}

	// OpenSearch-specific features
	return sl.applyOpenSearchFeatures(builder, schema)
}

// applyOpenSearchFeatures applies OpenSearch-specific features.
func (sl *SchemaLoader) applyOpenSearchFeatures(builder *Builder, schema map[string]interface{}) error {
	// Highlighting
	if highlight, exists := schema["highlight"]; exists {
		fields, err := sl.parseFields(highlight)
		if err != nil {
			return fmt.Errorf("invalid highlight fields: %w", err)
		}
		builder.WithHighlight(fields...)
	}

	// Source filtering
	if includes, exists := schema["source_includes"]; exists {
		fields, err := sl.parseFields(includes)
		if err != nil {
			return fmt.Errorf("invalid source_includes: %w", err)
		}
		builder.WithSourceIncludes(fields...)
	}

	if excludes, exists := schema["source_excludes"]; exists {
		fields, err := sl.parseFields(excludes)
		if err != nil {
			return fmt.Errorf("invalid source_excludes: %w", err)
		}
		builder.WithSourceExcludes(fields...)
	}

	// Minimum score
	if minScore, exists := schema["min_score"]; exists {
		score, ok := toFloat64(minScore)
		if !ok {
			return fmt.Errorf("min_score must be a number")
		}
		builder.WithMinScore(score)
	}

	// Analyzer
	if analyzer, exists := schema["analyzer"]; exists {
		analyzerStr, ok := analyzer.(string)
		if !ok {
			return fmt.Errorf("analyzer must be a string")
		}
		analyzerType := parseAnalyzer(analyzerStr)
		builder.WithAnalyzer(analyzerType)
	}

	return nil
}

// parseCondition parses a condition from schema.
func (sl *SchemaLoader) parseCondition(condition interface{}) (astql.ConditionItem, error) {
	switch cond := condition.(type) {
	case map[string]interface{}:
		// Check for logical operators
		if and, exists := cond["and"]; exists {
			return sl.parseLogicalGroup(and, astql.AND)
		}
		if or, exists := cond["or"]; exists {
			return sl.parseLogicalGroup(or, astql.OR)
		}

		// Simple condition
		return sl.parseSimpleCondition(cond)

	default:
		return nil, fmt.Errorf("condition must be an object")
	}
}

// parseLogicalGroup parses AND/OR groups.
func (sl *SchemaLoader) parseLogicalGroup(conditions interface{}, op astql.LogicOperator) (astql.ConditionGroup, error) {
	condList, ok := conditions.([]interface{})
	if !ok {
		return astql.ConditionGroup{}, fmt.Errorf("logical group must be an array")
	}

	items := make([]astql.ConditionItem, 0, len(condList))
	for _, c := range condList {
		item, err := sl.parseCondition(c)
		if err != nil {
			return astql.ConditionGroup{}, err
		}
		items = append(items, item)
	}

	if op == astql.AND {
		return astql.And(items...), nil
	}
	return astql.Or(items...), nil
}

// parseSimpleCondition parses a simple field condition.
func (*SchemaLoader) parseSimpleCondition(cond map[string]interface{}) (astql.Condition, error) {
	field, exists := cond["field"]
	if !exists {
		return astql.Condition{}, fmt.Errorf("condition missing field")
	}

	operator, exists := cond["operator"]
	if !exists {
		return astql.Condition{}, fmt.Errorf("condition missing operator")
	}

	value, exists := cond["value"]
	if !exists {
		return astql.Condition{}, fmt.Errorf("condition missing value")
	}

	fieldStr, ok := field.(string)
	if !ok {
		return astql.Condition{}, fmt.Errorf("field must be a string")
	}

	operatorStr, ok := operator.(string)
	if !ok {
		return astql.Condition{}, fmt.Errorf("operator must be a string")
	}

	valueStr, ok := value.(string)
	if !ok {
		return astql.Condition{}, fmt.Errorf("value must be a parameter name")
	}

	op := parseOperator(operatorStr)
	return astql.C(astql.F(fieldStr), op, astql.P(valueStr)), nil
}

// parseValues parses INSERT values.
func (*SchemaLoader) parseValues(values interface{}) (map[astql.Field]astql.Param, error) {
	valMap, ok := values.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("values must be an object")
	}

	result := make(map[astql.Field]astql.Param)
	for fieldStr, paramStr := range valMap {
		param, ok := paramStr.(string)
		if !ok {
			return nil, fmt.Errorf("values must map to parameter names")
		}
		result[astql.F(fieldStr)] = astql.P(param)
	}

	return result, nil
}

// parseFields parses a field list.
func (*SchemaLoader) parseFields(fields interface{}) ([]astql.Field, error) {
	switch f := fields.(type) {
	case []interface{}:
		result := make([]astql.Field, 0, len(f))
		for _, field := range f {
			fieldStr, ok := field.(string)
			if !ok {
				return nil, fmt.Errorf("field must be a string")
			}
			result = append(result, astql.F(fieldStr))
		}
		return result, nil

	case string:
		// Single field
		return []astql.Field{astql.F(f)}, nil

	default:
		return nil, fmt.Errorf("fields must be string or array")
	}
}

// parseOrderBy parses ORDER BY clause.
func (sl *SchemaLoader) parseOrderBy(builder *Builder, orderBy interface{}) error {
	switch ob := orderBy.(type) {
	case map[string]interface{}:
		for fieldStr, dirStr := range ob {
			dir, ok := dirStr.(string)
			if !ok {
				return fmt.Errorf("order direction must be a string")
			}
			direction := parseDirection(dir)
			builder.OrderBy(astql.F(fieldStr), direction)
		}
		return nil

	case []interface{}:
		for _, item := range ob {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				return fmt.Errorf("order_by array items must be objects")
			}
			if err := sl.parseOrderBy(builder, itemMap); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("order_by must be object or array")
	}
}

// Helper functions

func extractOperation(schema map[string]interface{}) (string, error) {
	op, exists := schema["operation"]
	if !exists {
		return "", fmt.Errorf("schema missing operation")
	}
	opStr, ok := op.(string)
	if !ok {
		return "", fmt.Errorf("operation must be a string")
	}
	return strings.ToLower(opStr), nil
}

func extractTable(schema map[string]interface{}) (astql.Table, error) {
	table, exists := schema["table"]
	if !exists {
		return astql.Table{}, fmt.Errorf("schema missing table")
	}
	tableStr, ok := table.(string)
	if !ok {
		return astql.Table{}, fmt.Errorf("table must be a string")
	}
	return astql.T(tableStr), nil
}

func parseOperator(op string) astql.Operator {
	switch strings.ToUpper(op) {
	case "=", "EQ", "EQUALS":
		return astql.EQ
	case "!=", "<>", "NE", "NOT_EQUALS":
		return astql.NE
	case ">", "GT", "GREATER_THAN":
		return astql.GT
	case ">=", "GTE", "GREATER_THAN_OR_EQUALS":
		return astql.GE
	case "<", "LT", "LESS_THAN":
		return astql.LT
	case "<=", "LTE", "LESS_THAN_OR_EQUALS":
		return astql.LE
	case "LIKE", "CONTAINS":
		return astql.LIKE
	case "IN":
		return astql.IN
	case "IS", "IS_NULL":
		return astql.IsNull
	case "IS_NOT_NULL":
		return astql.IsNotNull
	default:
		return astql.EQ // Default
	}
}

func parseDirection(dir string) astql.Direction {
	switch strings.ToUpper(dir) {
	case "DESC", "DESCENDING":
		return astql.DESC
	default:
		return astql.ASC
	}
}

func parseAnalyzer(analyzer string) AnalyzerType {
	switch strings.ToLower(analyzer) {
	case "standard":
		return AnalyzerStandard
	case "simple":
		return AnalyzerSimple
	case "whitespace":
		return AnalyzerWhitespace
	case "keyword":
		return AnalyzerKeyword
	case "english":
		return AnalyzerEnglish
	default:
		return AnalyzerStandard
	}
}

func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case float64:
		return int(val), true
	case int64:
		return int(val), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}
