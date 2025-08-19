package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// PostgreSQL-specific schema implementation for declarative query building

const (
	updateOperation    = "UPDATE"
	updateOperationLow = "update"
	insertOperation    = "INSERT"
	insertOperationLow = "insert"
	selectOperation    = "SELECT"
	selectOperationLow = "select"
	deleteOperation    = "DELETE"
	deleteOperationLow = "delete"
)

// QuerySchema represents a query that can be serialized from YAML/JSON.
type QuerySchema struct {
	// 8-byte aligned fields first (pointers, slices, maps)
	FieldExpressions []FieldExpressionSchema `json:"field_expressions,omitempty" yaml:"field_expressions,omitempty"`
	Joins            []JoinSchema            `json:"joins,omitempty" yaml:"joins,omitempty"`
	Fields           []string                `json:"fields,omitempty" yaml:"fields,omitempty"`
	GroupBy          []string                `json:"group_by,omitempty" yaml:"group_by,omitempty"`
	Having           []ConditionSchema       `json:"having,omitempty" yaml:"having,omitempty"`
	OrderBy          []OrderSchema           `json:"order_by,omitempty" yaml:"order_by,omitempty"`
	Returning        []string                `json:"returning,omitempty" yaml:"returning,omitempty"`
	Values           []map[string]string     `json:"values,omitempty" yaml:"values,omitempty"`
	Updates          map[string]string       `json:"updates,omitempty" yaml:"updates,omitempty"`
	Where            *ConditionSchema        `json:"where,omitempty" yaml:"where,omitempty"`
	OnConflict       *ConflictSchema         `json:"on_conflict,omitempty" yaml:"on_conflict,omitempty"`
	Limit            *int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset           *int                    `json:"offset,omitempty" yaml:"offset,omitempty"`
	// String fields
	Operation     string `json:"operation" yaml:"operation"`
	Table         string `json:"table" yaml:"table"`
	Alias         string `json:"alias,omitempty" yaml:"alias,omitempty"`
	NotifyPayload string `json:"notify_payload,omitempty" yaml:"notify_payload,omitempty"`
	// Smaller fields last
	Distinct bool `json:"distinct,omitempty" yaml:"distinct,omitempty"`
}

// ConditionSchema represents a condition in declarative form.
type ConditionSchema struct {
	// For simple conditions
	Field    string `json:"field,omitempty" yaml:"field,omitempty"`
	Operator string `json:"operator,omitempty" yaml:"operator,omitempty"`
	Param    string `json:"param,omitempty" yaml:"param,omitempty"`

	// For field-to-field comparisons
	LeftField  string `json:"left_field,omitempty" yaml:"left_field,omitempty"`
	RightField string `json:"right_field,omitempty" yaml:"right_field,omitempty"`

	// For subqueries
	Subquery *QuerySchema `json:"subquery,omitempty" yaml:"subquery,omitempty"`

	// For grouped conditions
	Logic      string            `json:"logic,omitempty" yaml:"logic,omitempty"`
	Conditions []ConditionSchema `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// OrderSchema represents ordering in declarative form.
type OrderSchema struct {
	Field     string `json:"field" yaml:"field"`
	Direction string `json:"direction,omitempty" yaml:"direction,omitempty"`
}

// JoinSchema represents a JOIN clause.
type JoinSchema struct {
	Type  string          `json:"type" yaml:"type"`
	Table string          `json:"table" yaml:"table"`
	Alias string          `json:"alias,omitempty" yaml:"alias,omitempty"`
	On    ConditionSchema `json:"on" yaml:"on"`
}

// FieldExpressionSchema represents a field with optional aggregate or expression.
type FieldExpressionSchema struct {
	Field     string          `json:"field" yaml:"field"`
	Aggregate string          `json:"aggregate,omitempty" yaml:"aggregate,omitempty"`
	Case      *CaseSchema     `json:"case,omitempty" yaml:"case,omitempty"`
	Coalesce  *CoalesceSchema `json:"coalesce,omitempty" yaml:"coalesce,omitempty"`
	NullIf    *NullIfSchema   `json:"nullif,omitempty" yaml:"nullif,omitempty"`
	Math      *MathSchema     `json:"math,omitempty" yaml:"math,omitempty"`
	Alias     string          `json:"alias,omitempty" yaml:"alias,omitempty"`
}

// CaseSchema represents a CASE expression.
type CaseSchema struct {
	When []WhenSchema `json:"when" yaml:"when"`
	Else string       `json:"else,omitempty" yaml:"else,omitempty"`
}

// WhenSchema represents a WHEN clause.
type WhenSchema struct {
	Condition ConditionSchema `json:"condition" yaml:"condition"`
	Result    string          `json:"result" yaml:"result"`
}

// CoalesceSchema represents a COALESCE expression.
type CoalesceSchema struct {
	Values []string `json:"values" yaml:"values"`
}

// NullIfSchema represents a NULLIF expression.
type NullIfSchema struct {
	Field string `json:"field" yaml:"field"`
	Value string `json:"value" yaml:"value"`
}

// MathSchema represents a math function.
type MathSchema struct {
	Function string  `json:"function" yaml:"function"`
	Field    string  `json:"field" yaml:"field"`
	Arg      *string `json:"arg,omitempty" yaml:"arg,omitempty"`
}

// ConflictSchema represents ON CONFLICT clause.
type ConflictSchema struct {
	Columns []string          `json:"columns" yaml:"columns"`
	Action  string            `json:"action" yaml:"action"`
	Updates map[string]string `json:"updates,omitempty" yaml:"updates,omitempty"`
}

// BuildFromSchema converts a QuerySchema to a PostgreSQL AST with full validation.
func BuildFromSchema(schema *QuerySchema) (*AST, error) {

	// Validate operation
	if err := ValidateOperation(schema.Operation); err != nil {
		return nil, err
	}

	// Create table using validated constructor
	var table types.Table
	var err error
	if schema.Alias != "" {
		table, err = astql.TryT(schema.Table, schema.Alias)
	} else {
		table, err = astql.TryT(schema.Table)
	}
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
	case updateOperation, updateOperationLow:
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
		payload, err := astql.TryP(schema.NotifyPayload)
		if err != nil {
			return nil, fmt.Errorf("invalid notify payload parameter: %w", err)
		}
		builder = Notify(table, payload)
	case "UNLISTEN", "unlisten":
		builder = Unlisten(table)
	}

	// Add fields and expressions for SELECT
	if schema.Operation == "SELECT" || schema.Operation == "select" {
		// Add simple fields using validated constructors
		if len(schema.Fields) > 0 {
			fields := make([]types.Field, len(schema.Fields))
			for i, fieldName := range schema.Fields {
				field, err := astql.TryF(fieldName)
				if err != nil {
					return nil, err
				}
				fields[i] = field
			}
			builder = builder.Fields(fields...)
		}

		// Add field expressions
		for _, expr := range schema.FieldExpressions {
			fieldExpr, err := buildFieldExpression(expr, schema.Table)
			if err != nil {
				return nil, fmt.Errorf("invalid field expression: %w", err)
			}
			builder = builder.SelectExpr(fieldExpr)
		}

		// Add DISTINCT
		if schema.Distinct {
			builder = builder.Distinct()
		}
	}

	// Add JOINs
	for i := range schema.Joins {
		join := &schema.Joins[i]
		if err := ValidateJoinType(join.Type); err != nil {
			return nil, err
		}

		joinTable, err := astql.TryT(join.Table, join.Alias)
		if err != nil {
			return nil, fmt.Errorf("invalid join table: %w", err)
		}
		on, err := buildConditionFromSchema(&join.On)
		if err != nil {
			return nil, fmt.Errorf("invalid join condition: %w", err)
		}

		switch join.Type {
		case "inner", "INNER":
			builder = builder.InnerJoin(joinTable, on)
		case "left", "LEFT":
			builder = builder.LeftJoin(joinTable, on)
		case "right", "RIGHT":
			builder = builder.RightJoin(joinTable, on)
		}
	}

	// Add WHERE clause
	if schema.Where != nil {
		condition, err := buildConditionFromSchema(schema.Where)
		if err != nil {
			return nil, fmt.Errorf("invalid where condition: %w", err)
		}
		builder = builder.Where(condition)
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
		for i := range schema.Having {
			havingSchema := &schema.Having[i]
			cond, err := buildConditionFromSchema(havingSchema)
			if err != nil {
				return nil, fmt.Errorf("invalid having condition: %w", err)
			}
			// Convert to simple condition
			if simpleCond, ok := cond.(types.Condition); ok {
				havingConditions = append(havingConditions, simpleCond)
			} else {
				return nil, fmt.Errorf("complex conditions not supported in HAVING clause")
			}
		}
		builder = builder.Having(havingConditions...)
	}

	// Add ORDER BY
	for _, order := range schema.OrderBy {
		field, err := astql.TryF(order.Field)
		if err != nil {
			return nil, fmt.Errorf("invalid order by field: %w", err)
		}
		if err := ValidateDirection(order.Direction); err != nil {
			return nil, err
		}

		dir := types.ASC
		if order.Direction == "DESC" || order.Direction == "desc" {
			dir = types.DESC
		}
		builder = builder.OrderBy(field, dir)
	}

	// Add LIMIT/OFFSET
	if schema.Limit != nil {
		if *schema.Limit < 0 {
			return nil, fmt.Errorf("limit must be non-negative")
		}
		builder = builder.Limit(*schema.Limit)
	}
	if schema.Offset != nil {
		if *schema.Offset < 0 {
			return nil, fmt.Errorf("offset must be non-negative")
		}
		builder = builder.Offset(*schema.Offset)
	}

	// Handle operation-specific fields
	switch schema.Operation {
	case updateOperation, updateOperationLow:
		if len(schema.Updates) == 0 {
			return nil, fmt.Errorf("UPDATE requires at least one field to update")
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

	case "INSERT", "insert":
		if len(schema.Values) == 0 {
			return nil, fmt.Errorf("INSERT requires at least one value set")
		}
		for _, valueSet := range schema.Values {
			values := make(map[types.Field]types.Param)
			for fieldName, paramName := range valueSet {
				field, err := astql.TryF(fieldName)
				if err != nil {
					return nil, fmt.Errorf("invalid insert field: %w", err)
				}
				param, err := astql.TryP(paramName)
				if err != nil {
					return nil, fmt.Errorf("invalid insert parameter: %w", err)
				}
				values[field] = param
			}
			builder = builder.Values(values)
		}

		// Add ON CONFLICT
		if schema.OnConflict != nil {
			if err := ValidateConflictAction(schema.OnConflict.Action); err != nil {
				return nil, err
			}

			conflictFields := make([]types.Field, len(schema.OnConflict.Columns))
			for i, col := range schema.OnConflict.Columns {
				field, err := astql.TryF(col)
				if err != nil {
					return nil, fmt.Errorf("invalid conflict column: %w", err)
				}
				conflictFields[i] = field
			}

			conflictBuilder := builder.OnConflict(conflictFields...)

			switch schema.OnConflict.Action {
			case "nothing", "NOTHING":
				builder = conflictBuilder.DoNothing()
			case "update", "UPDATE":
				if len(schema.OnConflict.Updates) == 0 {
					return nil, fmt.Errorf("ON CONFLICT DO UPDATE requires updates")
				}
				updates := make(map[types.Field]types.Param)
				for fieldName, paramName := range schema.OnConflict.Updates {
					field, err := astql.TryF(fieldName)
					if err != nil {
						return nil, fmt.Errorf("invalid conflict update field: %w", err)
					}
					param, err := astql.TryP(paramName)
					if err != nil {
						return nil, fmt.Errorf("invalid conflict update parameter: %w", err)
					}
					updates[field] = param
				}
				builder = conflictBuilder.DoUpdate(updates)
			}
		}
	}

	// Add RETURNING clause
	if len(schema.Returning) > 0 {
		returningFields := make([]types.Field, len(schema.Returning))
		for i, fieldName := range schema.Returning {
			field, err := astql.TryF(fieldName)
			if err != nil {
				return nil, fmt.Errorf("invalid returning field: %w", err)
			}
			returningFields[i] = field
		}
		builder = builder.Returning(returningFields...)
	}

	return builder.Build()
}

// buildConditionFromSchema converts a ConditionSchema to a ConditionItem with validation.
func buildConditionFromSchema(schema *ConditionSchema) (types.ConditionItem, error) {
	// Check if it's a group condition
	if schema.Logic != "" {
		if err := ValidateLogicOperator(schema.Logic); err != nil {
			return nil, err
		}
		if len(schema.Conditions) == 0 {
			return nil, fmt.Errorf("condition group requires at least one condition")
		}

		conditions := make([]types.ConditionItem, len(schema.Conditions))
		for i := range schema.Conditions {
			cond, err := buildConditionFromSchema(&schema.Conditions[i])
			if err != nil {
				return nil, err
			}
			conditions[i] = cond
		}

		switch schema.Logic {
		case "AND", "and":
			g, err := astql.TryAnd(conditions...)
			if err != nil {
				return nil, err
			}
			return g, nil
		case "OR", "or":
			g, err := astql.TryOr(conditions...)
			if err != nil {
				return nil, err
			}
			return g, nil
		}
	}

	// Check if it's a subquery condition
	if schema.Subquery != nil {
		if err := ValidateOperator(schema.Operator); err != nil {
			return nil, err
		}

		// Build the subquery
		subqueryAST, err := BuildFromSchema(schema.Subquery)
		if err != nil {
			return nil, fmt.Errorf("invalid subquery: %w", err)
		}

		// Create subquery
		subquery := Subquery{AST: subqueryAST}

		// Parse operator
		op, err := parseOperator(schema.Operator)
		if err != nil {
			return nil, err
		}

		// Handle EXISTS/NOT EXISTS vs IN/NOT IN
		switch op {
		case types.EXISTS, types.NotExists:
			if schema.Field != "" {
				return nil, fmt.Errorf("%s operator does not take a field", op)
			}
			return CSubExists(op, subquery), nil
		case types.IN, types.NotIn:
			if schema.Field == "" {
				return nil, fmt.Errorf("%s operator requires a field", op)
			}
			// Validate the field using Try variant
			field, err := astql.TryF(schema.Field)
			if err != nil {
				return nil, err
			}
			return CSub(field, op, subquery), nil
		default:
			return nil, fmt.Errorf("operator %s cannot be used with subqueries", op)
		}
	}

	// Check if it's a field-to-field comparison
	if schema.LeftField != "" && schema.RightField != "" {
		if err := ValidateOperator(schema.Operator); err != nil {
			return nil, err
		}

		// Validate fields using Try variants
		leftField, err := astql.TryF(schema.LeftField)
		if err != nil {
			return nil, err
		}
		rightField, err := astql.TryF(schema.RightField)
		if err != nil {
			return nil, err
		}

		op, err := parseOperator(schema.Operator)
		if err != nil {
			return nil, err
		}
		return CF(leftField, op, rightField), nil
	}

	// It's a simple condition
	if schema.Field == "" {
		return nil, fmt.Errorf("field is required for condition")
	}
	if err := ValidateOperator(schema.Operator); err != nil {
		return nil, err
	}

	// Validate field using Try variant
	field, err := astql.TryF(schema.Field)
	if err != nil {
		return nil, err
	}

	op, err := parseOperator(schema.Operator)
	if err != nil {
		return nil, err
	}

	// Handle NULL operators specially
	if op == types.IsNull || op == types.IsNotNull {
		if schema.Param != "" {
			return nil, fmt.Errorf("%s operator does not take a parameter", op)
		}
		if op == types.IsNull {
			nullCond, nullErr := astql.TryNull(field)
			if nullErr != nil {
				return nil, nullErr
			}
			return nullCond, nil
		}
		notNullCond, notNullErr := astql.TryNotNull(field)
		if notNullErr != nil {
			return nil, notNullErr
		}
		return notNullCond, nil
	}

	// Regular condition requires parameter
	if schema.Param == "" {
		return nil, fmt.Errorf("param is required for %s operator", op)
	}

	// Validate parameter using Try variant
	param, err := astql.TryP(schema.Param)
	if err != nil {
		return nil, err
	}

	c, err := astql.TryC(field, op, param)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// parseOperator converts an operator string to an Operator type.
func parseOperator(opStr string) (types.Operator, error) {
	switch opStr {
	case "=", "==", "EQ":
		return types.EQ, nil
	case "!=", "<>", "NE":
		return types.NE, nil
	case ">", "GT":
		return types.GT, nil
	case ">=", "GE":
		return types.GE, nil
	case "<", "LT":
		return types.LT, nil
	case "<=", "LE":
		return types.LE, nil
	case "LIKE", "like":
		return types.LIKE, nil
	case "NOT LIKE", "not like":
		return types.NotLike, nil
	case "IN", "in":
		return types.IN, nil
	case "NOT IN", "not in":
		return types.NotIn, nil
	case "IS NULL", "is null":
		return types.IsNull, nil
	case "IS NOT NULL", "is not null":
		return types.IsNotNull, nil
	case "EXISTS", "exists":
		return types.EXISTS, nil
	case "NOT EXISTS", "not exists":
		return types.NotExists, nil
	default:
		return "", fmt.Errorf("unsupported operator: %s", opStr)
	}
}

// buildFieldExpression converts a FieldExpressionSchema to a FieldExpression with validation.
func buildFieldExpression(schema FieldExpressionSchema, _ string) (FieldExpression, error) {
	var expr FieldExpression

	// Handle different expression types
	switch {
	case schema.Aggregate != "":
		if err := ValidateAggregate(schema.Aggregate); err != nil {
			return expr, err
		}
		field, err := astql.TryF(schema.Field)
		if err != nil {
			return expr, err
		}
		switch schema.Aggregate {
		case "sum", "SUM":
			expr = Sum(field)
		case "avg", "AVG":
			expr = Avg(field)
		case "min", "MIN":
			expr = Min(field)
		case "max", "MAX":
			expr = Max(field)
		case "count", "COUNT":
			expr = CountField(field)
		case "count_distinct", "COUNT_DISTINCT":
			expr = CountDistinct(field)
		}

	case schema.Case != nil:
		caseExpr, err := buildCaseExpression(*schema.Case)
		if err != nil {
			return expr, err
		}
		expr = FieldExpression{Case: &caseExpr}

	case schema.Coalesce != nil:
		params := make([]types.Param, len(schema.Coalesce.Values))
		for i, val := range schema.Coalesce.Values {
			param, err := astql.TryP(val)
			if err != nil {
				return expr, fmt.Errorf("invalid coalesce parameter: %w", err)
			}
			params[i] = param
		}
		coalesceExpr := Coalesce(params...)
		expr = FieldExpression{Coalesce: &coalesceExpr}

	case schema.NullIf != nil:
		fieldParam, err := astql.TryP(schema.NullIf.Field)
		if err != nil {
			return expr, fmt.Errorf("invalid nullif field parameter: %w", err)
		}
		valueParam, err := astql.TryP(schema.NullIf.Value)
		if err != nil {
			return expr, fmt.Errorf("invalid nullif value parameter: %w", err)
		}
		nullifExpr := NullIf(fieldParam, valueParam)
		expr = FieldExpression{NullIf: &nullifExpr}

	case schema.Math != nil:
		mathExpr, err := buildMathExpression(*schema.Math)
		if err != nil {
			return expr, err
		}
		expr = FieldExpression{Math: &mathExpr}

	default:
		// Simple field
		field, err := astql.TryF(schema.Field)
		if err != nil {
			return expr, err
		}
		expr = FieldExpression{Field: field}
	}

	// Add alias if provided
	if schema.Alias != "" {
		expr.Alias = schema.Alias
	}

	return expr, nil
}

// buildCaseExpression converts a CaseSchema to a CaseExpression with validation.
func buildCaseExpression(schema CaseSchema) (CaseExpression, error) {
	if len(schema.When) == 0 {
		return CaseExpression{}, fmt.Errorf("CASE expression requires at least one WHEN clause")
	}

	whenClauses := make([]WhenClause, len(schema.When))
	for i := range schema.When {
		when := &schema.When[i]

		// Validate result parameter
		resultParam, err := astql.TryP(when.Result)
		if err != nil {
			return CaseExpression{}, fmt.Errorf("invalid when result parameter: %w", err)
		}

		// Build condition
		cond, err := buildConditionFromSchema(&when.Condition)
		if err != nil {
			return CaseExpression{}, fmt.Errorf("invalid when condition: %w", err)
		}

		whenClauses[i] = WhenClause{
			Condition: cond,
			Result:    resultParam,
		}
	}

	caseExpr := CaseWithClauses(whenClauses...)
	if schema.Else != "" {
		elseParam, err := astql.TryP(schema.Else)
		if err != nil {
			return CaseExpression{}, fmt.Errorf("invalid else parameter: %w", err)
		}
		caseExpr = caseExpr.Else(elseParam)
	}

	return caseExpr, nil
}

// buildMathExpression converts a MathSchema to a MathExpression with validation.
func buildMathExpression(schema MathSchema) (MathExpression, error) {
	if err := ValidateMathFunction(schema.Function); err != nil {
		return MathExpression{}, err
	}
	field, err := astql.TryF(schema.Field)
	if err != nil {
		return MathExpression{}, err
	}

	switch schema.Function {
	case "round", "ROUND":
		if schema.Arg == nil {
			return MathExpression{}, fmt.Errorf("ROUND requires precision argument")
		}
		argParam, err := astql.TryP(*schema.Arg)
		if err != nil {
			return MathExpression{}, fmt.Errorf("invalid round argument parameter: %w", err)
		}
		return Round(field, argParam), nil

	case "floor", "FLOOR":
		return Floor(field), nil

	case "ceil", "CEIL":
		return Ceil(field), nil

	case "abs", "ABS":
		return Abs(field), nil

	case "power", "POWER":
		if schema.Arg == nil {
			return MathExpression{}, fmt.Errorf("POWER requires exponent argument")
		}
		argParam, err := astql.TryP(*schema.Arg)
		if err != nil {
			return MathExpression{}, fmt.Errorf("invalid power argument parameter: %w", err)
		}
		return Power(field, argParam), nil

	case "sqrt", "SQRT":
		return Sqrt(field), nil

	default:
		return MathExpression{}, fmt.Errorf("unsupported math function: %s", schema.Function)
	}
}

// Allowlists for operations, operators, and directions.
var allowedOperations = map[string]bool{
	"SELECT": true, "select": true,
	"INSERT": true, "insert": true,
	"UPDATE": true, "update": true,
	"DELETE": true, "delete": true,
	"COUNT": true, "count": true,
	"LISTEN": true, "listen": true,
	"NOTIFY": true, "notify": true,
	"UNLISTEN": true, "unlisten": true,
}

var allowedOperators = map[string]bool{
	"=": true, "==": true, "EQ": true,
	"!=": true, "<>": true, "NE": true,
	">": true, "GT": true,
	">=": true, "GE": true,
	"<": true, "LT": true,
	"<=": true, "LE": true,
	"LIKE": true, "like": true,
	"NOT LIKE": true, "not like": true,
	"IN": true, "in": true,
	"NOT IN": true, "not in": true,
	"IS NULL": true, "is null": true,
	"IS NOT NULL": true, "is not null": true,
	"EXISTS": true, "exists": true,
	"NOT EXISTS": true, "not exists": true,
}

var allowedDirections = map[string]bool{
	"ASC": true, "asc": true,
	"DESC": true, "desc": true,
}

var allowedLogicOperators = map[string]bool{
	"AND": true, "and": true,
	"OR": true, "or": true,
}

var allowedJoinTypes = map[string]bool{
	"inner": true, "INNER": true,
	"left": true, "LEFT": true,
	"right": true, "RIGHT": true,
}

var allowedConflictActions = map[string]bool{
	"nothing": true, "NOTHING": true,
	"update": true, "UPDATE": true,
}

var allowedAggregates = map[string]bool{
	"sum": true, "SUM": true,
	"avg": true, "AVG": true,
	"min": true, "MIN": true,
	"max": true, "MAX": true,
	"count": true, "COUNT": true,
	"count_distinct": true, "COUNT_DISTINCT": true,
}

var allowedMathFunctions = map[string]bool{
	"round": true, "ROUND": true,
	"floor": true, "FLOOR": true,
	"ceil": true, "CEIL": true,
	"abs": true, "ABS": true,
	"power": true, "POWER": true,
	"sqrt": true, "SQRT": true,
}

// ValidateOperation checks if an operation is allowed.
func ValidateOperation(op string) error {
	if !allowedOperations[op] {
		return fmt.Errorf("operation '%s' not allowed", op)
	}
	return nil
}

// ValidateOperator checks if an operator is allowed.
func ValidateOperator(op string) error {
	if !allowedOperators[op] {
		return fmt.Errorf("operator '%s' not allowed", op)
	}
	return nil
}

// ValidateDirection checks if a sort direction is allowed.
func ValidateDirection(dir string) error {
	if dir == "" {
		return nil // Empty means default (ASC)
	}
	if !allowedDirections[dir] {
		return fmt.Errorf("direction '%s' not allowed", dir)
	}
	return nil
}

// ValidateLogicOperator checks if a logic operator is allowed.
func ValidateLogicOperator(op string) error {
	if !allowedLogicOperators[op] {
		return fmt.Errorf("logic operator '%s' not allowed", op)
	}
	return nil
}

// ValidateJoinType checks if a join type is allowed.
func ValidateJoinType(jt string) error {
	if !allowedJoinTypes[jt] {
		return fmt.Errorf("join type '%s' not allowed", jt)
	}
	return nil
}

// ValidateConflictAction checks if a conflict action is allowed.
func ValidateConflictAction(action string) error {
	if !allowedConflictActions[action] {
		return fmt.Errorf("conflict action '%s' not allowed", action)
	}
	return nil
}

// ValidateAggregate checks if an aggregate function is allowed.
func ValidateAggregate(agg string) error {
	if !allowedAggregates[agg] {
		return fmt.Errorf("aggregate function '%s' not allowed", agg)
	}
	return nil
}

// ValidateMathFunction checks if a math function is allowed.
func ValidateMathFunction(fn string) error {
	if !allowedMathFunctions[fn] {
		return fmt.Errorf("math function '%s' not allowed", fn)
	}
	return nil
}
