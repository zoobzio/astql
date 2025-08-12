package postgres

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// Operation constants.
const (
	OpUpdate      = "UPDATE"
	OpUpdateLower = "update"
)

// PostgreSQL-specific schema implementation for declarative query building

// serialized from YAML/JSON and converted to a PostgreSQL AST.
//
//nolint:govet // fieldalignment: Logical grouping is preferred for readability
type QuerySchema struct {
	Limit            *int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	OnConflict       *ConflictSchema         `json:"on_conflict,omitempty" yaml:"on_conflict,omitempty"`
	Updates          map[string]string       `json:"updates,omitempty" yaml:"updates,omitempty"`
	Where            *ConditionSchema        `json:"where,omitempty" yaml:"where,omitempty"`
	Offset           *int                    `json:"offset,omitempty" yaml:"offset,omitempty"`
	Table            string                  `json:"table" yaml:"table"`
	Alias            string                  `json:"alias,omitempty" yaml:"alias,omitempty"`
	Operation        string                  `json:"operation" yaml:"operation"`
	FieldExpressions []FieldExpressionSchema `json:"field_expressions,omitempty" yaml:"field_expressions,omitempty"`
	Having           []ConditionSchema       `json:"having,omitempty" yaml:"having,omitempty"`
	OrderBy          []OrderSchema           `json:"order_by,omitempty" yaml:"order_by,omitempty"`
	GroupBy          []string                `json:"group_by,omitempty" yaml:"group_by,omitempty"`
	Joins            []JoinSchema            `json:"joins,omitempty" yaml:"joins,omitempty"`
	Cases            []CaseFieldSchema       `json:"cases,omitempty" yaml:"cases,omitempty"`
	Values           []map[string]string     `json:"values,omitempty" yaml:"values,omitempty"`
	Fields           []string                `json:"fields,omitempty" yaml:"fields,omitempty"`
	Returning        []string                `json:"returning,omitempty" yaml:"returning,omitempty"`
	Distinct         bool                    `json:"distinct,omitempty" yaml:"distinct,omitempty"`
	NotifyPayload    string                  `json:"notify_payload,omitempty" yaml:"notify_payload,omitempty"` // For NOTIFY operations
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
	Logic      string            `json:"logic,omitempty" yaml:"logic,omitempty"` // "AND" or "OR"
	Conditions []ConditionSchema `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// OrderSchema represents ordering in declarative form.
type OrderSchema struct {
	Field     string `json:"field" yaml:"field"`
	Direction string `json:"direction,omitempty" yaml:"direction,omitempty"` // defaults to ASC
}

// CaseFieldSchema represents a CASE expression in SELECT.
//
//nolint:govet // fieldalignment: readability is more important than memory optimization here
type CaseFieldSchema struct {
	Case  CaseSchema `json:"case" yaml:"case"`
	Alias string     `json:"alias,omitempty" yaml:"alias,omitempty"`
}

// CaseSchema represents a CASE expression.
type CaseSchema struct {
	Else string       `json:"else,omitempty" yaml:"else,omitempty"`
	When []WhenSchema `json:"when" yaml:"when"`
}

// WhenSchema represents a WHEN clause.
type WhenSchema struct {
	Result    string          `json:"result" yaml:"result"`
	Condition ConditionSchema `json:"condition" yaml:"condition"`
}

// JoinSchema represents a JOIN clause.
type JoinSchema struct {
	Type  string          `json:"type" yaml:"type"` // "inner", "left", "right"
	Table string          `json:"table" yaml:"table"`
	Alias string          `json:"alias,omitempty" yaml:"alias,omitempty"`
	On    ConditionSchema `json:"on" yaml:"on"`
}

// FieldExpressionSchema represents a field with optional aggregate or expression.
type FieldExpressionSchema struct {
	Field     string          `json:"field" yaml:"field"`
	Aggregate string          `json:"aggregate,omitempty" yaml:"aggregate,omitempty"` // "sum", "avg", "min", "max", "count", "count_distinct"
	Case      *CaseSchema     `json:"case,omitempty" yaml:"case,omitempty"`
	Coalesce  *CoalesceSchema `json:"coalesce,omitempty" yaml:"coalesce,omitempty"`
	NullIf    *NullIfSchema   `json:"nullif,omitempty" yaml:"nullif,omitempty"`
	Math      *MathSchema     `json:"math,omitempty" yaml:"math,omitempty"`
	Alias     string          `json:"alias,omitempty" yaml:"alias,omitempty"`
}

// CoalesceSchema represents a COALESCE expression.
type CoalesceSchema struct {
	Values []string `json:"values" yaml:"values"` // param names
}

// NullIfSchema represents a NULLIF expression.
type NullIfSchema struct {
	Field string `json:"field" yaml:"field"`
	Value string `json:"value" yaml:"value"` // param name
}

// MathSchema represents a math function.
type MathSchema struct {
	Arg      *string `json:"arg,omitempty" yaml:"arg,omitempty"`
	Function string  `json:"function" yaml:"function"`
	Field    string  `json:"field" yaml:"field"`
}

// ConflictSchema represents ON CONFLICT clause.
type ConflictSchema struct {
	Updates map[string]string `json:"updates,omitempty" yaml:"updates,omitempty"`
	Action  string            `json:"action" yaml:"action"`
	Columns []string          `json:"columns" yaml:"columns"`
}

// BuildFromSchema converts a QuerySchema to a AST.
func BuildFromSchema(schema *QuerySchema) (*AST, error) {
	if schema.Operation == "" {
		return nil, fmt.Errorf("operation is required")
	}
	if schema.Table == "" {
		return nil, fmt.Errorf("table is required")
	}

	// Create table with validation
	var table astql.Table
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
	case OpUpdate, OpUpdateLower:
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

	// Add fields and expressions for SELECT
	if schema.Operation == "SELECT" || schema.Operation == "select" {
		// Add simple fields
		if len(schema.Fields) > 0 {
			fields := make([]astql.Field, len(schema.Fields))
			for i, f := range schema.Fields {
				field, fieldErr := astql.TryF(f)
				if fieldErr != nil {
					return nil, fmt.Errorf("invalid field '%s': %w", f, fieldErr)
				}
				fields[i] = field
			}
			builder = builder.Fields(fields...)
		}

		// Add field expressions (aggregates, etc)
		for _, expr := range schema.FieldExpressions {
			fieldExpr, exprErr := buildFieldExpression(expr)
			if exprErr != nil {
				return nil, fmt.Errorf("invalid field expression: %w", exprErr)
			}
			builder = builder.SelectExpr(fieldExpr)
		}

		// Add CASE expressions
		for _, caseField := range schema.Cases {
			caseExpr, caseErr := buildCaseExpression(caseField.Case)
			if caseErr != nil {
				return nil, fmt.Errorf("invalid case expression: %w", caseErr)
			}
			if caseField.Alias != "" {
				caseExpr.Alias = caseField.Alias
			}
			builder = builder.SelectCase(caseExpr)
		}

		// Add DISTINCT
		if schema.Distinct {
			builder = builder.Distinct()
		}

		// If no fields specified, it defaults to SELECT *
	}

	// Add JOINs
	for i := range schema.Joins {
		join := &schema.Joins[i]
		var joinTable astql.Table
		if join.Alias != "" {
			joinTable, err = astql.TryT(join.Table, join.Alias)
		} else {
			joinTable, err = astql.TryT(join.Table)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid join table '%s': %w", join.Table, err)
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
		default:
			return nil, fmt.Errorf("unsupported join type: %s", join.Type)
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

	// Add GROUP BY
	if len(schema.GroupBy) > 0 {
		groupFields := make([]astql.Field, len(schema.GroupBy))
		for i, f := range schema.GroupBy {
			field, err := astql.TryF(f)
			if err != nil {
				return nil, fmt.Errorf("invalid group by field '%s': %w", f, err)
			}
			groupFields[i] = field
		}
		builder = builder.GroupBy(groupFields...)
	}

	// Add HAVING
	if len(schema.Having) > 0 {
		havingConditions := make([]astql.Condition, 0, len(schema.Having))
		for i := range schema.Having {
			cond, err := buildConditionFromSchema(&schema.Having[i])
			if err != nil {
				return nil, fmt.Errorf("invalid having condition: %w", err)
			}
			// Convert ConditionItem to Condition if it's a simple condition
			if simpleCond, ok := cond.(astql.Condition); ok {
				havingConditions = append(havingConditions, simpleCond)
			} else {
				return nil, fmt.Errorf("complex conditions not supported in HAVING clause")
			}
		}
		builder = builder.Having(havingConditions...)
	}

	// Add ORDER BY
	for _, order := range schema.OrderBy {
		dir := astql.ASC
		if order.Direction == "DESC" || order.Direction == "desc" {
			dir = astql.DESC
		}
		field, err := astql.TryF(order.Field)
		if err != nil {
			return nil, fmt.Errorf("invalid order by field '%s': %w", order.Field, err)
		}
		builder = builder.OrderBy(field, dir)
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
	case OpUpdate, OpUpdateLower:
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

		// Add ON CONFLICT
		if schema.OnConflict != nil {
			conflictFields := make([]astql.Field, len(schema.OnConflict.Columns))
			for i, col := range schema.OnConflict.Columns {
				field, err := astql.TryF(col)
				if err != nil {
					return nil, fmt.Errorf("invalid conflict column '%s': %w", col, err)
				}
				conflictFields[i] = field
			}

			conflictBuilder := builder.OnConflict(conflictFields...)

			switch schema.OnConflict.Action {
			case "nothing", "NOTHING":
				builder = conflictBuilder.DoNothing()
			case OpUpdateLower, OpUpdate:
				if len(schema.OnConflict.Updates) == 0 {
					return nil, fmt.Errorf("ON CONFLICT DO UPDATE requires updates")
				}
				updates := make(map[astql.Field]astql.Param)
				for field, param := range schema.OnConflict.Updates {
					f, err := astql.TryF(field)
					if err != nil {
						return nil, fmt.Errorf("invalid conflict update field '%s': %w", field, err)
					}
					updates[f] = astql.P(param)
				}
				builder = conflictBuilder.DoUpdate(updates)
			default:
				return nil, fmt.Errorf("unsupported conflict action: %s", schema.OnConflict.Action)
			}
		}
	}

	// Add RETURNING clause (for INSERT, UPDATE, DELETE)
	if len(schema.Returning) > 0 {
		returningFields := make([]astql.Field, len(schema.Returning))
		for i, f := range schema.Returning {
			field, err := astql.TryF(f)
			if err != nil {
				return nil, fmt.Errorf("invalid returning field '%s': %w", f, err)
			}
			returningFields[i] = field
		}
		builder = builder.Returning(returningFields...)
	}

	return builder.Build()
}

// buildConditionFromSchema converts a ConditionSchema to a ConditionItem.
func buildConditionFromSchema(schema *ConditionSchema) (astql.ConditionItem, error) {
	// Check if it's a group condition
	if schema.Logic != "" {
		if len(schema.Conditions) == 0 {
			return nil, fmt.Errorf("condition group requires at least one condition")
		}

		conditions := make([]astql.ConditionItem, len(schema.Conditions))
		for i := range schema.Conditions {
			cond, err := buildConditionFromSchema(&schema.Conditions[i])
			if err != nil {
				return nil, fmt.Errorf("condition %d: %w", i, err)
			}
			conditions[i] = cond
		}

		switch schema.Logic {
		case "AND", "and":
			return astql.And(conditions...), nil
		case "OR", "or":
			return astql.Or(conditions...), nil
		default:
			return nil, fmt.Errorf("invalid logic operator: %s", schema.Logic)
		}
	}

	// Check if it's a subquery condition
	if schema.Subquery != nil {
		if schema.Operator == "" {
			return nil, fmt.Errorf("operator is required for subquery condition")
		}

		// Convert operator string to Operator type
		op, err := parseOperator(schema.Operator)
		if err != nil {
			return nil, err
		}

		// Build the subquery
		subqueryAST, err := BuildFromSchema(schema.Subquery)
		if err != nil {
			return nil, fmt.Errorf("invalid subquery: %w", err)
		}

		// Create subquery directly from the AST
		subquery := astql.Subquery{AST: subqueryAST}

		// Handle EXISTS/NOT EXISTS vs IN/NOT IN
		switch op {
		case astql.EXISTS, astql.NotExists:
			if schema.Field != "" {
				return nil, fmt.Errorf("%s operator does not take a field", op)
			}
			return astql.CSubExists(op, subquery), nil
		case astql.IN, astql.NotIn:
			if schema.Field == "" {
				return nil, fmt.Errorf("%s operator requires a field", op)
			}
			field, err := astql.TryF(schema.Field)
			if err != nil {
				return nil, fmt.Errorf("invalid subquery field '%s': %w", schema.Field, err)
			}
			return astql.CSub(field, op, subquery), nil
		default:
			return nil, fmt.Errorf("operator %s cannot be used with subqueries", op)
		}
	}

	// Check if it's a field-to-field comparison
	if schema.LeftField != "" && schema.RightField != "" {
		if schema.Operator == "" {
			return nil, fmt.Errorf("operator is required for field comparison")
		}

		// Convert operator string to Operator type
		op, err := parseOperator(schema.Operator)
		if err != nil {
			return nil, err
		}

		leftField, err := astql.TryF(schema.LeftField)
		if err != nil {
			return nil, fmt.Errorf("invalid left field '%s': %w", schema.LeftField, err)
		}
		rightField, err := astql.TryF(schema.RightField)
		if err != nil {
			return nil, fmt.Errorf("invalid right field '%s': %w", schema.RightField, err)
		}
		return astql.CF(leftField, op, rightField), nil
	}

	// It's a simple condition
	if schema.Field == "" {
		return nil, fmt.Errorf("field is required for condition")
	}
	if schema.Operator == "" {
		return nil, fmt.Errorf("operator is required for condition")
	}
	if schema.Param == "" {
		return nil, fmt.Errorf("param is required for condition")
	}

	// Convert operator string to Operator type
	op, err := parseOperator(schema.Operator)
	if err != nil {
		return nil, err
	}

	field, err := astql.TryF(schema.Field)
	if err != nil {
		return nil, fmt.Errorf("invalid condition field '%s': %w", schema.Field, err)
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
	case "LIKE", "like":
		return astql.LIKE, nil
	case "NOT LIKE", "not like":
		return astql.NotLike, nil
	case "IN", "in":
		return astql.IN, nil
	case "NOT IN", "not in":
		return astql.NotIn, nil
	case "IS NULL", "is null":
		return astql.IsNull, nil
	case "IS NOT NULL", "is not null":
		return astql.IsNotNull, nil
	case "EXISTS", "exists":
		return astql.EXISTS, nil
	case "NOT EXISTS", "not exists":
		return astql.NotExists, nil
	default:
		return "", fmt.Errorf("unsupported operator: %s", opStr)
	}
}

// buildFieldExpression converts a FieldExpressionSchema to a FieldExpression.
func buildFieldExpression(schema FieldExpressionSchema) (FieldExpression, error) {
	var expr FieldExpression

	// Handle different expression types
	switch {
	case schema.Aggregate != "":
		field, err := astql.TryF(schema.Field)
		if err != nil {
			return expr, fmt.Errorf("invalid aggregate field '%s': %w", schema.Field, err)
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
		default:
			return expr, fmt.Errorf("unsupported aggregate function: %s", schema.Aggregate)
		}
	case schema.Case != nil:
		caseExpr, err := buildCaseExpression(*schema.Case)
		if err != nil {
			return expr, err
		}
		expr = FieldExpression{Case: &caseExpr}
	case schema.Coalesce != nil:
		params := make([]astql.Param, len(schema.Coalesce.Values))
		for i, val := range schema.Coalesce.Values {
			params[i] = astql.P(val)
		}
		coalesceExpr := Coalesce(params...)
		expr = FieldExpression{Coalesce: &coalesceExpr}
	case schema.NullIf != nil:
		// NullIf takes two params, but schema has field+param
		// Convert field to param by using its name as a literal
		fieldParam := astql.P(schema.NullIf.Field)
		nullifExpr := NullIf(fieldParam, astql.P(schema.NullIf.Value))
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
			return expr, fmt.Errorf("invalid field '%s': %w", schema.Field, err)
		}
		expr = FieldExpression{Field: field}
	}

	// Add alias if provided
	if schema.Alias != "" {
		expr.Alias = schema.Alias
	}

	return expr, nil
}

// buildCaseExpression converts a CaseSchema to a CaseExpression.
func buildCaseExpression(schema CaseSchema) (CaseExpression, error) {
	if len(schema.When) == 0 {
		return CaseExpression{}, fmt.Errorf("CASE expression requires at least one WHEN clause")
	}

	whenClauses := make([]WhenClause, len(schema.When))
	for i := range schema.When {
		when := &schema.When[i]
		cond, err := buildConditionFromSchema(&when.Condition)
		if err != nil {
			return CaseExpression{}, fmt.Errorf("invalid WHEN condition: %w", err)
		}
		whenClauses[i] = WhenClause{
			Condition: cond,
			Result:    astql.P(when.Result),
		}
	}

	caseExpr := CaseWithClauses(whenClauses...)
	if schema.Else != "" {
		caseExpr = caseExpr.Else(astql.P(schema.Else))
	}

	return caseExpr, nil
}

// buildMathExpression converts a MathSchema to a MathExpression.
func buildMathExpression(schema MathSchema) (MathExpression, error) {
	field, err := astql.TryF(schema.Field)
	if err != nil {
		return MathExpression{}, fmt.Errorf("invalid math field '%s': %w", schema.Field, err)
	}

	switch schema.Function {
	case "round", "ROUND":
		if schema.Arg == nil {
			return MathExpression{}, fmt.Errorf("ROUND requires precision argument")
		}
		return Round(field, astql.P(*schema.Arg)), nil
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
		return Power(field, astql.P(*schema.Arg)), nil
	case "sqrt", "SQRT":
		return Sqrt(field), nil
	default:
		return MathExpression{}, fmt.Errorf("unsupported math function: %s", schema.Function)
	}
}
