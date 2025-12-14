package astql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zoobzio/astql/internal/types"
)

// countStarSQL is the SQL for COUNT(*) aggregate.
const countStarSQL = "COUNT(*)"

// renderContext tracks rendering state for parameter namespacing and depth limiting.
type renderContext struct {
	usedParams    map[string]bool
	paramCallback func(types.Param) string
	paramPrefix   string
	depth         int
}

// newRenderContext creates a new render context.
func newRenderContext(paramCallback func(types.Param) string) *renderContext {
	return &renderContext{
		depth:         0,
		paramPrefix:   "",
		usedParams:    make(map[string]bool),
		paramCallback: paramCallback,
	}
}

// withSubquery creates a child context for rendering a subquery.
func (ctx *renderContext) withSubquery() (*renderContext, error) {
	if ctx.depth >= types.MaxSubqueryDepth {
		return nil, fmt.Errorf("maximum subquery depth (%d) exceeded", types.MaxSubqueryDepth)
	}

	return &renderContext{
		depth:         ctx.depth + 1,
		paramPrefix:   fmt.Sprintf("sq%d_", ctx.depth+1),
		usedParams:    ctx.usedParams, // Share the same map
		paramCallback: ctx.paramCallback,
	}, nil
}

// addParam adds a parameter with proper namespacing.
func (ctx *renderContext) addParam(param types.Param) string {
	// Apply prefix for subqueries
	if ctx.paramPrefix != "" {
		param = types.Param{Name: ctx.paramPrefix + param.Name}
	}
	return ctx.paramCallback(param)
}

// RenderCompound converts a CompoundQuery to a QueryResult with SQL and parameters.
// Parameters are namespaced per sub-query (q0_, q1_, etc.) to prevent collisions.
func RenderCompound(query *types.CompoundQuery) (*QueryResult, error) {
	var sql strings.Builder
	var params []string
	usedParams := make(map[string]bool)

	queryIndex := 0

	// Helper to create param callback with prefix
	makeParamCallback := func(prefix string) func(types.Param) string {
		return func(param types.Param) string {
			name := prefix + param.Name
			placeholder := ":" + name
			if !usedParams[name] {
				params = append(params, name)
				usedParams[name] = true
			}
			return placeholder
		}
	}

	// Render base query
	baseCtx := newRenderContext(makeParamCallback(fmt.Sprintf("q%d_", queryIndex)))
	sql.WriteString("(")
	if err := renderSelect(query.Base, &sql, baseCtx); err != nil {
		return nil, err
	}
	sql.WriteString(")")
	queryIndex++

	// Render each operand
	for _, operand := range query.Operands {
		sql.WriteString(" ")
		sql.WriteString(string(operand.Operation))
		sql.WriteString(" (")

		opCtx := newRenderContext(makeParamCallback(fmt.Sprintf("q%d_", queryIndex)))
		if err := renderSelect(operand.AST, &sql, opCtx); err != nil {
			return nil, err
		}
		sql.WriteString(")")
		queryIndex++
	}

	// Final ORDER BY
	if len(query.Ordering) > 0 {
		sql.WriteString(" ORDER BY ")
		var orderParts []string
		for _, order := range query.Ordering {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", renderField(order.Field), order.Direction))
		}
		sql.WriteString(strings.Join(orderParts, ", "))
	}

	// Final LIMIT
	if query.Limit != nil {
		fmt.Fprintf(&sql, " LIMIT %d", *query.Limit)
	}

	// Final OFFSET
	if query.Offset != nil {
		fmt.Fprintf(&sql, " OFFSET %d", *query.Offset)
	}

	return &QueryResult{
		SQL:            sql.String(),
		RequiredParams: params,
	}, nil
}

// Render converts an AST to a QueryResult with SQL, parameters, and metadata.
func Render(ast *types.AST) (*QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	var sql strings.Builder
	var params []string
	usedParams := make(map[string]bool)

	// Helper to add a parameter and return its placeholder
	addParam := func(param types.Param) string {
		// Use named parameters for sqlx
		placeholder := ":" + param.Name

		// Track unique parameter names
		if !usedParams[param.Name] {
			params = append(params, param.Name)
			usedParams[param.Name] = true
		}

		return placeholder
	}

	// Create render context for handling subqueries
	ctx := newRenderContext(addParam)

	// Render based on operation
	switch ast.Operation {
	case types.OpSelect:
		if err := renderSelect(ast, &sql, ctx); err != nil {
			return nil, err
		}
	case types.OpInsert:
		if err := renderInsert(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpUpdate:
		if err := renderUpdate(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpDelete:
		if err := renderDelete(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpCount:
		if err := renderCount(ast, &sql, addParam); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	return &QueryResult{
		SQL:            sql.String(),
		RequiredParams: params,
	}, nil
}

func renderSelect(ast *types.AST, sql *strings.Builder, ctx *renderContext) error {
	sql.WriteString("SELECT ")

	if len(ast.DistinctOn) > 0 {
		sql.WriteString("DISTINCT ON (")
		var distinctFields []string
		for _, field := range ast.DistinctOn {
			distinctFields = append(distinctFields, renderField(field))
		}
		sql.WriteString(strings.Join(distinctFields, ", "))
		sql.WriteString(") ")
	} else if ast.Distinct {
		sql.WriteString("DISTINCT ")
	}

	// Render fields and expressions
	if len(ast.Fields) == 0 && len(ast.FieldExpressions) == 0 {
		sql.WriteString("*")
	} else {
		var selections []string

		// Regular fields
		for _, field := range ast.Fields {
			selections = append(selections, renderField(field))
		}

		// Field expressions (aggregates, CASE, etc)
		for i := range ast.FieldExpressions {
			exprStr, err := renderFieldExpression(ast.FieldExpressions[i], ctx)
			if err != nil {
				return err
			}
			selections = append(selections, exprStr)
		}

		sql.WriteString(strings.Join(selections, ", "))
	}

	sql.WriteString(" FROM ")
	sql.WriteString(renderTable(ast.Target))

	// Render JOINs
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(renderTable(join.Table))
		// CROSS JOIN doesn't have ON clause
		if join.Type != types.CrossJoin {
			sql.WriteString(" ON ")
			if err := renderCondition(join.On, sql, ctx); err != nil {
				return err
			}
		}
	}

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		if err := renderCondition(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// GROUP BY
	if len(ast.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		var groupFields []string
		for _, field := range ast.GroupBy {
			groupFields = append(groupFields, renderField(field))
		}
		sql.WriteString(strings.Join(groupFields, ", "))
	}

	// HAVING
	if len(ast.Having) > 0 {
		sql.WriteString(" HAVING ")
		for i, cond := range ast.Having {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			if err := renderCondition(cond, sql, ctx); err != nil {
				return err
			}
		}
	}

	// ORDER BY
	if len(ast.Ordering) > 0 {
		sql.WriteString(" ORDER BY ")
		var orderParts []string
		for _, order := range ast.Ordering {
			var part string
			if order.Operator != "" {
				// Expression-based ordering: field <op> param direction
				part = fmt.Sprintf("%s %s %s %s",
					renderField(order.Field),
					renderOperator(order.Operator),
					ctx.addParam(order.Param),
					order.Direction)
			} else {
				// Simple field ordering
				part = fmt.Sprintf("%s %s", renderField(order.Field), order.Direction)
			}
			// Append NULLS FIRST/LAST if specified
			if order.Nulls != "" {
				part += " " + string(order.Nulls)
			}
			orderParts = append(orderParts, part)
		}
		sql.WriteString(strings.Join(orderParts, ", "))
	}

	// LIMIT
	if ast.Limit != nil {
		fmt.Fprintf(sql, " LIMIT %d", *ast.Limit)
	}

	// OFFSET
	if ast.Offset != nil {
		fmt.Fprintf(sql, " OFFSET %d", *ast.Offset)
	}

	// Row locking (FOR UPDATE, FOR SHARE, etc.)
	if ast.Lock != nil {
		sql.WriteString(" ")
		sql.WriteString(string(*ast.Lock))
	}

	return nil
}

func renderInsert(ast *types.AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("INSERT INTO ")
	sql.WriteString(renderTable(ast.Target))

	if len(ast.Values) == 0 {
		return fmt.Errorf("INSERT requires at least one value set")
	}

	// Extract field names from first value set
	fields := make([]string, 0, len(ast.Values[0]))
	fieldObjs := make([]types.Field, 0, len(ast.Values[0]))
	for field := range ast.Values[0] {
		fieldObjs = append(fieldObjs, field)
	}

	// Sort fields by name for deterministic output
	sort.Slice(fieldObjs, func(i, j int) bool {
		return fieldObjs[i].Name < fieldObjs[j].Name
	})

	for _, field := range fieldObjs {
		fields = append(fields, quoteIdentifier(field.Name))
	}

	sql.WriteString(" (")
	sql.WriteString(strings.Join(fields, ", "))
	sql.WriteString(") VALUES ")

	// Render value sets
	valueSets := make([]string, 0, len(ast.Values))
	for _, valueSet := range ast.Values {
		var values []string
		for _, field := range fieldObjs {
			param := valueSet[field]
			values = append(values, addParam(param))
		}
		valueSets = append(valueSets, "("+strings.Join(values, ", ")+")")
	}
	sql.WriteString(strings.Join(valueSets, ", "))

	// ON CONFLICT
	if ast.OnConflict != nil {
		sql.WriteString(" ON CONFLICT (")
		var conflictFields []string
		for _, field := range ast.OnConflict.Columns {
			conflictFields = append(conflictFields, quoteIdentifier(field.Name))
		}
		sql.WriteString(strings.Join(conflictFields, ", "))
		sql.WriteString(") ")

		switch ast.OnConflict.Action {
		case types.DoNothing:
			sql.WriteString("DO NOTHING")
		case types.DoUpdate:
			sql.WriteString("DO UPDATE SET ")

			// Collect fields for sorting
			conflictUpdateFields := make([]types.Field, 0, len(ast.OnConflict.Updates))
			for field := range ast.OnConflict.Updates {
				conflictUpdateFields = append(conflictUpdateFields, field)
			}

			// Sort fields by name for deterministic output
			sort.Slice(conflictUpdateFields, func(i, j int) bool {
				return conflictUpdateFields[i].Name < conflictUpdateFields[j].Name
			})

			// Build update clauses in sorted order
			var updates []string
			for _, field := range conflictUpdateFields {
				param := ast.OnConflict.Updates[field]
				updates = append(updates, fmt.Sprintf("%s = %s", quoteIdentifier(field.Name), addParam(param)))
			}
			sql.WriteString(strings.Join(updates, ", "))
		}
	}

	// RETURNING
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		var fields []string
		for _, field := range ast.Returning {
			fields = append(fields, renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func renderUpdate(ast *types.AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("UPDATE ")
	sql.WriteString(renderTable(ast.Target))
	sql.WriteString(" SET ")

	// Render updates
	// First collect all fields to sort them
	updateFields := make([]types.Field, 0, len(ast.Updates))
	for field := range ast.Updates {
		updateFields = append(updateFields, field)
	}

	// Sort fields by name for deterministic output
	sort.Slice(updateFields, func(i, j int) bool {
		return updateFields[i].Name < updateFields[j].Name
	})

	// Build update clauses in sorted order
	updates := make([]string, 0, len(ast.Updates))
	for _, field := range updateFields {
		param := ast.Updates[field]
		updates = append(updates, fmt.Sprintf("%s = %s", quoteIdentifier(field.Name), addParam(param)))
	}
	sql.WriteString(strings.Join(updates, ", "))

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		ctx := newRenderContext(addParam)
		if err := renderCondition(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// RETURNING
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		var fields []string
		for _, field := range ast.Returning {
			fields = append(fields, renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func renderDelete(ast *types.AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("DELETE FROM ")
	sql.WriteString(renderTable(ast.Target))

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		ctx := newRenderContext(addParam)
		if err := renderCondition(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// RETURNING
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		var fields []string
		for _, field := range ast.Returning {
			fields = append(fields, renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func renderCount(ast *types.AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("SELECT " + countStarSQL + " FROM ")
	sql.WriteString(renderTable(ast.Target))

	// Render JOINs (COUNT can have JOINs)
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(renderTable(join.Table))
		// CROSS JOIN doesn't have ON clause
		if join.Type != types.CrossJoin {
			sql.WriteString(" ON ")
			ctx := newRenderContext(addParam)
			if err := renderCondition(join.On, sql, ctx); err != nil {
				return err
			}
		}
	}

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		ctx := newRenderContext(addParam)
		if err := renderCondition(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	return nil
}

// quoteIdentifier quotes a PostgreSQL identifier to handle reserved words and special characters.
func quoteIdentifier(name string) string {
	// In PostgreSQL, identifiers are quoted with double quotes
	// We need to escape any existing double quotes by doubling them
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

func renderTable(table types.Table) string {
	quotedName := quoteIdentifier(table.Name)
	if table.Alias != "" {
		// Aliases don't need quoting since they're restricted to single lowercase letters
		return fmt.Sprintf("%s %s", quotedName, table.Alias)
	}
	return quotedName
}

func renderField(field types.Field) string {
	quotedName := quoteIdentifier(field.Name)
	if field.Table != "" {
		// Table aliases don't need quoting (single lowercase letter)
		return fmt.Sprintf("%s.%s", field.Table, quotedName)
	}
	return quotedName
}

func renderAggregateExpression(aggregate types.AggregateFunc, field types.Field) string {
	switch aggregate {
	case types.AggCountField:
		if field.Name == "" {
			return countStarSQL
		}
		return fmt.Sprintf("COUNT(%s)", renderField(field))
	case types.AggCountDistinct:
		return fmt.Sprintf("COUNT(DISTINCT %s)", renderField(field))
	case types.AggSum:
		return fmt.Sprintf("SUM(%s)", renderField(field))
	case types.AggAvg:
		return fmt.Sprintf("AVG(%s)", renderField(field))
	case types.AggMin:
		return fmt.Sprintf("MIN(%s)", renderField(field))
	case types.AggMax:
		return fmt.Sprintf("MAX(%s)", renderField(field))
	default:
		return renderField(field) // Fallback
	}
}

func renderFieldExpression(expr types.FieldExpression, ctx *renderContext) (string, error) {
	var result string

	switch {
	case expr.Case != nil:
		// Render CASE expression
		caseStr, err := renderCaseExpression(*expr.Case, ctx)
		if err != nil {
			return "", err
		}
		result = caseStr
	case expr.Coalesce != nil:
		// Render COALESCE expression
		coalesceStr, err := renderCoalesceExpression(*expr.Coalesce, ctx)
		if err != nil {
			return "", err
		}
		result = coalesceStr
	case expr.NullIf != nil:
		// Render NULLIF expression
		nullifStr, err := renderNullIfExpression(*expr.NullIf, ctx)
		if err != nil {
			return "", err
		}
		result = nullifStr
	case expr.Math != nil:
		// Render math expression
		mathStr, err := renderMathExpression(*expr.Math, ctx)
		if err != nil {
			return "", err
		}
		result = mathStr
	case expr.Cast != nil:
		// Render type cast
		result = fmt.Sprintf("CAST(%s AS %s)", renderField(expr.Cast.Field), string(expr.Cast.CastType))
	case expr.Window != nil:
		// Render window function
		windowStr, err := renderWindowExpression(*expr.Window, ctx)
		if err != nil {
			return "", err
		}
		result = windowStr
	case expr.Aggregate != "":
		result = renderAggregateExpression(expr.Aggregate, expr.Field)
		// Add FILTER clause if present
		if expr.Filter != nil {
			var filterSQL strings.Builder
			filterSQL.WriteString(" FILTER (WHERE ")
			if err := renderCondition(expr.Filter, &filterSQL, ctx); err != nil {
				return "", err
			}
			filterSQL.WriteString(")")
			result += filterSQL.String()
		}
	default:
		result = renderField(expr.Field)
	}

	if expr.Alias != "" {
		result += " AS " + quoteIdentifier(expr.Alias)
	}

	return result, nil
}

func renderCondition(cond types.ConditionItem, sql *strings.Builder, ctx *renderContext) error {
	switch c := cond.(type) {
	case types.Condition:
		sql.WriteString(renderSimpleCondition(c, ctx.addParam))
	case types.ConditionGroup:
		// Skip empty condition groups
		if len(c.Conditions) == 0 {
			return fmt.Errorf("empty condition group")
		}
		sql.WriteString("(")
		for i, subCond := range c.Conditions {
			if i > 0 {
				fmt.Fprintf(sql, " %s ", c.Logic)
			}
			if err := renderCondition(subCond, sql, ctx); err != nil {
				return err
			}
		}
		sql.WriteString(")")
	case types.FieldComparison:
		fmt.Fprintf(sql, "%s %s %s",
			renderField(c.LeftField),
			renderOperator(c.Operator),
			renderField(c.RightField))
	case types.SubqueryCondition:
		if err := renderSubqueryCondition(c, sql, ctx); err != nil {
			return err
		}
	case types.AggregateCondition:
		sql.WriteString(renderAggregateCondition(c, ctx.addParam))
	case types.BetweenCondition:
		sql.WriteString(renderBetweenCondition(c, ctx.addParam))
	default:
		return fmt.Errorf("unknown condition type: %T", c)
	}
	return nil
}

func renderSimpleCondition(cond types.Condition, addParam func(types.Param) string) string {
	field := renderField(cond.Field)
	op := renderOperator(cond.Operator)

	switch cond.Operator {
	case types.IsNull:
		return fmt.Sprintf("%s IS NULL", field)
	case types.IsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", field)
	case types.IN:
		// PostgreSQL: field = ANY(:param) for array parameters
		return fmt.Sprintf("%s = ANY(%s)", field, addParam(cond.Value))
	case types.NotIn:
		// PostgreSQL: field != ALL(:param) for array parameters
		return fmt.Sprintf("%s != ALL(%s)", field, addParam(cond.Value))
	default:
		return fmt.Sprintf("%s %s %s", field, op, addParam(cond.Value))
	}
}

// Examples: COUNT(*) > :min_count, SUM("amount") >= :threshold.
func renderAggregateCondition(cond types.AggregateCondition, addParam func(types.Param) string) string {
	var aggExpr string

	switch cond.Func {
	case types.AggCountField:
		if cond.Field == nil {
			aggExpr = countStarSQL
		} else {
			aggExpr = fmt.Sprintf("COUNT(%s)", renderField(*cond.Field))
		}
	case types.AggCountDistinct:
		if cond.Field == nil {
			aggExpr = countStarSQL // COUNT DISTINCT without field falls back to COUNT(*)
		} else {
			aggExpr = fmt.Sprintf("COUNT(DISTINCT %s)", renderField(*cond.Field))
		}
	case types.AggSum:
		if cond.Field == nil {
			aggExpr = "SUM(*)" // Invalid but let DB handle it
		} else {
			aggExpr = fmt.Sprintf("SUM(%s)", renderField(*cond.Field))
		}
	case types.AggAvg:
		if cond.Field == nil {
			aggExpr = "AVG(*)"
		} else {
			aggExpr = fmt.Sprintf("AVG(%s)", renderField(*cond.Field))
		}
	case types.AggMin:
		if cond.Field == nil {
			aggExpr = "MIN(*)"
		} else {
			aggExpr = fmt.Sprintf("MIN(%s)", renderField(*cond.Field))
		}
	case types.AggMax:
		if cond.Field == nil {
			aggExpr = "MAX(*)"
		} else {
			aggExpr = fmt.Sprintf("MAX(%s)", renderField(*cond.Field))
		}
	default:
		aggExpr = "UNKNOWN_AGG(*)"
	}

	return fmt.Sprintf("%s %s %s", aggExpr, renderOperator(cond.Operator), addParam(cond.Value))
}

// renderBetweenCondition renders a BETWEEN condition.
func renderBetweenCondition(cond types.BetweenCondition, addParam func(types.Param) string) string {
	field := renderField(cond.Field)
	op := "BETWEEN"
	if cond.Negated {
		op = "NOT BETWEEN"
	}
	return fmt.Sprintf("%s %s %s AND %s", field, op, addParam(cond.Low), addParam(cond.High))
}

func renderSubqueryCondition(cond types.SubqueryCondition, sql *strings.Builder, ctx *renderContext) error {
	switch cond.Operator {
	case types.EXISTS, types.NotExists:
		// EXISTS/NOT EXISTS don't need a field
		sql.WriteString(string(cond.Operator))
		sql.WriteString(" ")
	default:
		// IN/NOT IN need a field
		if cond.Field == nil {
			return fmt.Errorf("operator %s requires a field", cond.Operator)
		}
		sql.WriteString(renderField(*cond.Field))
		sql.WriteString(" ")
		sql.WriteString(string(cond.Operator))
		sql.WriteString(" ")
	}

	// Render the subquery
	sql.WriteString("(")
	if err := renderSubquery(cond.Subquery, sql, ctx); err != nil {
		return err
	}
	sql.WriteString(")")

	return nil
}

func renderSubquery(subquery types.Subquery, sql *strings.Builder, ctx *renderContext) error {
	// Create a new context for the subquery
	subCtx, err := ctx.withSubquery()
	if err != nil {
		return err
	}

	ast := subquery.AST
	// Render full query AST
	return renderSelect(ast, sql, subCtx)
}

func renderCaseExpression(expr types.CaseExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder
	sql.WriteString("CASE")

	for _, when := range expr.WhenClauses {
		sql.WriteString(" WHEN ")
		if err := renderCondition(when.Condition, &sql, ctx); err != nil {
			return "", err
		}
		sql.WriteString(" THEN ")
		sql.WriteString(ctx.addParam(when.Result))
	}

	if expr.ElseValue != nil {
		sql.WriteString(" ELSE ")
		sql.WriteString(ctx.addParam(*expr.ElseValue))
	}

	sql.WriteString(" END")
	return sql.String(), nil
}

func renderCoalesceExpression(expr types.CoalesceExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder
	sql.WriteString("COALESCE(")

	params := make([]string, 0, len(expr.Values))
	for _, value := range expr.Values {
		params = append(params, ctx.addParam(value))
	}
	sql.WriteString(strings.Join(params, ", "))
	sql.WriteString(")")
	return sql.String(), nil
}

func renderNullIfExpression(expr types.NullIfExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder
	sql.WriteString("NULLIF(")
	sql.WriteString(ctx.addParam(expr.Value1))
	sql.WriteString(", ")
	sql.WriteString(ctx.addParam(expr.Value2))
	sql.WriteString(")")
	return sql.String(), nil
}

func renderMathExpression(expr types.MathExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder

	switch expr.Function {
	case types.MathRound:
		sql.WriteString("ROUND(")
		sql.WriteString(renderField(expr.Field))
		if expr.Precision != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.Precision))
		}
		sql.WriteString(")")
	case types.MathFloor:
		sql.WriteString("FLOOR(")
		sql.WriteString(renderField(expr.Field))
		sql.WriteString(")")
	case types.MathCeil:
		sql.WriteString("CEIL(")
		sql.WriteString(renderField(expr.Field))
		sql.WriteString(")")
	case types.MathAbs:
		sql.WriteString("ABS(")
		sql.WriteString(renderField(expr.Field))
		sql.WriteString(")")
	case types.MathPower:
		sql.WriteString("POWER(")
		sql.WriteString(renderField(expr.Field))
		if expr.Exponent != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.Exponent))
		} else {
			return "", fmt.Errorf("POWER requires an exponent parameter")
		}
		sql.WriteString(")")
	case types.MathSqrt:
		sql.WriteString("SQRT(")
		sql.WriteString(renderField(expr.Field))
		sql.WriteString(")")
	default:
		return "", fmt.Errorf("unsupported math function: %s", expr.Function)
	}

	return sql.String(), nil
}

func renderWindowExpression(expr types.WindowExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder

	// Render the function name and arguments
	switch expr.Function {
	case types.WinRowNumber, types.WinRank, types.WinDenseRank:
		// No arguments
		sql.WriteString(string(expr.Function))
		sql.WriteString("()")
	case types.WinNtile:
		sql.WriteString("NTILE(")
		if expr.NtileParam != nil {
			sql.WriteString(ctx.addParam(*expr.NtileParam))
		} else {
			return "", fmt.Errorf("NTILE requires a parameter")
		}
		sql.WriteString(")")
	case types.WinLag, types.WinLead:
		sql.WriteString(string(expr.Function))
		sql.WriteString("(")
		if expr.Field != nil {
			sql.WriteString(renderField(*expr.Field))
		} else {
			return "", fmt.Errorf("%s requires a field", expr.Function)
		}
		if expr.LagOffset != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.LagOffset))
		}
		if expr.LagDefault != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.LagDefault))
		}
		sql.WriteString(")")
	case types.WinFirstValue, types.WinLastValue:
		sql.WriteString(string(expr.Function))
		sql.WriteString("(")
		if expr.Field != nil {
			sql.WriteString(renderField(*expr.Field))
		} else {
			return "", fmt.Errorf("%s requires a field", expr.Function)
		}
		sql.WriteString(")")
	default:
		// Aggregate window function (SUM OVER, COUNT OVER, etc.)
		if expr.Aggregate != "" {
			if expr.Field != nil {
				sql.WriteString(renderAggregateExpression(expr.Aggregate, *expr.Field))
			} else {
				// COUNT(*) OVER case
				sql.WriteString(countStarSQL)
			}
		} else {
			return "", fmt.Errorf("unknown window function: %s", expr.Function)
		}
	}

	// Render OVER clause
	sql.WriteString(" OVER (")

	var overParts []string

	// PARTITION BY
	if len(expr.Window.PartitionBy) > 0 {
		var partitionFields []string
		for _, field := range expr.Window.PartitionBy {
			partitionFields = append(partitionFields, renderField(field))
		}
		overParts = append(overParts, "PARTITION BY "+strings.Join(partitionFields, ", "))
	}

	// ORDER BY
	if len(expr.Window.OrderBy) > 0 {
		var orderParts []string
		for _, order := range expr.Window.OrderBy {
			var part string
			if order.Operator != "" {
				part = fmt.Sprintf("%s %s %s %s",
					renderField(order.Field),
					renderOperator(order.Operator),
					ctx.addParam(order.Param),
					order.Direction)
			} else {
				part = fmt.Sprintf("%s %s", renderField(order.Field), order.Direction)
			}
			if order.Nulls != "" {
				part += " " + string(order.Nulls)
			}
			orderParts = append(orderParts, part)
		}
		overParts = append(overParts, "ORDER BY "+strings.Join(orderParts, ", "))
	}

	// Frame clause
	if expr.Window.FrameStart != "" {
		framePart := "ROWS BETWEEN " + string(expr.Window.FrameStart) + " AND "
		if expr.Window.FrameEnd != "" {
			framePart += string(expr.Window.FrameEnd)
		} else {
			framePart += "CURRENT ROW"
		}
		overParts = append(overParts, framePart)
	}

	sql.WriteString(strings.Join(overParts, " "))
	sql.WriteString(")")

	return sql.String(), nil
}

func renderOperator(op types.Operator) string {
	switch op {
	case types.EQ:
		return "="
	case types.NE:
		return "!="
	case types.GT:
		return ">"
	case types.GE:
		return ">="
	case types.LT:
		return "<"
	case types.LE:
		return "<="
	case types.LIKE:
		return "LIKE"
	case types.NotLike:
		return "NOT LIKE"
	case types.ILIKE:
		return "ILIKE"
	case types.NotILike:
		return "NOT ILIKE"
	case types.RegexMatch:
		return "~"
	case types.RegexIMatch:
		return "~*"
	case types.NotRegexMatch:
		return "!~"
	case types.NotRegexIMatch:
		return "!~*"
	case types.ArrayContains:
		return "@>"
	case types.ArrayContainedBy:
		return "<@"
	case types.ArrayOverlap:
		return "&&"
	case types.IN:
		return "IN"
	case types.NotIn:
		return "NOT IN"
	case types.EXISTS:
		return "EXISTS"
	case types.NotExists:
		return "NOT EXISTS"
	case types.VectorL2Distance:
		return "<->"
	case types.VectorInnerProduct:
		return "<#>"
	case types.VectorCosineDistance:
		return "<=>"
	case types.VectorL1Distance:
		return "<+>"
	default:
		return string(op)
	}
}
