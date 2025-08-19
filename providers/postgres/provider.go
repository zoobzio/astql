package postgres

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// Provider renders PostgreSQL AST to SQL with named parameters.
type Provider struct{}

// NewProvider creates a new PostgreSQL provider.
func NewProvider() *Provider {
	return &Provider{}
}

// Render converts a PostgreSQL AST to a QueryResult with SQL, parameters, and metadata.
func (p *Provider) Render(ast *AST) (*astql.QueryResult, error) {
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
		if err := p.renderSelectWithContext(ast, &sql, ctx); err != nil {
			return nil, err
		}
	case types.OpInsert:
		if err := p.renderInsert(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpUpdate:
		if err := p.renderUpdate(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpDelete:
		if err := p.renderDelete(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpCount:
		if err := p.renderCount(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case types.OpListen:
		p.renderListen(ast, &sql)
	case types.OpNotify:
		p.renderNotify(ast, &sql, addParam)
	case types.OpUnlisten:
		p.renderUnlisten(ast, &sql)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	return &astql.QueryResult{
		SQL:            sql.String(),
		RequiredParams: params,
	}, nil
}

func (p *Provider) renderSelectWithContext(ast *AST, sql *strings.Builder, ctx *renderContext) error {
	sql.WriteString("SELECT ")

	if ast.Distinct {
		sql.WriteString("DISTINCT ")
	}

	// Render fields and expressions
	if len(ast.Fields) == 0 && len(ast.FieldExpressions) == 0 {
		sql.WriteString("*")
	} else {
		var selections []string

		// Regular fields
		for _, field := range ast.Fields {
			selections = append(selections, p.renderField(field))
		}

		// Field expressions (aggregates, CASE, etc)
		for _, expr := range ast.FieldExpressions {
			exprStr, err := p.renderFieldExpressionWithContext(expr, ctx)
			if err != nil {
				return err
			}
			selections = append(selections, exprStr)
		}

		sql.WriteString(strings.Join(selections, ", "))
	}

	sql.WriteString(" FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// Render JOINs
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(p.renderTable(join.Table))
		sql.WriteString(" ON ")
		if err := p.renderConditionWithContext(join.On, sql, ctx); err != nil {
			return err
		}
	}

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		if err := p.renderConditionWithContext(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// GROUP BY
	if len(ast.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		var groupFields []string
		for _, field := range ast.GroupBy {
			groupFields = append(groupFields, p.renderField(field))
		}
		sql.WriteString(strings.Join(groupFields, ", "))
	}

	// HAVING
	if len(ast.Having) > 0 {
		sql.WriteString(" HAVING ")
		var havingConds []string
		for _, cond := range ast.Having {
			havingConds = append(havingConds, p.renderSimpleCondition(cond, ctx.addParam))
		}
		sql.WriteString(strings.Join(havingConds, " AND "))
	}

	// ORDER BY
	if len(ast.Ordering) > 0 {
		sql.WriteString(" ORDER BY ")
		var orderParts []string
		for _, order := range ast.Ordering {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", p.renderField(order.Field), order.Direction))
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

	return nil
}

func (p *Provider) renderInsert(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("INSERT INTO ")
	sql.WriteString(p.renderTable(ast.Target))

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
		case DoNothing:
			sql.WriteString("DO NOTHING")
		case DoUpdate:
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
			fields = append(fields, p.renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func (p *Provider) renderUpdate(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("UPDATE ")
	sql.WriteString(p.renderTable(ast.Target))
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
		if err := p.renderConditionWithContext(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// RETURNING
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		var fields []string
		for _, field := range ast.Returning {
			fields = append(fields, p.renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func (p *Provider) renderDelete(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("DELETE FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		ctx := newRenderContext(addParam)
		if err := p.renderConditionWithContext(ast.WhereClause, sql, ctx); err != nil {
			return err
		}
	}

	// RETURNING
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		var fields []string
		for _, field := range ast.Returning {
			fields = append(fields, p.renderField(field))
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	return nil
}

func (p *Provider) renderCount(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("SELECT COUNT(*) FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// Render JOINs (COUNT can have JOINs)
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(p.renderTable(join.Table))
		sql.WriteString(" ON ")
		ctx := newRenderContext(addParam)
		if err := p.renderConditionWithContext(join.On, sql, ctx); err != nil {
			return err
		}
	}

	// WHERE clause
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		ctx := newRenderContext(addParam)
		if err := p.renderConditionWithContext(ast.WhereClause, sql, ctx); err != nil {
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

func (*Provider) renderTable(table types.Table) string {
	quotedName := quoteIdentifier(table.Name)
	if table.Alias != "" {
		// Aliases don't need quoting since they're restricted to single lowercase letters
		return fmt.Sprintf("%s %s", quotedName, table.Alias)
	}
	return quotedName
}

func (*Provider) renderField(field types.Field) string {
	quotedName := quoteIdentifier(field.Name)
	if field.Table != "" {
		// Table aliases don't need quoting (single lowercase letter)
		return fmt.Sprintf("%s.%s", field.Table, quotedName)
	}
	return quotedName
}

func (p *Provider) renderAggregateExpression(aggregate AggregateFunc, field types.Field) string {
	switch aggregate {
	case AggCountField:
		return fmt.Sprintf("COUNT(%s)", p.renderField(field))
	case AggCountDistinct:
		return fmt.Sprintf("COUNT(DISTINCT %s)", p.renderField(field))
	case AggSum:
		return fmt.Sprintf("SUM(%s)", p.renderField(field))
	case AggAvg:
		return fmt.Sprintf("AVG(%s)", p.renderField(field))
	case AggMin:
		return fmt.Sprintf("MIN(%s)", p.renderField(field))
	case AggMax:
		return fmt.Sprintf("MAX(%s)", p.renderField(field))
	default:
		return p.renderField(field) // Fallback
	}
}

func (p *Provider) renderFieldExpressionWithContext(expr FieldExpression, ctx *renderContext) (string, error) {
	var result string

	switch {
	case expr.Case != nil:
		// Render CASE expression
		caseStr, err := p.renderCaseExpression(*expr.Case, ctx)
		if err != nil {
			return "", err
		}
		result = caseStr
	case expr.Coalesce != nil:
		// Render COALESCE expression
		coalesceStr, err := p.renderCoalesceExpression(*expr.Coalesce, ctx)
		if err != nil {
			return "", err
		}
		result = coalesceStr
	case expr.NullIf != nil:
		// Render NULLIF expression
		nullifStr, err := p.renderNullIfExpression(*expr.NullIf, ctx)
		if err != nil {
			return "", err
		}
		result = nullifStr
	case expr.Math != nil:
		// Render math expression
		mathStr, err := p.renderMathExpression(*expr.Math, ctx)
		if err != nil {
			return "", err
		}
		result = mathStr
	case expr.Aggregate != "":
		result = p.renderAggregateExpression(expr.Aggregate, expr.Field)
	default:
		result = p.renderField(expr.Field)
	}

	if expr.Alias != "" {
		result += " AS " + expr.Alias
	}

	return result, nil
}

func (p *Provider) renderConditionWithContext(cond types.ConditionItem, sql *strings.Builder, ctx *renderContext) error {
	switch c := cond.(type) {
	case types.Condition:
		sql.WriteString(p.renderSimpleCondition(c, ctx.addParam))
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
			if err := p.renderConditionWithContext(subCond, sql, ctx); err != nil {
				return err
			}
		}
		sql.WriteString(")")
	case FieldComparison:
		fmt.Fprintf(sql, "%s %s %s",
			p.renderField(c.LeftField),
			p.renderOperator(c.Operator),
			p.renderField(c.RightField))
	case SubqueryCondition:
		if err := p.renderSubqueryCondition(c, sql, ctx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown condition type: %T", c)
	}
	return nil
}

func (p *Provider) renderSimpleCondition(cond types.Condition, addParam func(types.Param) string) string {
	field := p.renderField(cond.Field)
	op := p.renderOperator(cond.Operator)

	switch cond.Operator {
	case types.IsNull:
		return fmt.Sprintf("%s IS NULL", field)
	case types.IsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", field)
	default:
		return fmt.Sprintf("%s %s %s", field, op, addParam(cond.Value))
	}
}

func (p *Provider) renderSubqueryCondition(cond SubqueryCondition, sql *strings.Builder, ctx *renderContext) error {
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
		sql.WriteString(p.renderField(*cond.Field))
		sql.WriteString(" ")
		sql.WriteString(string(cond.Operator))
		sql.WriteString(" ")
	}

	// Render the subquery
	sql.WriteString("(")
	if err := p.renderSubquery(cond.Subquery, sql, ctx); err != nil {
		return err
	}
	sql.WriteString(")")

	return nil
}

func (p *Provider) renderSubquery(subquery Subquery, sql *strings.Builder, ctx *renderContext) error {
	// Create a new context for the subquery
	subCtx, err := ctx.withSubquery()
	if err != nil {
		return err
	}

	switch ast := subquery.AST.(type) {
	case *types.QueryAST:
		// Render basic AST as SELECT
		sql.WriteString("SELECT ")
		if len(ast.Fields) == 0 {
			sql.WriteString("*")
		} else {
			var fields []string
			for _, field := range ast.Fields {
				fields = append(fields, p.renderField(field))
			}
			sql.WriteString(strings.Join(fields, ", "))
		}
		sql.WriteString(" FROM ")
		sql.WriteString(p.renderTable(ast.Target))

		if ast.WhereClause != nil {
			sql.WriteString(" WHERE ")
			if err := p.renderConditionWithContext(ast.WhereClause, sql, subCtx); err != nil {
				return err
			}
		}

	case *AST:
		// Render full PostgreSQL AST
		if err := p.renderSelectWithContext(ast, sql, subCtx); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported subquery AST type: %T", ast)
	}

	return nil
}

func (p *Provider) renderCaseExpression(expr CaseExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder
	sql.WriteString("CASE")

	for _, when := range expr.WhenClauses {
		sql.WriteString(" WHEN ")
		if err := p.renderConditionWithContext(when.Condition, &sql, ctx); err != nil {
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

func (*Provider) renderCoalesceExpression(expr CoalesceExpression, ctx *renderContext) (string, error) {
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

func (*Provider) renderNullIfExpression(expr NullIfExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder
	sql.WriteString("NULLIF(")
	sql.WriteString(ctx.addParam(expr.Value1))
	sql.WriteString(", ")
	sql.WriteString(ctx.addParam(expr.Value2))
	sql.WriteString(")")
	return sql.String(), nil
}

func (p *Provider) renderMathExpression(expr MathExpression, ctx *renderContext) (string, error) {
	var sql strings.Builder

	switch expr.Function {
	case MathRound:
		sql.WriteString("ROUND(")
		sql.WriteString(p.renderField(expr.Field))
		if expr.Precision != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.Precision))
		}
		sql.WriteString(")")
	case MathFloor:
		sql.WriteString("FLOOR(")
		sql.WriteString(p.renderField(expr.Field))
		sql.WriteString(")")
	case MathCeil:
		sql.WriteString("CEIL(")
		sql.WriteString(p.renderField(expr.Field))
		sql.WriteString(")")
	case MathAbs:
		sql.WriteString("ABS(")
		sql.WriteString(p.renderField(expr.Field))
		sql.WriteString(")")
	case MathPower:
		sql.WriteString("POWER(")
		sql.WriteString(p.renderField(expr.Field))
		if expr.Exponent != nil {
			sql.WriteString(", ")
			sql.WriteString(ctx.addParam(*expr.Exponent))
		} else {
			return "", fmt.Errorf("POWER requires an exponent parameter")
		}
		sql.WriteString(")")
	case MathSqrt:
		sql.WriteString("SQRT(")
		sql.WriteString(p.renderField(expr.Field))
		sql.WriteString(")")
	default:
		return "", fmt.Errorf("unsupported math function: %s", expr.Function)
	}

	return sql.String(), nil
}

func (*Provider) renderOperator(op types.Operator) string {
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
	case types.IN:
		return "IN"
	case types.NotIn:
		return "NOT IN"
	case types.EXISTS:
		return "EXISTS"
	case types.NotExists:
		return "NOT EXISTS"
	default:
		return string(op)
	}
}

func (*Provider) renderListen(ast *AST, sql *strings.Builder) {
	// Channel name is derived from table name (e.g., "users" -> "users_changes")
	channelName := ast.Target.Name + "_changes"
	sql.WriteString("LISTEN ")
	sql.WriteString(channelName)
}

func (*Provider) renderNotify(ast *AST, sql *strings.Builder, addParam func(types.Param) string) {
	// Channel name is derived from table name (e.g., "users" -> "users_changes")
	channelName := ast.Target.Name + "_changes"
	sql.WriteString("NOTIFY ")
	sql.WriteString(channelName)

	// Add payload if provided
	if ast.NotifyPayload != nil {
		sql.WriteString(", ")
		sql.WriteString(addParam(*ast.NotifyPayload))
	}
}

func (*Provider) renderUnlisten(ast *AST, sql *strings.Builder) {
	// Channel name is derived from table name (e.g., "users" -> "users_changes")
	channelName := ast.Target.Name + "_changes"
	sql.WriteString("UNLISTEN ")
	sql.WriteString(channelName)
}
