package postgres

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// Result type constants.
const (
	ResultTypeCount = "count"
)

// Provider renders PostgreSQL AST to SQL with named parameters.
type Provider struct {
	// IncludeMetadata determines if type metadata should be extracted
	IncludeMetadata bool
}

// NewProvider creates a new PostgreSQL provider.
func NewProvider() *Provider {
	return &Provider{
		IncludeMetadata: true,
	}
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
	addParam := func(param astql.Param) string {
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
	case astql.OpSelect:
		if err := p.renderSelectWithContext(ast, &sql, ctx); err != nil {
			return nil, err
		}
	case astql.OpInsert:
		if err := p.renderInsert(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case astql.OpUpdate:
		if err := p.renderUpdate(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case astql.OpDelete:
		if err := p.renderDelete(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case astql.OpCount:
		if err := p.renderCount(ast, &sql, addParam); err != nil {
			return nil, err
		}
	case astql.OpListen:
		p.renderListen(ast, &sql)
	case astql.OpNotify:
		p.renderNotify(ast, &sql, addParam)
	case astql.OpUnlisten:
		p.renderUnlisten(ast, &sql)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	result := &astql.QueryResult{
		SQL:            sql.String(),
		RequiredParams: params,
	}

	// Extract metadata if enabled
	if p.IncludeMetadata {
		if metadata := p.extractQueryMetadata(ast); metadata != nil {
			result.Metadata = *metadata
		}
	}

	return result, nil
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

func (p *Provider) renderInsert(ast *AST, sql *strings.Builder, addParam func(astql.Param) string) error {
	sql.WriteString("INSERT INTO ")
	sql.WriteString(p.renderTable(ast.Target))

	if len(ast.Values) == 0 {
		return fmt.Errorf("INSERT requires at least one value set")
	}

	// Extract field names from first value set
	fields := make([]string, 0, len(ast.Values[0]))
	fieldObjs := make([]astql.Field, 0, len(ast.Values[0]))
	for field := range ast.Values[0] {
		fields = append(fields, field.Name)
		fieldObjs = append(fieldObjs, field)
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
			conflictFields = append(conflictFields, field.Name)
		}
		sql.WriteString(strings.Join(conflictFields, ", "))
		sql.WriteString(") ")

		switch ast.OnConflict.Action {
		case DoNothing:
			sql.WriteString("DO NOTHING")
		case DoUpdate:
			sql.WriteString("DO UPDATE SET ")
			var updates []string
			for field, param := range ast.OnConflict.Updates {
				updates = append(updates, fmt.Sprintf("%s = %s", field.Name, addParam(param)))
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

func (p *Provider) renderUpdate(ast *AST, sql *strings.Builder, addParam func(astql.Param) string) error {
	sql.WriteString("UPDATE ")
	sql.WriteString(p.renderTable(ast.Target))
	sql.WriteString(" SET ")

	// Render updates
	updates := make([]string, 0, len(ast.Updates))
	for field, param := range ast.Updates {
		updates = append(updates, fmt.Sprintf("%s = %s", field.Name, addParam(param)))
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

func (p *Provider) renderDelete(ast *AST, sql *strings.Builder, addParam func(astql.Param) string) error {
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

func (p *Provider) renderCount(ast *AST, sql *strings.Builder, addParam func(astql.Param) string) error {
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

func (*Provider) renderTable(table astql.Table) string {
	if table.Alias != "" {
		return fmt.Sprintf("%s %s", table.Name, table.Alias)
	}
	return table.Name
}

func (*Provider) renderField(field astql.Field) string {
	if field.Table != "" {
		return fmt.Sprintf("%s.%s", field.Table, field.Name)
	}
	return field.Name
}

func (p *Provider) renderAggregateExpression(aggregate AggregateFunc, field astql.Field) string {
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

func (p *Provider) renderConditionWithContext(cond astql.ConditionItem, sql *strings.Builder, ctx *renderContext) error {
	switch c := cond.(type) {
	case astql.Condition:
		sql.WriteString(p.renderSimpleCondition(c, ctx.addParam))
	case astql.ConditionGroup:
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
	case astql.FieldComparison:
		fmt.Fprintf(sql, "%s %s %s",
			p.renderField(c.LeftField),
			p.renderOperator(c.Operator),
			p.renderField(c.RightField))
	case astql.SubqueryCondition:
		if err := p.renderSubqueryCondition(c, sql, ctx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown condition type: %T", c)
	}
	return nil
}

func (p *Provider) renderSimpleCondition(cond astql.Condition, addParam func(astql.Param) string) string {
	field := p.renderField(cond.Field)
	op := p.renderOperator(cond.Operator)

	switch cond.Operator {
	case astql.IsNull:
		return fmt.Sprintf("%s IS NULL", field)
	case astql.IsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", field)
	default:
		return fmt.Sprintf("%s %s %s", field, op, addParam(cond.Value))
	}
}

func (p *Provider) renderSubqueryCondition(cond astql.SubqueryCondition, sql *strings.Builder, ctx *renderContext) error {
	switch cond.Operator {
	case astql.EXISTS, astql.NotExists:
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

func (p *Provider) renderSubquery(subquery astql.Subquery, sql *strings.Builder, ctx *renderContext) error {
	// Create a new context for the subquery
	subCtx, err := ctx.withSubquery()
	if err != nil {
		return err
	}

	switch ast := subquery.AST.(type) {
	case *astql.QueryAST:
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

func (*Provider) renderOperator(op astql.Operator) string {
	switch op {
	case astql.EQ:
		return "="
	case astql.NE:
		return "!="
	case astql.GT:
		return ">"
	case astql.GE:
		return ">="
	case astql.LT:
		return "<"
	case astql.LE:
		return "<="
	case astql.LIKE:
		return "LIKE"
	case astql.NotLike:
		return "NOT LIKE"
	case astql.IN:
		return "IN"
	case astql.NotIn:
		return "NOT IN"
	case astql.EXISTS:
		return "EXISTS"
	case astql.NotExists:
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

func (*Provider) renderNotify(ast *AST, sql *strings.Builder, addParam func(astql.Param) string) {
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

// extractQueryMetadata builds metadata about the query.
func (*Provider) extractQueryMetadata(ast *AST) *astql.QueryMetadata {
	tableMeta := astql.GetTableMetadata(ast.Target.Name)
	metadata := &astql.QueryMetadata{
		Operation: ast.Operation,
		Table:     *tableMeta,
	}

	// Determine result type
	switch ast.Operation {
	case astql.OpSelect:
		if ast.Limit != nil && *ast.Limit == 1 {
			metadata.ResultType = "single"
		} else {
			metadata.ResultType = "multiple"
		}
	case astql.OpInsert, astql.OpUpdate, astql.OpDelete:
		if len(ast.Returning) > 0 {
			metadata.ResultType = "single"
		} else {
			metadata.ResultType = "affected"
		}
	case astql.OpCount:
		metadata.ResultType = ResultTypeCount
	}

	// Extract returned fields
	if ast.Operation == astql.OpSelect {
		if len(ast.Fields) == 0 && len(ast.FieldExpressions) == 0 {
			// SELECT * - return all fields from table metadata
			metadata.ReturnedFields = metadata.Table.Fields
		} else {
			// Specific fields
			for _, field := range ast.Fields {
				if fieldMeta := astql.GetFieldMetadata(ast.Target.Name, field.Name); fieldMeta != nil {
					metadata.ReturnedFields = append(metadata.ReturnedFields, *fieldMeta)
				}
			}
			// TODO: Handle field expressions (aggregates, aliases)
		}
	} else if len(ast.Returning) > 0 {
		// RETURNING clause
		for _, field := range ast.Returning {
			if fieldMeta := astql.GetFieldMetadata(ast.Target.Name, field.Name); fieldMeta != nil {
				metadata.ReturnedFields = append(metadata.ReturnedFields, *fieldMeta)
			}
		}
	}

	// Extract modified fields for INSERT/UPDATE
	if ast.Operation == astql.OpInsert && len(ast.Values) > 0 {
		// Get fields from first value set
		for field := range ast.Values[0] {
			if fieldMeta := astql.GetFieldMetadata(ast.Target.Name, field.Name); fieldMeta != nil {
				metadata.ModifiedFields = append(metadata.ModifiedFields, *fieldMeta)
			}
		}
	} else if ast.Operation == astql.OpUpdate {
		// Get fields from updates map
		for field := range ast.Updates {
			if fieldMeta := astql.GetFieldMetadata(ast.Target.Name, field.Name); fieldMeta != nil {
				metadata.ModifiedFields = append(metadata.ModifiedFields, *fieldMeta)
			}
		}
	}

	// Extract parameters would require walking the AST to find all Param references
	// For now, the RequiredParams list serves this purpose

	return metadata
}
