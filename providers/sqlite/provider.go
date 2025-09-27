package sqlite

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

// Provider renders SQLite AST to SQL with positional parameters.
type Provider struct{}

// NewProvider creates a new SQLite provider.
func NewProvider() *Provider {
	return &Provider{}
}

// Render converts a SQLite AST to a QueryResult with SQL and positional parameters.
func (p *Provider) Render(ast *AST) (*astql.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	var sql strings.Builder
	var params []string
	paramPositions := make(map[string]int) // Track parameter positions
	nextPosition := 1

	// Helper to add a parameter and return its placeholder
	addParam := func(param types.Param) string {
		// SQLite uses positional parameters (?)
		// But we track the names for the result
		if _, exists := paramPositions[param.Name]; !exists {
			params = append(params, param.Name)
			paramPositions[param.Name] = nextPosition
			nextPosition++
		}
		return "?"
	}

	// Render based on operation
	switch ast.Operation {
	case types.OpSelect:
		if err := p.renderSelect(ast, &sql, addParam); err != nil {
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
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}

	return &astql.QueryResult{
		SQL:            sql.String(),
		RequiredParams: params,
	}, nil
}

func (p *Provider) renderSelect(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("SELECT ")

	// DISTINCT
	if ast.Distinct {
		sql.WriteString("DISTINCT ")
	}

	// Fields and Field Expressions
	switch {
	case len(ast.FieldExpressions) > 0:
		// Use field expressions if provided
		exprs := make([]string, len(ast.FieldExpressions))
		for i, expr := range ast.FieldExpressions {
			exprs[i] = p.renderFieldExpression(expr, addParam)
		}
		sql.WriteString(strings.Join(exprs, ", "))
	case len(ast.Fields) == 0:
		sql.WriteString("*")
	default:
		fields := make([]string, len(ast.Fields))
		for i, field := range ast.Fields {
			fields[i] = p.renderField(field)
		}
		sql.WriteString(strings.Join(fields, ", "))
	}

	// FROM
	sql.WriteString(" FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// JOINs
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(p.renderTable(join.Table))
		if join.On != nil {
			sql.WriteString(" ON ")
			p.renderCondition(join.On, sql, addParam)
		}
	}

	// WHERE
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		p.renderCondition(ast.WhereClause, sql, addParam)
	}

	// GROUP BY
	if len(ast.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		groupFields := make([]string, len(ast.GroupBy))
		for i, field := range ast.GroupBy {
			groupFields[i] = p.renderField(field)
		}
		sql.WriteString(strings.Join(groupFields, ", "))
	}

	// HAVING
	if len(ast.Having) > 0 {
		sql.WriteString(" HAVING ")
		havingConds := make([]string, len(ast.Having))
		for i, cond := range ast.Having {
			havingConds[i] = p.renderSimpleCondition(cond, addParam)
		}
		sql.WriteString(strings.Join(havingConds, " AND "))
	}

	// ORDER BY
	if len(ast.Ordering) > 0 {
		sql.WriteString(" ORDER BY ")
		orderParts := make([]string, len(ast.Ordering))
		for i, order := range ast.Ordering {
			orderParts[i] = fmt.Sprintf("%s %s", p.renderField(order.Field), order.Direction)
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
	// Check if we need to handle OnConflict with SQLite's OR syntax
	if ast.OnConflict != nil && len(ast.OnConflict.Columns) == 0 {
		// OnConflict without specific columns - use SQLite's OR syntax
		switch ast.OnConflict.Action {
		case ConflictDoNothing:
			sql.WriteString("INSERT OR IGNORE INTO ")
		case ConflictDoUpdate:
			sql.WriteString("INSERT OR REPLACE INTO ")
		default:
			sql.WriteString("INSERT INTO ")
		}
	} else {
		sql.WriteString("INSERT INTO ")
	}

	sql.WriteString(p.renderTable(ast.Target))

	if len(ast.Values) == 0 {
		return fmt.Errorf("INSERT requires at least one value set")
	}

	// Get fields from first value set
	fields := make([]types.Field, 0, len(ast.Values[0]))
	fieldStrs := make([]string, 0, len(ast.Values[0]))
	for field := range ast.Values[0] {
		fields = append(fields, field)
		fieldStrs = append(fieldStrs, p.renderField(field))
	}

	sql.WriteString(" (")
	sql.WriteString(strings.Join(fieldStrs, ", "))
	sql.WriteString(") VALUES ")

	// Values
	for i, valueSet := range ast.Values {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("(")
		values := make([]string, len(fields))
		for j, field := range fields {
			values[j] = addParam(valueSet[field])
		}
		sql.WriteString(strings.Join(values, ", "))
		sql.WriteString(")")
	}

	// ON CONFLICT (UPSERT) - only if we have specific columns
	// (OnConflict without columns is handled above with OR syntax)
	if ast.OnConflict != nil && len(ast.OnConflict.Columns) > 0 {
		sql.WriteString(" ON CONFLICT")
		if len(ast.OnConflict.Columns) > 0 {
			sql.WriteString(" (")
			cols := make([]string, len(ast.OnConflict.Columns))
			for i, col := range ast.OnConflict.Columns {
				cols[i] = p.renderField(col)
			}
			sql.WriteString(strings.Join(cols, ", "))
			sql.WriteString(")")
		}

		switch ast.OnConflict.Action {
		case ConflictDoNothing:
			sql.WriteString(" DO NOTHING")
		case ConflictDoUpdate:
			sql.WriteString(" DO UPDATE SET ")
			var updates []string
			for field, param := range ast.OnConflict.Updates {
				updates = append(updates, fmt.Sprintf("%s = %s",
					p.renderField(field), addParam(param)))
			}
			sql.WriteString(strings.Join(updates, ", "))
		}
	}

	// RETURNING clause (SQLite 3.35.0+)
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		retFields := make([]string, len(ast.Returning))
		for i, field := range ast.Returning {
			retFields[i] = p.renderField(field)
		}
		sql.WriteString(strings.Join(retFields, ", "))
	}

	return nil
}

func (p *Provider) renderUpdate(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("UPDATE ")
	sql.WriteString(p.renderTable(ast.Target))
	sql.WriteString(" SET ")

	updates := make([]string, 0, len(ast.Updates))
	for field, param := range ast.Updates {
		updates = append(updates, fmt.Sprintf("%s = %s",
			p.renderField(field), addParam(param)))
	}
	sql.WriteString(strings.Join(updates, ", "))

	// WHERE
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		p.renderCondition(ast.WhereClause, sql, addParam)
	}

	// RETURNING clause (SQLite 3.35.0+)
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		retFields := make([]string, len(ast.Returning))
		for i, field := range ast.Returning {
			retFields[i] = p.renderField(field)
		}
		sql.WriteString(strings.Join(retFields, ", "))
	}

	return nil
}

func (p *Provider) renderDelete(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("DELETE FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// WHERE
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		p.renderCondition(ast.WhereClause, sql, addParam)
	}

	// RETURNING clause (SQLite 3.35.0+)
	if len(ast.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		retFields := make([]string, len(ast.Returning))
		for i, field := range ast.Returning {
			retFields[i] = p.renderField(field)
		}
		sql.WriteString(strings.Join(retFields, ", "))
	}

	return nil
}

func (p *Provider) renderCount(ast *AST, sql *strings.Builder, addParam func(types.Param) string) error {
	sql.WriteString("SELECT COUNT(*) FROM ")
	sql.WriteString(p.renderTable(ast.Target))

	// JOINs (COUNT can have JOINs)
	for _, join := range ast.Joins {
		sql.WriteString(" ")
		sql.WriteString(string(join.Type))
		sql.WriteString(" ")
		sql.WriteString(p.renderTable(join.Table))
		if join.On != nil {
			sql.WriteString(" ON ")
			p.renderCondition(join.On, sql, addParam)
		}
	}

	// WHERE
	if ast.WhereClause != nil {
		sql.WriteString(" WHERE ")
		p.renderCondition(ast.WhereClause, sql, addParam)
	}

	return nil
}

func (p *Provider) renderCondition(cond types.ConditionItem, sql *strings.Builder, addParam func(types.Param) string) {
	switch c := cond.(type) {
	case types.Condition:
		sql.WriteString(p.renderSimpleCondition(c, addParam))
	case types.ConditionGroup:
		sql.WriteString("(")
		for i, condition := range c.Conditions {
			if i > 0 {
				fmt.Fprintf(sql, " %s ", c.Logic)
			}
			p.renderCondition(condition, sql, addParam)
		}
		sql.WriteString(")")
	case FieldComparison:
		sql.WriteString(p.renderFieldComparison(c))
	case SubqueryCondition:
		sql.WriteString(p.renderSubqueryCondition(c, addParam))
	}
}

func (p *Provider) renderFieldComparison(fc FieldComparison) string {
	leftField := p.renderField(fc.LeftField)
	rightField := p.renderField(fc.RightField)
	op := string(fc.Operator)

	return fmt.Sprintf("%s %s %s", leftField, op, rightField)
}

func (p *Provider) renderSimpleCondition(c types.Condition, addParam func(types.Param) string) string {
	field := p.renderField(c.Field)
	op := string(c.Operator)

	// Handle special operators
	switch c.Operator {
	case types.IsNull:
		return fmt.Sprintf("%s IS NULL", field)
	case types.IsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", field)
	default:
		param := addParam(c.Value)
		return fmt.Sprintf("%s %s %s", field, op, param)
	}
}

func (*Provider) renderField(f types.Field) string {
	// SQLite uses double quotes for identifiers
	if f.Table != "" {
		return fmt.Sprintf("%q.%q", f.Table, f.Name)
	}
	return fmt.Sprintf("%q", f.Name)
}

func (*Provider) renderTable(t types.Table) string {
	// SQLite uses double quotes for identifiers
	if t.Alias != "" {
		return fmt.Sprintf("%q AS %q", t.Name, t.Alias)
	}
	return fmt.Sprintf("%q", t.Name)
}

// renderFieldExpression renders a field expression with aggregates or functions.
func (p *Provider) renderFieldExpression(expr FieldExpression, addParam func(types.Param) string) string {
	var result string

	// Handle different expression types
	switch {
	case expr.Case != nil:
		result = p.renderCaseExpression(*expr.Case, addParam)
	case expr.Coalesce != nil:
		result = p.renderCoalesceExpression(*expr.Coalesce, addParam)
	case expr.NullIf != nil:
		result = p.renderNullIfExpression(*expr.NullIf, addParam)
	case expr.Aggregate != "":
		// Handle aggregate functions
		switch expr.Aggregate {
		case AggCountDistinct:
			result = fmt.Sprintf("COUNT(DISTINCT %s)", p.renderField(expr.Field))
		default:
			result = fmt.Sprintf("%s(%s)", expr.Aggregate, p.renderField(expr.Field))
		}
	default:
		// Regular field
		result = p.renderField(expr.Field)
	}

	// Add alias if present
	if expr.Alias != "" {
		result = fmt.Sprintf("%s AS %q", result, expr.Alias)
	}

	return result
}

// renderCaseExpression renders a CASE expression.
func (p *Provider) renderCaseExpression(ce CaseExpression, addParam func(types.Param) string) string {
	var sql strings.Builder
	sql.WriteString("CASE")

	for _, when := range ce.WhenClauses {
		sql.WriteString(" WHEN ")
		p.renderCondition(when.Condition, &sql, addParam)
		sql.WriteString(" THEN ")
		sql.WriteString(addParam(when.Result))
	}

	if ce.ElseClause != nil {
		sql.WriteString(" ELSE ")
		sql.WriteString(addParam(*ce.ElseClause))
	}

	sql.WriteString(" END")
	return sql.String()
}

// renderCoalesceExpression renders a COALESCE expression.
func (*Provider) renderCoalesceExpression(ce CoalesceExpression, addParam func(types.Param) string) string {
	params := make([]string, len(ce.Values))
	for i, val := range ce.Values {
		params[i] = addParam(val)
	}
	return fmt.Sprintf("COALESCE(%s)", strings.Join(params, ", "))
}

// renderNullIfExpression renders a NULLIF expression.
func (*Provider) renderNullIfExpression(ne NullIfExpression, addParam func(types.Param) string) string {
	return fmt.Sprintf("NULLIF(%s, %s)", addParam(ne.Value1), addParam(ne.Value2))
}

// renderSubqueryCondition renders a subquery condition.
func (p *Provider) renderSubqueryCondition(sc SubqueryCondition, addParam func(types.Param) string) string {
	subquerySQL := p.renderSubquery(sc.Subquery, addParam)

	if sc.Field != nil {
		// IN/NOT IN with field
		field := p.renderField(*sc.Field)
		op := string(sc.Operator)
		return fmt.Sprintf("%s %s (%s)", field, op, subquerySQL)
	}
	// EXISTS/NOT EXISTS
	op := string(sc.Operator)
	return fmt.Sprintf("%s (%s)", op, subquerySQL)
}

// renderSubquery renders a subquery.
func (p *Provider) renderSubquery(sub Subquery, addParam func(types.Param) string) string {
	switch ast := sub.AST.(type) {
	case *AST:
		// SQLite-specific AST
		var sql strings.Builder
		if err := p.renderSelect(ast, &sql, addParam); err != nil {
			// This shouldn't happen if validation passed
			return fmt.Sprintf("/* ERROR: %v */", err)
		}
		return sql.String()
	case *types.QueryAST:
		// Base AST - create a wrapper
		wrapped := &AST{QueryAST: ast}
		var sql strings.Builder
		if err := p.renderSelect(wrapped, &sql, addParam); err != nil {
			return fmt.Sprintf("/* ERROR: %v */", err)
		}
		return sql.String()
	default:
		return "/* UNKNOWN SUBQUERY TYPE */"
	}
}
