package astql

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/dbml"
)

// ASTQL represents an instance of the query builder with a specific DBML schema.
type ASTQL struct {
	project *dbml.Project
	// Internal indexes for fast validation
	tables map[string]*dbml.Table
	fields map[string]map[string]*dbml.Column // table -> field -> column
}

// NewFromDBML creates a new ASTQL instance from a DBML project.
func NewFromDBML(project *dbml.Project) (*ASTQL, error) {
	if project == nil {
		return nil, fmt.Errorf("project cannot be nil")
	}

	a := &ASTQL{
		project: project,
		tables:  make(map[string]*dbml.Table),
		fields:  make(map[string]map[string]*dbml.Column),
	}

	// Build indexes for fast validation
	for _, table := range project.Tables {
		a.tables[table.Name] = table
		a.fields[table.Name] = make(map[string]*dbml.Column)
		for _, col := range table.Columns {
			a.fields[table.Name][col.Name] = col
		}
	}

	return a, nil
}

// LoadFromDBML loads a DBML file and creates an ASTQL instance.
// Note: This requires a DBML parser to be implemented.
// For now, use NewFromDBML with a programmatically created project.
func LoadFromDBML(_ string) (*ASTQL, error) {
	return nil, fmt.Errorf("LoadFromDBML not yet implemented - use NewFromDBML instead")
}

// validateTable checks if a table exists in the schema.
func (a *ASTQL) validateTable(name string) error {
	if _, ok := a.tables[name]; !ok {
		return fmt.Errorf("table '%s' not found in schema", name)
	}
	return nil
}

// validateField checks if a field exists in any table in the schema.
func (a *ASTQL) validateField(field string) error {
	// Handle SQL expressions with AS aliases like "c.name AS customer_name"
	if asIndex := findAS(field); asIndex != -1 {
		field = field[:asIndex]
	}

	// Handle table aliases like "o.id" by extracting just the field name
	fieldName := field
	if dotIndex := lastDotIndex(field); dotIndex != -1 {
		fieldName = field[dotIndex+1:]
	}

	// Check if field exists in any table
	for _, tableFields := range a.fields {
		if _, ok := tableFields[fieldName]; ok {
			return nil // Found it
		}
	}

	return fmt.Errorf("field '%s' not found in schema", field)
}

// validateTableOrAlias validates both table names and aliases.
func (a *ASTQL) validateTableOrAlias(tableOrAlias string) error {
	// Must be either:
	// 1. A single lowercase letter (table alias), OR
	// 2. A registered table name
	if isValidTableAlias(tableOrAlias) {
		// It's a valid single-letter alias
		return nil
	}
	if err := a.validateTable(tableOrAlias); err == nil {
		// It's a valid table name
		return nil
	}
	return fmt.Errorf("WithTable requires single-letter alias (a-z) or valid table name, got: %s", tableOrAlias)
}

// isValidTableAlias checks if a string is a valid single-letter table alias.
func isValidTableAlias(alias string) bool {
	return len(alias) == 1 && alias[0] >= 'a' && alias[0] <= 'z'
}

// findAS finds the position of " AS " in a string.
func findAS(s string) int {
	for i := 0; i < len(s)-3; i++ {
		if s[i] == ' ' && s[i+1] == 'A' && s[i+2] == 'S' && s[i+3] == ' ' {
			return i
		}
	}
	return -1
}

// lastDotIndex finds the last dot in a string.
func lastDotIndex(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}

// isValidSQLIdentifier checks if a string is a valid SQL identifier.
func isValidSQLIdentifier(s string) bool {
	if s == "" {
		return false
	}

	// Must start with letter or underscore
	first := s[0]
	if !((first >= 'a' && first <= 'z') ||
		(first >= 'A' && first <= 'Z') ||
		first == '_') {
		return false
	}

	// Rest must be alphanumeric or underscore
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_') {
			return false
		}
	}

	// Check for SQL injection patterns
	lower := strings.ToLower(s)

	suspiciousPatterns := []string{
		";", "--", "/*", "*/", "'", "\"", "`", "\\",
		" or ", " and ", "drop table", "delete from",
		"insert into", "update set", "select ",
		"union all", "union select",
	}

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	// Reject if it contains spaces
	if strings.Contains(s, " ") {
		return false
	}

	return true
}

// TryF creates a validated field reference, returning an error if invalid.
func (a *ASTQL) TryF(name string) (types.Field, error) {
	if err := a.validateField(name); err != nil {
		return types.Field{}, fmt.Errorf("invalid field: %w", err)
	}
	return types.Field{Name: name}, nil
}

// F creates a validated field reference.
func (a *ASTQL) F(name string) types.Field {
	f, err := a.TryF(name)
	if err != nil {
		panic(err)
	}
	return f
}

// TryT creates a validated table reference, returning an error if invalid.
func (a *ASTQL) TryT(name string, alias ...string) (types.Table, error) {
	if err := a.validateTable(name); err != nil {
		return types.Table{}, fmt.Errorf("invalid table: %w", err)
	}

	var tableAlias string
	if len(alias) > 0 {
		if len(alias) > 1 {
			return types.Table{}, fmt.Errorf("only one alias allowed")
		}
		tableAlias = alias[0]
		if !isValidTableAlias(tableAlias) {
			return types.Table{}, fmt.Errorf("alias must be single lowercase letter (a-z), got: %s", tableAlias)
		}
	}

	return types.Table{Name: name, Alias: tableAlias}, nil
}

// T creates a validated table reference.
func (a *ASTQL) T(name string, alias ...string) types.Table {
	t, err := a.TryT(name, alias...)
	if err != nil {
		panic(err)
	}
	return t
}

// TryP creates a validated parameter reference, returning an error if invalid.
func (*ASTQL) TryP(name string) (types.Param, error) {
	if !isValidSQLIdentifier(name) {
		return types.Param{}, fmt.Errorf("invalid parameter name: %s", name)
	}
	return types.Param{Name: name}, nil
}

// P creates a validated parameter reference.
func (a *ASTQL) P(name string) types.Param {
	p, err := a.TryP(name)
	if err != nil {
		panic(err)
	}
	return p
}

// TryC creates a validated condition, returning an error if invalid.
func (a *ASTQL) TryC(field types.Field, op types.Operator, param types.Param) (types.Condition, error) {
	// Validate field exists
	if err := a.validateField(field.Name); err != nil {
		return types.Condition{}, err
	}
	return types.Condition{
		Field:    field,
		Operator: op,
		Value:    param,
	}, nil
}

// C creates a validated condition.
func (a *ASTQL) C(field types.Field, op types.Operator, param types.Param) types.Condition {
	c, err := a.TryC(field, op, param)
	if err != nil {
		panic(err)
	}
	return c
}

// TryNull creates a NULL condition, returning an error if invalid.
func (a *ASTQL) TryNull(field types.Field) (types.Condition, error) {
	if err := a.validateField(field.Name); err != nil {
		return types.Condition{}, err
	}
	return types.Condition{
		Field:    field,
		Operator: types.IsNull,
	}, nil
}

// Null creates a NULL condition.
func (a *ASTQL) Null(field types.Field) types.Condition {
	c, err := a.TryNull(field)
	if err != nil {
		panic(err)
	}
	return c
}

// TryNotNull creates a NOT NULL condition, returning an error if invalid.
func (a *ASTQL) TryNotNull(field types.Field) (types.Condition, error) {
	if err := a.validateField(field.Name); err != nil {
		return types.Condition{}, err
	}
	return types.Condition{
		Field:    field,
		Operator: types.IsNotNull,
	}, nil
}

// NotNull creates a NOT NULL condition.
func (a *ASTQL) NotNull(field types.Field) types.Condition {
	c, err := a.TryNotNull(field)
	if err != nil {
		panic(err)
	}
	return c
}

// TryAnd creates an AND condition group, returning an error if invalid.
func (*ASTQL) TryAnd(conditions ...types.ConditionItem) (types.ConditionGroup, error) {
	if len(conditions) == 0 {
		return types.ConditionGroup{}, fmt.Errorf("AND requires at least one condition")
	}
	return types.ConditionGroup{
		Logic:      types.AND,
		Conditions: conditions,
	}, nil
}

// And creates an AND condition group.
func (a *ASTQL) And(conditions ...types.ConditionItem) types.ConditionGroup {
	g, err := a.TryAnd(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}

// TryOr creates an OR condition group, returning an error if invalid.
func (*ASTQL) TryOr(conditions ...types.ConditionItem) (types.ConditionGroup, error) {
	if len(conditions) == 0 {
		return types.ConditionGroup{}, fmt.Errorf("OR requires at least one condition")
	}
	return types.ConditionGroup{
		Logic:      types.OR,
		Conditions: conditions,
	}, nil
}

// Or creates an OR condition group.
func (a *ASTQL) Or(conditions ...types.ConditionItem) types.ConditionGroup {
	g, err := a.TryOr(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}

// WithTable creates a new Field with a table/alias prefix, validated against the schema.
func (a *ASTQL) WithTable(field types.Field, tableOrAlias string) types.Field {
	if err := a.validateTableOrAlias(tableOrAlias); err != nil {
		panic(err)
	}
	return types.Field{
		Name:  field.Name,
		Table: tableOrAlias,
	}
}

// TryWithTable creates a new Field with a table/alias prefix, returning an error if invalid.
func (a *ASTQL) TryWithTable(field types.Field, tableOrAlias string) (types.Field, error) {
	if err := a.validateTableOrAlias(tableOrAlias); err != nil {
		return types.Field{}, err
	}
	return types.Field{
		Name:  field.Name,
		Table: tableOrAlias,
	}, nil
}

// GetInstance returns the ASTQL instance (for use by provider packages).
func (a *ASTQL) GetInstance() *ASTQL {
	return a
}
