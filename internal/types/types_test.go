package types

import (
	"errors"
	"testing"
)

// =============================================================================
// Field Tests
// =============================================================================

func TestField_GetName(t *testing.T) {
	f := Field{Name: "username", Table: "users"}
	if got := f.GetName(); got != "username" {
		t.Errorf("GetName() = %q, want %q", got, "username")
	}
}

func TestField_GetTable(t *testing.T) {
	f := Field{Name: "username", Table: "users"}
	if got := f.GetTable(); got != "users" {
		t.Errorf("GetTable() = %q, want %q", got, "users")
	}
}

func TestField_GetTable_Empty(t *testing.T) {
	f := Field{Name: "username"}
	if got := f.GetTable(); got != "" {
		t.Errorf("GetTable() = %q, want empty string", got)
	}
}

func TestField_WithTable(t *testing.T) {
	// Clear any existing validator
	SetTableValidator(nil)

	f := Field{Name: "username"}
	result := f.WithTable("u")

	if result.Table != "u" {
		t.Errorf("WithTable().Table = %q, want %q", result.Table, "u")
	}
	if result.Name != "username" {
		t.Errorf("WithTable().Name = %q, want %q", result.Name, "username")
	}
	// Original should be unchanged
	if f.Table != "" {
		t.Errorf("Original field modified: Table = %q, want empty", f.Table)
	}
}

func TestField_WithTable_ValidValidator(t *testing.T) {
	SetTableValidator(func(_ string) error {
		return nil // Always valid
	})
	defer SetTableValidator(nil)

	f := Field{Name: "id"}
	result := f.WithTable("users")

	if result.Table != "users" {
		t.Errorf("WithTable().Table = %q, want %q", result.Table, "users")
	}
}

func TestField_WithTable_InvalidValidator_Panics(t *testing.T) {
	SetTableValidator(func(s string) error {
		return errors.New("invalid table: " + s)
	})
	defer SetTableValidator(nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when validator fails")
		}
	}()

	f := Field{Name: "id"}
	f.WithTable("invalid_table")
}

func TestSetTableValidator(t *testing.T) {
	called := false
	validator := func(_ string) error {
		called = true
		return nil
	}

	SetTableValidator(validator)
	defer SetTableValidator(nil)

	f := Field{Name: "test"}
	f.WithTable("t")

	if !called {
		t.Error("Validator was not called")
	}
}

// =============================================================================
// Table Tests
// =============================================================================

func TestTable_GetName(t *testing.T) {
	tbl := Table{Name: "users", Alias: "u"}
	if got := tbl.GetName(); got != "users" {
		t.Errorf("GetName() = %q, want %q", got, "users")
	}
}

func TestTable_GetAlias(t *testing.T) {
	tbl := Table{Name: "users", Alias: "u"}
	if got := tbl.GetAlias(); got != "u" {
		t.Errorf("GetAlias() = %q, want %q", got, "u")
	}
}

func TestTable_GetAlias_Empty(t *testing.T) {
	tbl := Table{Name: "users"}
	if got := tbl.GetAlias(); got != "" {
		t.Errorf("GetAlias() = %q, want empty string", got)
	}
}

// =============================================================================
// Param Tests
// =============================================================================

func TestParam_GetName(t *testing.T) {
	p := Param{Name: "user_id"}
	if got := p.GetName(); got != "user_id" {
		t.Errorf("GetName() = %q, want %q", got, "user_id")
	}
}

// =============================================================================
// ConditionItem Interface Tests
// =============================================================================

func TestCondition_IsConditionItem(_ *testing.T) {
	var item ConditionItem = Condition{
		Field:    Field{Name: "age"},
		Operator: GT,
		Value:    Param{Name: "min_age"},
	}
	item.IsConditionItem() // Should not panic
}

func TestConditionGroup_IsConditionItem(_ *testing.T) {
	var item ConditionItem = ConditionGroup{
		Logic: AND,
		Conditions: []ConditionItem{
			Condition{Field: Field{Name: "a"}, Operator: EQ, Value: Param{Name: "p1"}},
		},
	}
	item.IsConditionItem() // Should not panic
}

func TestAggregateCondition_IsConditionItem(_ *testing.T) {
	var item ConditionItem = AggregateCondition{
		Func:     AggSum,
		Field:    &Field{Name: "amount"},
		Operator: GE,
		Value:    Param{Name: "threshold"},
	}
	item.IsConditionItem() // Should not panic
}

func TestBetweenCondition_IsConditionItem(_ *testing.T) {
	var item ConditionItem = BetweenCondition{
		Field: Field{Name: "age"},
		Low:   Param{Name: "min"},
		High:  Param{Name: "max"},
	}
	item.IsConditionItem() // Should not panic
}

func TestFieldComparison_IsConditionItem(_ *testing.T) {
	var item ConditionItem = FieldComparison{
		LeftField:  Field{Name: "created_at"},
		Operator:   LT,
		RightField: Field{Name: "updated_at"},
	}
	item.IsConditionItem() // Should not panic
}

func TestSubqueryCondition_IsConditionItem(_ *testing.T) {
	var item ConditionItem = SubqueryCondition{
		Operator: EXISTS,
		Subquery: Subquery{AST: &AST{Operation: OpSelect, Target: Table{Name: "users"}}},
	}
	item.IsConditionItem() // Should not panic
}

// =============================================================================
// AST.Validate() Tests
// =============================================================================

func TestAST_Validate_MissingTarget(t *testing.T) {
	ast := &AST{Operation: OpSelect}
	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for missing target table")
	}
	if err.Error() != "target table is required" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_TooManyJoins(t *testing.T) {
	joins := make([]Join, MaxJoinCount+1)
	for i := range joins {
		joins[i] = Join{Type: InnerJoin, Table: Table{Name: "t"}}
	}

	ast := &AST{
		Operation: OpSelect,
		Target:    Table{Name: "users"},
		Joins:     joins,
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for too many JOINs")
	}
}

func TestAST_Validate_TooManyFields(t *testing.T) {
	fields := make([]Field, MaxFieldCount+1)
	for i := range fields {
		fields[i] = Field{Name: "f"}
	}

	ast := &AST{
		Operation: OpSelect,
		Target:    Table{Name: "users"},
		Fields:    fields,
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for too many fields")
	}
}

func TestAST_Validate_TooManyWindowFunctions(t *testing.T) {
	exprs := make([]FieldExpression, MaxWindowFunctions+1)
	for i := range exprs {
		exprs[i] = FieldExpression{Window: &WindowExpression{Function: WinRowNumber}}
	}

	ast := &AST{
		Operation:        OpSelect,
		Target:           Table{Name: "users"},
		FieldExpressions: exprs,
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for too many window functions")
	}
}

func TestAST_Validate_Select_Valid(t *testing.T) {
	ast := &AST{
		Operation: OpSelect,
		Target:    Table{Name: "users"},
		Fields:    []Field{{Name: "id"}},
	}

	if err := ast.Validate(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_Insert_NoValues(t *testing.T) {
	ast := &AST{
		Operation: OpInsert,
		Target:    Table{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for INSERT without values")
	}
	if err.Error() != "INSERT requires at least one value set" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_Insert_Valid(t *testing.T) {
	ast := &AST{
		Operation: OpInsert,
		Target:    Table{Name: "users"},
		Values: []map[Field]Param{
			{{Name: "username"}: {Name: "name"}},
		},
	}

	if err := ast.Validate(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_Insert_InconsistentValueSets(t *testing.T) {
	ast := &AST{
		Operation: OpInsert,
		Target:    Table{Name: "users"},
		Values: []map[Field]Param{
			{{Name: "username"}: {Name: "name1"}},
			{{Name: "email"}: {Name: "email1"}}, // Different field
		},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for inconsistent value sets")
	}
}

func TestAST_Validate_Insert_DifferentValueSetSize(t *testing.T) {
	ast := &AST{
		Operation: OpInsert,
		Target:    Table{Name: "users"},
		Values: []map[Field]Param{
			{{Name: "username"}: {Name: "name1"}, {Name: "email"}: {Name: "email1"}},
			{{Name: "username"}: {Name: "name2"}}, // Missing email
		},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for different value set sizes")
	}
}

func TestAST_Validate_Insert_OnConflictNoColumns(t *testing.T) {
	ast := &AST{
		Operation: OpInsert,
		Target:    Table{Name: "users"},
		Values: []map[Field]Param{
			{{Name: "username"}: {Name: "name"}},
		},
		OnConflict: &ConflictClause{
			Action:  DoNothing,
			Columns: []Field{}, // Empty
		},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for ON CONFLICT without columns")
	}
}

func TestAST_Validate_Update_NoUpdates(t *testing.T) {
	ast := &AST{
		Operation: OpUpdate,
		Target:    Table{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for UPDATE without updates")
	}
}

func TestAST_Validate_Update_Valid(t *testing.T) {
	ast := &AST{
		Operation: OpUpdate,
		Target:    Table{Name: "users"},
		Updates:   map[Field]Param{{Name: "age"}: {Name: "new_age"}},
	}

	if err := ast.Validate(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_Update_WithSelectFeatures(t *testing.T) {
	ast := &AST{
		Operation: OpUpdate,
		Target:    Table{Name: "users"},
		Updates:   map[Field]Param{{Name: "age"}: {Name: "new_age"}},
		Distinct:  true, // Not allowed on UPDATE
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for UPDATE with DISTINCT")
	}
}

func TestAST_Validate_Delete_Valid(t *testing.T) {
	ast := &AST{
		Operation: OpDelete,
		Target:    Table{Name: "users"},
	}

	if err := ast.Validate(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_Delete_WithSelectFeatures(t *testing.T) {
	ast := &AST{
		Operation: OpDelete,
		Target:    Table{Name: "users"},
		Joins:     []Join{{Type: InnerJoin, Table: Table{Name: "t"}}}, // Not allowed
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for DELETE with JOIN")
	}
}

func TestAST_Validate_Count_Valid(t *testing.T) {
	ast := &AST{
		Operation: OpCount,
		Target:    Table{Name: "users"},
	}

	if err := ast.Validate(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAST_Validate_UnsupportedOperation(t *testing.T) {
	ast := &AST{
		Operation: Operation("INVALID"),
		Target:    Table{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for unsupported operation")
	}
}

func TestAST_Validate_HavingWithoutGroupBy(t *testing.T) {
	ast := &AST{
		Operation: OpSelect,
		Target:    Table{Name: "users"},
		Having:    []ConditionItem{AggregateCondition{Func: AggCountField, Operator: GT, Value: Param{Name: "min"}}},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for HAVING without GROUP BY")
	}
}

func TestAST_Validate_DistinctAndDistinctOn(t *testing.T) {
	ast := &AST{
		Operation:  OpSelect,
		Target:     Table{Name: "users"},
		Distinct:   true,
		DistinctOn: []Field{{Name: "id"}},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for both DISTINCT and DISTINCT ON")
	}
}

// =============================================================================
// validateConditionDepth Tests
// =============================================================================

func TestValidateConditionDepth_Simple(t *testing.T) {
	cond := Condition{Field: Field{Name: "a"}, Operator: EQ, Value: Param{Name: "p"}}
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestValidateConditionDepth_FieldComparison(t *testing.T) {
	cond := FieldComparison{LeftField: Field{Name: "a"}, Operator: EQ, RightField: Field{Name: "b"}}
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestValidateConditionDepth_SubqueryCondition(t *testing.T) {
	cond := SubqueryCondition{Operator: EXISTS}
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestValidateConditionDepth_AggregateCondition(t *testing.T) {
	cond := AggregateCondition{Func: AggCountField, Operator: GT, Value: Param{Name: "min"}}
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestValidateConditionDepth_BetweenCondition(t *testing.T) {
	cond := BetweenCondition{Field: Field{Name: "age"}, Low: Param{Name: "min"}, High: Param{Name: "max"}}
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestValidateConditionDepth_NestedWithinLimit(t *testing.T) {
	// Build nested groups up to the limit
	cond := buildNestedConditionGroup(MaxConditionDepth)
	if err := validateConditionDepth(cond, 0); err != nil {
		t.Errorf("Unexpected error for depth %d: %v", MaxConditionDepth, err)
	}
}

func TestValidateConditionDepth_ExceedsLimit(t *testing.T) {
	// Build nested groups exceeding the limit
	cond := buildNestedConditionGroup(MaxConditionDepth + 1)
	err := validateConditionDepth(cond, 0)
	if err == nil {
		t.Error("Expected error for exceeding condition depth")
	}
}

func TestAST_Validate_DeepCondition(t *testing.T) {
	cond := buildNestedConditionGroup(MaxConditionDepth + 1)
	ast := &AST{
		Operation:   OpSelect,
		Target:      Table{Name: "users"},
		WhereClause: cond,
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for deep condition nesting")
	}
}

// buildNestedConditionGroup builds nested condition groups for testing depth limits.
func buildNestedConditionGroup(depth int) ConditionItem {
	if depth <= 0 {
		return Condition{Field: Field{Name: "x"}, Operator: EQ, Value: Param{Name: "p"}}
	}
	return ConditionGroup{
		Logic:      AND,
		Conditions: []ConditionItem{buildNestedConditionGroup(depth - 1)},
	}
}
