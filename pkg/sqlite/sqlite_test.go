package sqlite

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestNew(t *testing.T) {
	r := New()
	if r == nil {
		t.Fatal("New() returned nil")
	}
}

func TestRender_SimpleSelect(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}, {Name: "name"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id", "name" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_SelectWithWhere(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "active"},
			Operator: types.EQ,
			Value:    types.Param{Name: "is_active"},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" WHERE "active" = :is_active`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_Insert(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{
				{Name: "name"}:  {Name: "name_val"},
				{Name: "email"}: {Name: "email_val"},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `INSERT INTO "users" ("email", "name") VALUES (:email_val, :name_val)`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_Update(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpUpdate,
		Target:    types.Table{Name: "users"},
		Updates: map[types.Field]types.Param{
			{Name: "name"}: {Name: "new_name"},
		},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "id"},
			Operator: types.EQ,
			Value:    types.Param{Name: "user_id"},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `UPDATE "users" SET "name" = :new_name WHERE "id" = :user_id`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_Delete(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpDelete,
		Target:    types.Table{Name: "users"},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "id"},
			Operator: types.EQ,
			Value:    types.Param{Name: "user_id"},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `DELETE FROM "users" WHERE "id" = :user_id`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

// Test unsupported features are rejected

func TestRender_RejectsDistinctOn(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation:  types.OpSelect,
		Target:     types.Table{Name: "users"},
		Fields:     []types.Field{{Name: "id"}},
		DistinctOn: []types.Field{{Name: "email"}},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for DISTINCT ON, got nil")
	}
	if !strings.Contains(err.Error(), "DISTINCT ON") {
		t.Errorf("error = %q, want to contain 'DISTINCT ON'", err.Error())
	}
}

func TestRender_RejectsForUpdate(t *testing.T) {
	r := New()
	lock := types.LockForUpdate
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		Lock:      &lock,
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for FOR UPDATE, got nil")
	}
	if !strings.Contains(err.Error(), "row-level locking") {
		t.Errorf("error = %q, want to contain 'row-level locking'", err.Error())
	}
}

func TestRender_RejectsIN(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "status"},
			Operator: types.IN,
			Value:    types.Param{Name: "statuses"},
		},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for IN, got nil")
	}
	if !strings.Contains(err.Error(), "IN") {
		t.Errorf("error = %q, want to contain 'IN'", err.Error())
	}
}

func TestRender_RejectsILIKE(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "name"},
			Operator: types.ILIKE,
			Value:    types.Param{Name: "pattern"},
		},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for ILIKE, got nil")
	}
	if !strings.Contains(err.Error(), "ILIKE") {
		t.Errorf("error = %q, want to contain 'ILIKE'", err.Error())
	}
}

func TestRender_RejectsRegex(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "email"},
			Operator: types.RegexMatch,
			Value:    types.Param{Name: "pattern"},
		},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for regex, got nil")
	}
	if !strings.Contains(err.Error(), "regex") {
		t.Errorf("error = %q, want to contain 'regex'", err.Error())
	}
}

func TestRender_RejectsArrayOperators(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "tags"},
			Operator: types.ArrayContains,
			Value:    types.Param{Name: "tag"},
		},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for array operators, got nil")
	}
	if !strings.Contains(err.Error(), "array") {
		t.Errorf("error = %q, want to contain 'array'", err.Error())
	}
}

func TestRender_RejectsVectorOperators(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "items"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "embedding"},
			Operator: types.VectorL2Distance,
			Value:    types.Param{Name: "query_vec"},
		},
	}

	_, err := r.Render(ast)
	if err == nil {
		t.Fatal("expected error for vector operators, got nil")
	}
	if !strings.Contains(err.Error(), "vector") {
		t.Errorf("error = %q, want to contain 'vector'", err.Error())
	}
}

// Test supported features

func TestRender_SupportsDistinct(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "name"}},
		Distinct:  true,
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT DISTINCT "name" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_SupportsReturning(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{{Name: "name"}: {Name: "name_val"}},
		},
		Returning: []types.Field{{Name: "id"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "RETURNING") {
		t.Errorf("SQL = %q, want to contain 'RETURNING'", result.SQL)
	}
}

func TestRender_SupportsOnConflict(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{{Name: "email"}: {Name: "email_val"}},
		},
		OnConflict: &types.ConflictClause{
			Columns: []types.Field{{Name: "email"}},
			Action:  types.DoNothing,
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "ON CONFLICT") {
		t.Errorf("SQL = %q, want to contain 'ON CONFLICT'", result.SQL)
	}
	if !strings.Contains(result.SQL, "DO NOTHING") {
		t.Errorf("SQL = %q, want to contain 'DO NOTHING'", result.SQL)
	}
}

func TestRender_SupportsLIKE(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "name"},
			Operator: types.LIKE,
			Value:    types.Param{Name: "pattern"},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" WHERE "name" LIKE :pattern`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_SupportsBetween(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.BetweenCondition{
			Field:   types.Field{Name: "amount"},
			Low:     types.Param{Name: "min"},
			High:    types.Param{Name: "max"},
			Negated: false,
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "orders" WHERE "amount" BETWEEN :min AND :max`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_SupportsJoin(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders", Alias: "o"},
		Fields:    []types.Field{{Name: "id", Table: "o"}},
		Joins: []types.Join{
			{
				Type:  types.InnerJoin,
				Table: types.Table{Name: "users", Alias: "u"},
				On: types.FieldComparison{
					LeftField:  types.Field{Name: "user_id", Table: "o"},
					Operator:   types.EQ,
					RightField: types.Field{Name: "id", Table: "u"},
				},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "INNER JOIN") {
		t.Errorf("SQL = %q, want to contain 'INNER JOIN'", result.SQL)
	}
}

func TestRender_SupportsWindowFunction(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "sales"},
		FieldExpressions: []types.FieldExpression{
			{
				Window: &types.WindowExpression{
					Function: types.WinRowNumber,
					Window: types.WindowSpec{
						PartitionBy: []types.Field{{Name: "category"}},
						OrderBy:     []types.OrderBy{{Field: types.Field{Name: "amount"}, Direction: types.DESC}},
					},
				},
				Alias: "rn",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "ROW_NUMBER()") {
		t.Errorf("SQL = %q, want to contain 'ROW_NUMBER()'", result.SQL)
	}
	if !strings.Contains(result.SQL, "OVER") {
		t.Errorf("SQL = %q, want to contain 'OVER'", result.SQL)
	}
}

func TestRenderCompound_Union(t *testing.T) {
	r := New()
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetUnion,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "admins"},
					Fields:    []types.Field{{Name: "id"}},
				},
			},
		},
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" UNION SELECT "id" FROM "admins"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_CastTypesMapCorrectly(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "data"},
		FieldExpressions: []types.FieldExpression{
			{
				Cast: &types.CastExpression{
					Field:    types.Field{Name: "value"},
					CastType: types.CastInteger,
				},
				Alias: "int_val",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite should map to INTEGER, not PostgreSQL's INTEGER
	if !strings.Contains(result.SQL, "CAST") {
		t.Errorf("SQL = %q, want to contain 'CAST'", result.SQL)
	}
	if !strings.Contains(result.SQL, "INTEGER") {
		t.Errorf("SQL = %q, want to contain 'INTEGER'", result.SQL)
	}
}

func TestRender_ParameterizedLimit(t *testing.T) {
	r := New()
	pageSize := types.Param{Name: "page_size"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		Limit:     &types.PaginationValue{Param: &pageSize},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" LIMIT :page_size`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}

	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "page_size" {
		t.Errorf("RequiredParams = %v, want [page_size]", result.RequiredParams)
	}
}

func TestRender_ParameterizedOffset(t *testing.T) {
	r := New()
	offset := types.Param{Name: "offset"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		Offset:    &types.PaginationValue{Param: &offset},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" OFFSET :offset`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}

	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "offset" {
		t.Errorf("RequiredParams = %v, want [offset]", result.RequiredParams)
	}
}

func TestRender_ParameterizedLimitAndOffset(t *testing.T) {
	r := New()
	pageSize := types.Param{Name: "page_size"}
	offset := types.Param{Name: "offset"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		Limit:     &types.PaginationValue{Param: &pageSize},
		Offset:    &types.PaginationValue{Param: &offset},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT "id" FROM "users" LIMIT :page_size OFFSET :offset`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("RequiredParams = %v, want [page_size, offset]", result.RequiredParams)
	}
}

func TestRender_StringUpper(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				String: &types.StringExpression{
					Function: types.StringUpper,
					Field:    types.Field{Name: "name"},
				},
				Alias: "upper_name",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT UPPER("name") AS "upper_name" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_StringLower(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				String: &types.StringExpression{
					Function: types.StringLower,
					Field:    types.Field{Name: "email"},
				},
				Alias: "lower_email",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT LOWER("email") AS "lower_email" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_StringConcat(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				String: &types.StringExpression{
					Function: types.StringConcat,
					Field:    types.Field{Name: "first_name"},
					Fields:   []types.Field{{Name: "last_name"}},
				},
				Alias: "full_name",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses || for concatenation
	expected := `SELECT ("first_name" || "last_name") AS "full_name" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_StringSubstring(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				String: &types.StringExpression{
					Function: types.StringSubstring,
					Field:    types.Field{Name: "name"},
					Args:     []types.Param{{Name: "start"}, {Name: "length"}},
				},
				Alias: "substr",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses SUBSTR instead of SUBSTRING
	expected := `SELECT SUBSTR("name", :start, :length) AS "substr" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_StringReplace(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				String: &types.StringExpression{
					Function: types.StringReplace,
					Field:    types.Field{Name: "name"},
					Args:     []types.Param{{Name: "search"}, {Name: "replacement"}},
				},
				Alias: "replaced",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := `SELECT REPLACE("name", :search, :replacement) AS "replaced" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DateNow(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Date: &types.DateExpression{
					Function: types.DateNow,
				},
				Alias: "current_time",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses DATETIME('now') instead of NOW()
	expected := `SELECT DATETIME('now') AS "current_time" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DateCurrentDate(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Date: &types.DateExpression{
					Function: types.DateCurrentDate,
				},
				Alias: "today",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses DATE('now') instead of CURRENT_DATE
	expected := `SELECT DATE('now') AS "today" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DateExtract(t *testing.T) {
	r := New()
	createdAt := types.Field{Name: "created_at"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Date: &types.DateExpression{
					Function: types.DateExtract,
					Part:     types.PartYear,
					Field:    &createdAt,
				},
				Alias: "year",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses STRFTIME with CAST to INTEGER for EXTRACT
	expected := `SELECT CAST(STRFTIME('%Y', "created_at") AS INTEGER) AS "year" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DateTrunc(t *testing.T) {
	r := New()
	createdAt := types.Field{Name: "created_at"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Date: &types.DateExpression{
					Function: types.DateTrunc,
					Part:     types.PartMonth,
					Field:    &createdAt,
				},
				Alias: "month_start",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// SQLite uses STRFTIME for DATE_TRUNC
	expected := `SELECT STRFTIME('%Y-%m-01 00:00:00', "created_at") AS "month_start" FROM "users"`
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}
