package mariadb

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql/internal/render"
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

	// MariaDB uses backticks for quoting
	expected := "SELECT `id`, `name` FROM `users`"
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

	expected := "SELECT `id` FROM `users` WHERE `active` = :is_active"
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

	expected := "INSERT INTO `users` (`email`, `name`) VALUES (:email_val, :name_val)"
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

	expected := "UPDATE `users` SET `name` = :new_name WHERE `id` = :user_id"
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

	expected := "DELETE FROM `users` WHERE `id` = :user_id"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_OnDuplicateKeyUpdate(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{
				{Name: "id"}:    {Name: "id_val"},
				{Name: "name"}:  {Name: "name_val"},
				{Name: "email"}: {Name: "email_val"},
			},
		},
		OnConflict: &types.ConflictClause{
			Columns: []types.Field{{Name: "id"}},
			Action:  types.DoUpdate,
			Updates: map[types.Field]types.Param{
				{Name: "name"}:  {Name: "new_name"},
				{Name: "email"}: {Name: "new_email"},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// MariaDB uses ON DUPLICATE KEY UPDATE
	if !strings.Contains(result.SQL, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("SQL = %q, want to contain 'ON DUPLICATE KEY UPDATE'", result.SQL)
	}
}

func TestRender_OnDuplicateKeyDoNothing(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{
				{Name: "id"}:   {Name: "id_val"},
				{Name: "name"}: {Name: "name_val"},
			},
		},
		OnConflict: &types.ConflictClause{
			Columns: []types.Field{{Name: "id"}},
			Action:  types.DoNothing,
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// MariaDB DO NOTHING is simulated with a no-op update
	if !strings.Contains(result.SQL, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("SQL = %q, want to contain 'ON DUPLICATE KEY UPDATE'", result.SQL)
	}
}

func TestRender_InsertWithReturning(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpInsert,
		Target:    types.Table{Name: "users"},
		Values: []map[types.Field]types.Param{
			{
				{Name: "name"}: {Name: "name_val"},
			},
		},
		Returning: []types.Field{{Name: "id"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// MariaDB 10.5+ supports RETURNING
	expected := "INSERT INTO `users` (`name`) VALUES (:name_val) RETURNING `id`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_UpdateWithReturning(t *testing.T) {
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
		Returning: []types.Field{{Name: "name"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "UPDATE `users` SET `name` = :new_name WHERE `id` = :user_id RETURNING `name`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DeleteWithReturning(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpDelete,
		Target:    types.Table{Name: "users"},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "id"},
			Operator: types.EQ,
			Value:    types.Param{Name: "user_id"},
		},
		Returning: []types.Field{{Name: "id"}, {Name: "name"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "DELETE FROM `users` WHERE `id` = :user_id RETURNING `id`, `name`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

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
		t.Fatal("expected error for DISTINCT ON")
	}
	if !strings.Contains(err.Error(), "DISTINCT ON") {
		t.Errorf("error = %q, want to mention DISTINCT ON", err.Error())
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
		t.Fatal("expected error for array operators")
	}
}

func TestRender_SupportsIN(t *testing.T) {
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

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// MariaDB uses standard IN syntax
	expected := "SELECT `id` FROM `users` WHERE `status` IN (:statuses)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_SupportsILIKE(t *testing.T) {
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

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// MariaDB LIKE is case-insensitive by default
	expected := "SELECT `id` FROM `users` WHERE `name` LIKE :pattern"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
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

	expected := "(SELECT `id` FROM `users`) UNION (SELECT `id` FROM `admins`)"
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

	// MariaDB uses CONCAT()
	expected := "SELECT CONCAT(`first_name`, `last_name`) AS `full_name` FROM `users`"
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

	expected := "SELECT NOW() AS `current_time` FROM `users`"
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

	// MariaDB uses CURDATE()
	expected := "SELECT CURDATE() AS `today` FROM `users`"
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

	expected := "SELECT EXTRACT(YEAR FROM `created_at`) AS `year` FROM `users`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_DateTruncMonth(t *testing.T) {
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

	// MariaDB uses DATE_FORMAT for month truncation
	expected := "SELECT DATE_FORMAT(`created_at`, '%Y-%m-01') AS `month_start` FROM `users`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_ForUpdate(t *testing.T) {
	r := New()
	lock := types.LockForUpdate
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		Lock:      &lock,
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `id` FROM `users` FOR UPDATE"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
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

	expected := "SELECT `id` FROM `users` LIMIT :page_size"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_WindowFunction(t *testing.T) {
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

func TestRender_Count(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpCount,
		Target:    types.Table{Name: "users"},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT COUNT(*) FROM `users`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_CountWithWhere(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpCount,
		Target:    types.Table{Name: "users"},
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

	expected := "SELECT COUNT(*) FROM `users` WHERE `active` = :is_active"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_AggregateExpression(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders"},
		FieldExpressions: []types.FieldExpression{
			{
				Aggregate: types.AggSum,
				Field:     types.Field{Name: "total"},
				Alias:     "total_sum",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT SUM(`total`) AS `total_sum` FROM `orders`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_AggregateWithGroupBy(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders"},
		Fields:    []types.Field{{Name: "user_id"}},
		FieldExpressions: []types.FieldExpression{
			{
				Aggregate: types.AggCountField,
				Alias:     "order_count",
			},
		},
		GroupBy: []types.Field{{Name: "user_id"}},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `user_id`, COUNT(*) AS `order_count` FROM `orders` GROUP BY `user_id`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_CaseExpression(t *testing.T) {
	r := New()
	elseVal := types.Param{Name: "default_val"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Case: &types.CaseExpression{
					WhenClauses: []types.WhenClause{
						{
							Condition: types.Condition{
								Field:    types.Field{Name: "status"},
								Operator: types.EQ,
								Value:    types.Param{Name: "active_status"},
							},
							Result: types.Param{Name: "active_label"},
						},
					},
					ElseValue: &elseVal,
				},
				Alias: "status_label",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "CASE WHEN") {
		t.Errorf("SQL = %q, want to contain 'CASE WHEN'", result.SQL)
	}
	if !strings.Contains(result.SQL, "ELSE") {
		t.Errorf("SQL = %q, want to contain 'ELSE'", result.SQL)
	}
}

func TestRender_CoalesceExpression(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				Coalesce: &types.CoalesceExpression{
					Values: []types.Param{{Name: "nickname"}, {Name: "username"}},
				},
				Alias: "display_name",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT COALESCE(:nickname, :username) AS `display_name` FROM `users`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_NullIfExpression(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		FieldExpressions: []types.FieldExpression{
			{
				NullIf: &types.NullIfExpression{
					Value1: types.Param{Name: "status"},
					Value2: types.Param{Name: "empty_status"},
				},
				Alias: "clean_status",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT NULLIF(:status, :empty_status) AS `clean_status` FROM `users`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_BetweenCondition(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "products"},
		Fields:    []types.Field{{Name: "name"}, {Name: "price"}},
		WhereClause: types.BetweenCondition{
			Field: types.Field{Name: "price"},
			Low:   types.Param{Name: "min_price"},
			High:  types.Param{Name: "max_price"},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `name`, `price` FROM `products` WHERE `price` BETWEEN :min_price AND :max_price"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_ConditionGroup_AND(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.ConditionGroup{
			Logic: types.AND,
			Conditions: []types.ConditionItem{
				types.Condition{
					Field:    types.Field{Name: "active"},
					Operator: types.EQ,
					Value:    types.Param{Name: "is_active"},
				},
				types.Condition{
					Field:    types.Field{Name: "age"},
					Operator: types.GE,
					Value:    types.Param{Name: "min_age"},
				},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `id` FROM `users` WHERE (`active` = :is_active AND `age` >= :min_age)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_ConditionGroup_OR(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.ConditionGroup{
			Logic: types.OR,
			Conditions: []types.ConditionItem{
				types.Condition{
					Field:    types.Field{Name: "role"},
					Operator: types.EQ,
					Value:    types.Param{Name: "admin_role"},
				},
				types.Condition{
					Field:    types.Field{Name: "role"},
					Operator: types.EQ,
					Value:    types.Param{Name: "super_role"},
				},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `id` FROM `users` WHERE (`role` = :admin_role OR `role` = :super_role)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_Subquery(t *testing.T) {
	r := New()
	subquery := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders"},
		Fields:    []types.Field{{Name: "user_id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "status"},
			Operator: types.EQ,
			Value:    types.Param{Name: "order_status"},
		},
	}

	idField := types.Field{Name: "id"}
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "name"}},
		WhereClause: types.SubqueryCondition{
			Field:    &idField,
			Operator: types.IN,
			Subquery: types.Subquery{AST: subquery},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "IN (SELECT") {
		t.Errorf("SQL = %q, want to contain 'IN (SELECT'", result.SQL)
	}
}

func TestRender_HavingCondition(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "orders"},
		Fields:    []types.Field{{Name: "user_id"}},
		FieldExpressions: []types.FieldExpression{
			{
				Aggregate: types.AggCountField,
				Alias:     "order_count",
			},
		},
		GroupBy: []types.Field{{Name: "user_id"}},
		Having: []types.ConditionItem{
			types.AggregateCondition{
				Func:     types.AggCountField,
				Operator: types.GE,
				Value:    types.Param{Name: "min_orders"},
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "HAVING COUNT(*)") {
		t.Errorf("SQL = %q, want to contain 'HAVING COUNT(*)'", result.SQL)
	}
}

func TestRender_Join(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users", Alias: "u"},
		Fields:    []types.Field{{Name: "name", Table: "u"}},
		Joins: []types.Join{
			{
				Type:  types.InnerJoin,
				Table: types.Table{Name: "orders", Alias: "o"},
				On: types.FieldComparison{
					LeftField:  types.Field{Name: "id", Table: "u"},
					Operator:   types.EQ,
					RightField: types.Field{Name: "user_id", Table: "o"},
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

func TestRender_MathExpression(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "products"},
		FieldExpressions: []types.FieldExpression{
			{
				Math: &types.MathExpression{
					Function: types.MathFloor,
					Field:    types.Field{Name: "price"},
				},
				Alias: "floor_price",
			},
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT FLOOR(`price`) AS `floor_price` FROM `products`"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_Operators(t *testing.T) {
	tests := []struct {
		name     string
		operator types.Operator
		expected string
	}{
		{"NE", types.NE, "SELECT `id` FROM `users` WHERE `status` != :status"},
		{"GT", types.GT, "SELECT `id` FROM `users` WHERE `status` > :status"},
		{"GE", types.GE, "SELECT `id` FROM `users` WHERE `status` >= :status"},
		{"LT", types.LT, "SELECT `id` FROM `users` WHERE `status` < :status"},
		{"LE", types.LE, "SELECT `id` FROM `users` WHERE `status` <= :status"},
		{"LIKE", types.LIKE, "SELECT `id` FROM `users` WHERE `status` LIKE :status"},
	}

	r := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := &types.AST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users"},
				Fields:    []types.Field{{Name: "id"}},
				WhereClause: types.Condition{
					Field:    types.Field{Name: "status"},
					Operator: tt.operator,
					Value:    types.Param{Name: "status"},
				},
			}

			result, err := r.Render(ast)
			if err != nil {
				t.Fatalf("Render() error = %v", err)
			}

			if result.SQL != tt.expected {
				t.Errorf("SQL = %q, want %q", result.SQL, tt.expected)
			}
		})
	}
}

func TestRender_IsNull(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "deleted_at"},
			Operator: types.IsNull,
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `id` FROM `users` WHERE `deleted_at` IS NULL"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRender_IsNotNull(t *testing.T) {
	r := New()
	ast := &types.AST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
		Fields:    []types.Field{{Name: "id"}},
		WhereClause: types.Condition{
			Field:    types.Field{Name: "email"},
			Operator: types.IsNotNull,
		},
	}

	result, err := r.Render(ast)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	expected := "SELECT `id` FROM `users` WHERE `email` IS NOT NULL"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

// =============================================================================
// Additional RenderCompound Tests
// =============================================================================

func TestRenderCompound_UnionAll(t *testing.T) {
	r := New()
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetUnionAll,
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

	expected := "(SELECT `id` FROM `users`) UNION ALL (SELECT `id` FROM `admins`)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRenderCompound_Intersect(t *testing.T) {
	r := New()
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetIntersect,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "active_users"},
					Fields:    []types.Field{{Name: "id"}},
				},
			},
		},
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	expected := "(SELECT `id` FROM `users`) INTERSECT (SELECT `id` FROM `active_users`)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRenderCompound_Except(t *testing.T) {
	r := New()
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetExcept,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "banned_users"},
					Fields:    []types.Field{{Name: "id"}},
				},
			},
		},
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	expected := "(SELECT `id` FROM `users`) EXCEPT (SELECT `id` FROM `banned_users`)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRenderCompound_WithOrderByLimit(t *testing.T) {
	r := New()
	limitVal := 10
	limit := types.PaginationValue{Static: &limitVal}
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}, {Name: "name"}},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetUnion,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "admins"},
					Fields:    []types.Field{{Name: "id"}, {Name: "name"}},
				},
			},
		},
		Ordering: []types.OrderBy{{Field: types.Field{Name: "name"}, Direction: types.ASC}},
		Limit:    &limit,
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	expected := "(SELECT `id`, `name` FROM `users`) UNION (SELECT `id`, `name` FROM `admins`) ORDER BY `name` ASC LIMIT 10"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRenderCompound_MultipleOperations(t *testing.T) {
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
			{
				Operation: types.SetIntersect,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "active_users"},
					Fields:    []types.Field{{Name: "id"}},
				},
			},
		},
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	expected := "(SELECT `id` FROM `users`) UNION (SELECT `id` FROM `admins`) INTERSECT (SELECT `id` FROM `active_users`)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}
}

func TestRenderCompound_ParameterNamespacing(t *testing.T) {
	r := New()
	query := &types.CompoundQuery{
		Base: &types.AST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields:    []types.Field{{Name: "id"}},
			WhereClause: types.Condition{
				Field:    types.Field{Name: "active"},
				Operator: types.EQ,
				Value:    types.Param{Name: "is_active"},
			},
		},
		Operands: []types.SetOperand{
			{
				Operation: types.SetUnion,
				AST: &types.AST{
					Operation: types.OpSelect,
					Target:    types.Table{Name: "admins"},
					Fields:    []types.Field{{Name: "id"}},
					WhereClause: types.Condition{
						Field:    types.Field{Name: "active"},
						Operator: types.EQ,
						Value:    types.Param{Name: "is_active"},
					},
				},
			},
		},
	}

	result, err := r.RenderCompound(query)
	if err != nil {
		t.Fatalf("RenderCompound() error = %v", err)
	}

	// Parameters should be namespaced with q0_ and q1_ prefixes
	expected := "(SELECT `id` FROM `users` WHERE `active` = :q0_is_active) UNION (SELECT `id` FROM `admins` WHERE `active` = :q1_is_active)"
	if result.SQL != expected {
		t.Errorf("SQL = %q, want %q", result.SQL, expected)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("RequiredParams = %v, want 2 params", result.RequiredParams)
	}
}

func TestCapabilities(t *testing.T) {
	r := New()
	caps := r.Capabilities()

	if caps.DistinctOn {
		t.Error("DistinctOn should be false")
	}
	if !caps.Upsert {
		t.Error("Upsert should be true")
	}
	if !caps.Returning {
		t.Error("Returning should be true")
	}
	if !caps.CaseInsensitiveLike {
		t.Error("CaseInsensitiveLike should be true")
	}
	if caps.RegexOperators {
		t.Error("RegexOperators should be false")
	}
	if caps.ArrayOperators {
		t.Error("ArrayOperators should be false")
	}
	if !caps.InArray {
		t.Error("InArray should be true")
	}
	if caps.RowLocking != render.RowLockingBasic {
		t.Errorf("RowLocking = %v, want RowLockingBasic", caps.RowLocking)
	}
}
