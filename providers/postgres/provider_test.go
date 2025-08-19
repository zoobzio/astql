package postgres

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

func TestNewProvider(t *testing.T) {
	p := NewProvider()
	if p == nil {
		t.Error("Expected provider to be created")
	}
}

func TestRenderBasicSelect(t *testing.T) {
	p := NewProvider()

	// Create a simple SELECT query
	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields: []types.Field{
				{Name: "id"},
				types.Field{Name: "name"},
				types.Field{Name: "email"},
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT "id", "name", "email" FROM "users"`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}

	if len(result.RequiredParams) != 0 {
		t.Errorf("Expected no parameters, got: %v", result.RequiredParams)
	}
}

func TestRenderSelectAll(t *testing.T) {
	p := NewProvider()

	// SELECT * query (no fields specified)
	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT * FROM "users"`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

func TestRenderSelectWithWhere(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Fields: []types.Field{
				{Name: "id"},
				types.Field{Name: "name"},
			},
			WhereClause: astql.C(
				types.Field{Name: "age"},
				types.GT,
				types.Param{Name: "minAge"},
			),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT "id", "name" FROM "users" WHERE "age" > :minAge`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}

	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "minAge" {
		t.Errorf("Expected params [minAge], got: %v", result.RequiredParams)
	}
}

func TestRenderSelectWithMultipleConditions(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			WhereClause: astql.And(
				astql.C(types.Field{Name: "age"}, types.GE, types.Param{Name: "minAge"}),
				astql.C(types.Field{Name: "age"}, types.LE, types.Param{Name: "maxAge"}),
				astql.C(types.Field{Name: "status"}, types.EQ, types.Param{Name: "status"}),
			),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT * FROM "users" WHERE ("age" >= :minAge AND "age" <= :maxAge AND "status" = :status)`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}

	// Check all required params are present
	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got: %d", len(result.RequiredParams))
	}
}

// Test for potential SQL injection via field names.
func TestFieldTableNameSafety(t *testing.T) {
	p := NewProvider()

	// Code smell #1: Field and table names are not quoted in PostgreSQL
	// This could be problematic with reserved words or special characters
	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "order"}, // "order" is a reserved word
			Fields: []types.Field{
				types.Field{Name: "select"}, // "select" is a reserved word
				types.Field{Name: "from"},   // "from" is a reserved word
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Without quotes, this SQL would fail due to reserved words
	t.Logf("Generated SQL: %s", result.SQL)

	// The SQL should either quote identifiers or the validator should prevent reserved words
	// Current output: "SELECT select, from FROM order" - This is invalid SQL!
}

func TestFieldWithTablePrefix(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users", Alias: "u"},
			Fields: []types.Field{
				types.Field{Name: "id"}.WithTable("u"),
				types.Field{Name: "name"}.WithTable("u"),
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT u."id", u."name" FROM "users" u`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

func TestParameterDeduplication(t *testing.T) {
	p := NewProvider()

	// Using the same parameter multiple times
	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "products"},
			WhereClause: astql.Or(
				astql.C(types.Field{Name: "category"}, types.EQ, types.Param{Name: "cat"}),
				astql.C(types.Field{Name: "subcategory"}, types.EQ, types.Param{Name: "cat"}),
				astql.C(types.Field{Name: "tags"}, types.LIKE, types.Param{Name: "cat"}),
			),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should deduplicate - only one "cat" param
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 unique param, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}

	expectedSQL := `SELECT * FROM "products" WHERE ("category" = :cat OR "subcategory" = :cat OR "tags" LIKE :cat)`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

func TestInsertBasic(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpInsert,
			Target:    types.Table{Name: "users"},
			Values: []map[types.Field]types.Param{
				{
					types.Field{Name: "name"}:  types.Param{Name: "userName"},
					types.Field{Name: "email"}: types.Param{Name: "userEmail"},
					types.Field{Name: "age"}:   types.Param{Name: "userAge"},
				},
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Note: Map iteration order is not guaranteed, so we can't check exact SQL
	t.Logf("Generated SQL: %s", result.SQL)

	// Should have all 3 params
	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}

	// Check it contains the basic structure
	if !strings.Contains(result.SQL, `INSERT INTO "users"`) {
		t.Error("SQL should start with INSERT INTO users")
	}
	if !strings.Contains(result.SQL, "VALUES") {
		t.Error("SQL should contain VALUES")
	}
}

// Test NULL operations.
func TestNullOperations(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			WhereClause: astql.And(
				astql.Null(types.Field{Name: "deleted_at"}),
				astql.NotNull(types.Field{Name: "email"}),
			),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT * FROM "users" WHERE ("deleted_at" IS NULL AND "email" IS NOT NULL)`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}

	// Should have no parameters for IS NULL/IS NOT NULL
	if len(result.RequiredParams) != 0 {
		t.Errorf("Expected no params for NULL checks, got: %v", result.RequiredParams)
	}
}

func TestUpdateBasic(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpUpdate,
			Target:    types.Table{Name: "users"},
			Updates: map[types.Field]types.Param{
				types.Field{Name: "name"}:       types.Param{Name: "newName"},
				types.Field{Name: "updated_at"}: types.Param{Name: "now"},
			},
			WhereClause: astql.C(types.Field{Name: "id"}, types.EQ, types.Param{Name: "userId"}),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	t.Logf("Generated SQL: %s", result.SQL)

	// Should contain basic UPDATE structure
	if !strings.Contains(result.SQL, `UPDATE "users" SET`) {
		t.Error("SQL should contain UPDATE users SET")
	}
	if !strings.Contains(result.SQL, `WHERE "id" = :userId`) {
		t.Error("SQL should contain WHERE clause")
	}

	// Should have all params
	if len(result.RequiredParams) != 3 {
		t.Errorf("Expected 3 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

// Code smell check: What happens with empty conditions?
func TestEmptyConditions(t *testing.T) {
	t.Run("Empty AND group", func(t *testing.T) {
		// Empty AND should panic at creation time
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for empty AND group")
			} else if !strings.Contains(r.(error).Error(), "AND requires at least one condition") {
				t.Errorf("Expected 'AND requires at least one condition' error, got: %v", r)
			}
		}()

		// This should panic
		astql.And()
	})

	t.Run("Empty OR group", func(t *testing.T) {
		// Empty OR should panic at creation time
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for empty OR group")
			} else if !strings.Contains(r.(error).Error(), "OR requires at least one condition") {
				t.Errorf("Expected 'OR requires at least one condition' error, got: %v", r)
			}
		}()

		// This should panic
		astql.Or()
	})
}

func TestJoins(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users", Alias: "u"},
			Fields: []types.Field{
				types.Field{Name: "id"}.WithTable("u"),
				types.Field{Name: "name"}.WithTable("u"),
				types.Field{Name: "title"}.WithTable("p"),
			},
		},
		Joins: []Join{
			{
				Type:  InnerJoin,
				Table: types.Table{Name: "posts", Alias: "p"},
				On: astql.C(
					types.Field{Name: "user_id"}.WithTable("p"),
					types.EQ,
					types.Param{Name: "userId"},
				),
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT u."id", u."name", p."title" FROM "users" u INNER JOIN "posts" p ON p."user_id" = :userId`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, result.SQL)
	}
}

func TestDeleteBasic(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation:   types.OpDelete,
			Target:      types.Table{Name: "users"},
			WhereClause: astql.C(types.Field{Name: "id"}, types.EQ, types.Param{Name: "userId"}),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `DELETE FROM "users" WHERE "id" = :userId`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

func TestCount(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation:   types.OpCount,
			Target:      types.Table{Name: "users"},
			WhereClause: astql.C(types.Field{Name: "active"}, types.EQ, types.Param{Name: "isActive"}),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT COUNT(*) FROM "users" WHERE "active" = :isActive`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

// Test invalid AST.
func TestInvalidAST(t *testing.T) {
	p := NewProvider()

	t.Run("Missing target table", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				// Missing Target
			},
		}

		_, err := p.Render(ast)
		if err == nil {
			t.Error("Expected error for missing target table")
		}
	})

	t.Run("Invalid operation", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: "INVALID_OP",
				Target:    types.Table{Name: "users"},
			},
		}

		_, err := p.Render(ast)
		if err == nil {
			t.Error("Expected error for invalid operation")
		}
		if !strings.Contains(err.Error(), "unsupported operation") {
			t.Errorf("Expected 'unsupported operation' error, got: %v", err)
		}
	})
}

// Code smell: What happens with special PostgreSQL features?
func TestPostgreSQLSpecificFeatures(t *testing.T) {
	p := NewProvider()

	t.Run("RETURNING clause", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpInsert,
				Target:    types.Table{Name: "users"},
				Values: []map[types.Field]types.Param{
					{
						types.Field{Name: "name"}: types.Param{Name: "userName"},
					},
				},
			},
			Returning: []types.Field{
				{Name: "id"},
				types.Field{Name: "created_at"},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !strings.Contains(result.SQL, `RETURNING "id", "created_at"`) {
			t.Errorf("Expected RETURNING clause in SQL: %s", result.SQL)
		}
	})

	t.Run("DISTINCT", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users"},
				Fields: []types.Field{
					types.Field{Name: "email"},
				},
			},
			Distinct: true,
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `SELECT DISTINCT "email" FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}
	})
}

// Code smell: Check ORDER BY and LIMIT/OFFSET.
func TestOrderByLimitOffset(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
			Ordering: []types.OrderBy{
				{Field: types.Field{Name: "created_at"}, Direction: types.DESC},
				{Field: types.Field{Name: "name"}, Direction: types.ASC},
			},
			Limit:  intPtrProvider(10),
			Offset: intPtrProvider(20),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedSQL := `SELECT * FROM "users" ORDER BY "created_at" DESC, "name" ASC LIMIT 10 OFFSET 20`
	if result.SQL != expectedSQL {
		t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
	}
}

func intPtrProvider(i int) *int {
	return &i
}

// Test CASE expressions.
func TestCaseExpressions(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
		},
		FieldExpressions: []FieldExpression{
			{
				Field: types.Field{Name: "name"},
			},
			{
				Case: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "minAge"}),
							Result:    types.Param{Name: "child"},
						},
						{
							Condition: astql.C(types.Field{Name: "age"}, types.GE, types.Param{Name: "seniorAge"}),
							Result:    types.Param{Name: "senior"},
						},
					},
					ElseValue: &types.Param{Name: "adult"},
				},
				Alias: "age_group",
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check CASE expression is rendered correctly
	if !strings.Contains(result.SQL, "CASE WHEN") {
		t.Errorf("Expected CASE WHEN in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "AS age_group") {
		t.Errorf("Expected alias 'AS age_group' in SQL: %s", result.SQL)
	}

	// Should have all parameters
	expectedParams := []string{"minAge", "child", "seniorAge", "senior", "adult"}
	if len(result.RequiredParams) != len(expectedParams) {
		t.Errorf("Expected %d params, got %d: %v", len(expectedParams), len(result.RequiredParams), result.RequiredParams)
	}
}

// Test math expressions.
func TestMathExpressions(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "products"},
		},
		FieldExpressions: []FieldExpression{
			{
				Field: types.Field{Name: "name"},
			},
			{
				Math: &MathExpression{
					Function:  MathRound,
					Field:     types.Field{Name: "price"},
					Precision: &types.Param{Name: "precision"},
				},
				Alias: "rounded_price",
			},
			{
				Math: &MathExpression{
					Function: MathFloor,
					Field:    types.Field{Name: "discount"},
				},
				Alias: "floor_discount",
			},
			{
				Math: &MathExpression{
					Function: MathCeil,
					Field:    types.Field{Name: "weight"},
				},
				Alias: "ceil_weight",
			},
			{
				Math: &MathExpression{
					Function: MathAbs,
					Field:    types.Field{Name: "balance"},
				},
				Alias: "abs_balance",
			},
			{
				Math: &MathExpression{
					Function: MathSqrt,
					Field:    types.Field{Name: "area"},
				},
				Alias: "sqrt_area",
			},
			{
				Math: &MathExpression{
					Function: MathPower,
					Field:    types.Field{Name: "base"},
					Exponent: &types.Param{Name: "exponent"},
				},
				Alias: "power_result",
			},
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check all math functions are rendered
	if !strings.Contains(result.SQL, "ROUND(\"price\", :precision) AS rounded_price") {
		t.Errorf("Expected ROUND function in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "FLOOR(\"discount\") AS floor_discount") {
		t.Errorf("Expected FLOOR function in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "CEIL(\"weight\") AS ceil_weight") {
		t.Errorf("Expected CEIL function in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "ABS(\"balance\") AS abs_balance") {
		t.Errorf("Expected ABS function in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "SQRT(\"area\") AS sqrt_area") {
		t.Errorf("Expected SQRT function in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "POWER(\"base\", :exponent) AS power_result") {
		t.Errorf("Expected POWER function in SQL: %s", result.SQL)
	}
}

// Test PostgreSQL Listen/Notify operations.
func TestListenNotify(t *testing.T) {
	p := NewProvider()

	t.Run("LISTEN", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpListen,
				Target:    types.Table{Name: "users"},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `LISTEN users_changes`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}

		if len(result.RequiredParams) != 0 {
			t.Errorf("Expected no params for LISTEN, got: %v", result.RequiredParams)
		}
	})

	t.Run("NOTIFY without payload", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpNotify,
				Target:    types.Table{Name: "orders"},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `NOTIFY orders_changes`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}
	})

	t.Run("NOTIFY with payload", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation:     types.OpNotify,
				Target:        types.Table{Name: "events"},
				NotifyPayload: &types.Param{Name: "payload"},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `NOTIFY events_changes, :payload`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "payload" {
			t.Errorf("Expected params [payload], got: %v", result.RequiredParams)
		}
	})

	t.Run("UNLISTEN", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpUnlisten,
				Target:    types.Table{Name: "notifications"},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `UNLISTEN notifications_changes`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}
	})
}

// Test aggregates with GROUP BY and HAVING.
func TestAggregatesGroupByHaving(t *testing.T) {
	p := NewProvider()

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "orders"},
		},
		FieldExpressions: []FieldExpression{
			{
				Field: types.Field{Name: "user_id"},
			},
			{
				Aggregate: AggSum,
				Field:     types.Field{Name: "total"},
				Alias:     "total_amount",
			},
			{
				Aggregate: AggCountField,
				Field:     types.Field{Name: "id"},
				Alias:     "order_count",
			},
			{
				Aggregate: AggAvg,
				Field:     types.Field{Name: "total"},
				Alias:     "avg_order",
			},
		},
		GroupBy: []types.Field{
			types.Field{Name: "user_id"},
		},
		Having: []types.Condition{
			astql.C(types.Field{Name: "total_amount"}, types.GT, types.Param{Name: "minTotal"}),
			astql.C(types.Field{Name: "order_count"}, types.GE, types.Param{Name: "minOrders"}),
		},
	}

	result, err := p.Render(ast)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check all components are present
	if !strings.Contains(result.SQL, "SUM(\"total\") AS total_amount") {
		t.Errorf("Expected SUM aggregate in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "COUNT(\"id\") AS order_count") {
		t.Errorf("Expected COUNT aggregate in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "AVG(\"total\") AS avg_order") {
		t.Errorf("Expected AVG aggregate in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "GROUP BY \"user_id\"") {
		t.Errorf("Expected GROUP BY in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "HAVING") {
		t.Errorf("Expected HAVING in SQL: %s", result.SQL)
	}

	// Check parameters
	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

// Test COALESCE and NULLIF.
func TestCoalesceNullIf(t *testing.T) {
	p := NewProvider()

	t.Run("COALESCE", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users"},
			},
			FieldExpressions: []FieldExpression{
				{
					Coalesce: &CoalesceExpression{
						Values: []types.Param{
							types.Param{Name: "preferredName"},
							types.Param{Name: "firstName"},
							types.Param{Name: "defaultName"},
						},
					},
					Alias: "display_name",
				},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `SELECT COALESCE(:preferredName, :firstName, :defaultName) AS display_name FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}
	})

	t.Run("NULLIF", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users"},
			},
			FieldExpressions: []FieldExpression{
				{
					NullIf: &NullIfExpression{
						Value1: types.Param{Name: "status"},
						Value2: types.Param{Name: "emptyValue"},
					},
					Alias: "real_status",
				},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `SELECT NULLIF(:status, :emptyValue) AS real_status FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}
	})
}

// Test PostgreSQL subqueries.
func TestSubqueries(t *testing.T) {
	p := NewProvider()

	t.Run("IN subquery", func(t *testing.T) {
		subqueryAST := &types.QueryAST{
			Operation:   types.OpSelect,
			Target:      types.Table{Name: "departments"},
			Fields:      []types.Field{types.Field{Name: "id"}},
			WhereClause: astql.C(types.Field{Name: "active"}, types.EQ, types.Param{Name: "isActive"}),
		}

		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users"},
				WhereClause: SubqueryCondition{
					Field:    &types.Field{Name: "department_id"},
					Operator: types.IN,
					Subquery: Subquery{AST: subqueryAST},
				},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedSQL := `SELECT * FROM "users" WHERE "department_id" IN (SELECT "id" FROM "departments" WHERE "active" = :sq1_isActive)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, result.SQL)
		}

		// Subqueries prefix their parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "sq1_isActive" {
			t.Errorf("Expected params [sq1_isActive], got: %v", result.RequiredParams)
		}
	})

	t.Run("EXISTS subquery", func(t *testing.T) {
		subqueryAST := &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "orders"},
			WhereClause: astql.And(
				FieldComparison{
					LeftField:  types.Field{Name: "user_id"}.WithTable("o"),
					Operator:   types.EQ,
					RightField: types.Field{Name: "id"}.WithTable("u"),
				},
				astql.C(types.Field{Name: "total"}.WithTable("o"), types.GT, types.Param{Name: "minTotal"}),
			),
		}

		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users", Alias: "u"},
				WhereClause: SubqueryCondition{
					Operator: types.EXISTS,
					Subquery: Subquery{AST: subqueryAST},
				},
			},
		}

		result, err := p.Render(ast)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !strings.Contains(result.SQL, "EXISTS (SELECT * FROM") {
			t.Errorf("Expected EXISTS subquery in SQL: %s", result.SQL)
		}

		// Subqueries prefix their parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "sq1_minTotal" {
			t.Errorf("Expected params [sq1_minTotal], got: %v", result.RequiredParams)
		}
	})
}

// Test that field order is deterministic in INSERT/UPDATE operations.
func TestDeterministicFieldOrder(t *testing.T) {
	p := NewProvider()

	t.Run("INSERT field order", func(t *testing.T) {
		// Run multiple times to ensure consistent output
		var results []string
		for i := 0; i < 10; i++ {
			ast := &AST{
				QueryAST: &types.QueryAST{
					Operation: types.OpInsert,
					Target:    types.Table{Name: "users"},
					Values: []map[types.Field]types.Param{
						{
							types.Field{Name: "name"}:  types.Param{Name: "userName"},
							types.Field{Name: "email"}: types.Param{Name: "userEmail"},
							types.Field{Name: "age"}:   types.Param{Name: "userAge"},
							types.Field{Name: "city"}:  types.Param{Name: "userCity"},
						},
					},
				},
			}

			result, err := p.Render(ast)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			results = append(results, result.SQL)
		}

		// All results should be identical
		for i := 1; i < len(results); i++ {
			if results[i] != results[0] {
				t.Errorf("Non-deterministic output detected:\nFirst:  %s\nOther:  %s", results[0], results[i])
			}
		}

		// Check that fields are in alphabetical order
		expectedSQL := `INSERT INTO "users" ("age", "city", "email", "name") VALUES (:userAge, :userCity, :userEmail, :userName)`
		if results[0] != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, results[0])
		}
	})

	t.Run("UPDATE field order", func(t *testing.T) {
		// Run multiple times to ensure consistent output
		var results []string
		for i := 0; i < 10; i++ {
			ast := &AST{
				QueryAST: &types.QueryAST{
					Operation: types.OpUpdate,
					Target:    types.Table{Name: "users"},
					Updates: map[types.Field]types.Param{
						types.Field{Name: "name"}:       types.Param{Name: "newName"},
						types.Field{Name: "email"}:      types.Param{Name: "newEmail"},
						types.Field{Name: "updated_at"}: types.Param{Name: "now"},
					},
					WhereClause: astql.C(types.Field{Name: "id"}, types.EQ, types.Param{Name: "userId"}),
				},
			}

			result, err := p.Render(ast)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			results = append(results, result.SQL)
		}

		// All results should be identical
		for i := 1; i < len(results); i++ {
			if results[i] != results[0] {
				t.Errorf("Non-deterministic output detected:\nFirst:  %s\nOther:  %s", results[0], results[i])
			}
		}

		// Check that fields are in alphabetical order
		expectedSQL := `UPDATE "users" SET "email" = :newEmail, "name" = :newName, "updated_at" = :now WHERE "id" = :userId`
		if results[0] != expectedSQL {
			t.Errorf("Expected SQL: %s\nGot: %s", expectedSQL, results[0])
		}
	})
}
