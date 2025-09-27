package postgres

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

func TestSelectBuilder(t *testing.T) {
	t.Run("Basic SELECT", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"})

		if builder.pgAst == nil {
			t.Error("Expected pgAst to be created")
		}
		if builder.pgAst.Operation != types.OpSelect {
			t.Errorf("Expected OpSelect, got %s", builder.pgAst.Operation)
		}
		if builder.pgAst.Target.Name != "users" {
			t.Errorf("Expected table 'users', got %s", builder.pgAst.Target.Name)
		}
	})

	t.Run("SELECT with DISTINCT", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"}).Distinct()

		if !builder.pgAst.Distinct {
			t.Error("Expected Distinct to be true")
		}

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		if !ast.Distinct {
			t.Error("Expected AST Distinct to be true")
		}
	})
}

func TestInsertBuilder(t *testing.T) {
	t.Run("Basic INSERT", func(t *testing.T) {
		builder := Insert(types.Table{Name: "users"})

		if builder.pgAst.Operation != types.OpInsert {
			t.Errorf("Expected OpInsert, got %s", builder.pgAst.Operation)
		}
	})

	t.Run("INSERT with ON CONFLICT", func(t *testing.T) {
		builder := Insert(types.Table{Name: "users"}).
			Values(map[types.Field]types.Param{
				{Name: "name"}:             {Name: "userName"},
				types.Field{Name: "email"}: types.Param{Name: "userEmail"},
			}).
			OnConflict(types.Field{Name: "email"}).DoNothing()

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.OnConflict == nil {
			t.Error("Expected OnConflict to be set")
		}
		if ast.OnConflict.Action != DoNothing {
			t.Errorf("Expected DoNothing action, got %s", ast.OnConflict.Action)
		}
		if len(ast.OnConflict.Columns) != 1 || ast.OnConflict.Columns[0].Name != "email" {
			t.Errorf("Expected email column, got %v", ast.OnConflict.Columns)
		}
	})

	t.Run("INSERT with ON CONFLICT DO UPDATE", func(t *testing.T) {
		builder := Insert(types.Table{Name: "User"}).
			Values(map[types.Field]types.Param{
				{Name: "name"}:             {Name: "userName"},
				types.Field{Name: "email"}: types.Param{Name: "userEmail"},
			}).
			OnConflict(types.Field{Name: "email"}).
			DoUpdate().
			Set(types.Field{Name: "name"}, types.Param{Name: "newName"}).
			Build()

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.OnConflict.Action != DoUpdate {
			t.Errorf("Expected DoUpdate action, got %s", ast.OnConflict.Action)
		}
		if len(ast.OnConflict.Updates) != 1 {
			t.Errorf("Expected 1 update, got %d", len(ast.OnConflict.Updates))
		}
	})
}

func TestUpdateBuilder(t *testing.T) {
	builder := Update(types.Table{Name: "users"})

	if builder.pgAst.Operation != types.OpUpdate {
		t.Errorf("Expected OpUpdate, got %s", builder.pgAst.Operation)
	}
}

func TestDeleteBuilder(t *testing.T) {
	builder := Delete(types.Table{Name: "users"})

	if builder.pgAst.Operation != types.OpDelete {
		t.Errorf("Expected OpDelete, got %s", builder.pgAst.Operation)
	}
}

func TestCountBuilder(t *testing.T) {
	builder := Count(types.Table{Name: "users"})

	if builder.pgAst.Operation != types.OpCount {
		t.Errorf("Expected OpCount, got %s", builder.pgAst.Operation)
	}
}

func TestJoinBuilders(t *testing.T) {
	t.Run("InnerJoin", func(t *testing.T) {
		builder := Select(types.Table{Name: "users", Alias: "u"}).
			InnerJoin(types.Table{Name: "posts", Alias: "p"}, astql.C(types.Field{Name: "user_id"}, types.EQ, types.Param{Name: "userId"}))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.Joins) != 1 {
			t.Errorf("Expected 1 join, got %d", len(ast.Joins))
		}
		if ast.Joins[0].Type != InnerJoin {
			t.Errorf("Expected InnerJoin, got %s", ast.Joins[0].Type)
		}
		if ast.Joins[0].Table.Name != "posts" {
			t.Errorf("Expected posts table, got %s", ast.Joins[0].Table.Name)
		}
	})

	t.Run("LeftJoin", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"}).
			LeftJoin(types.Table{Name: "profiles"}, astql.C(types.Field{Name: "user_id"}, types.EQ, types.Param{Name: "userId"}))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.Joins[0].Type != LeftJoin {
			t.Errorf("Expected LeftJoin, got %s", ast.Joins[0].Type)
		}
	})

	t.Run("RightJoin", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"}).
			RightJoin(types.Table{Name: "departments"}, astql.C(types.Field{Name: "dept_id"}, types.EQ, types.Param{Name: "deptId"}))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.Joins[0].Type != RightJoin {
			t.Errorf("Expected RightJoin, got %s", ast.Joins[0].Type)
		}
	})
}

func TestGroupByHaving(t *testing.T) {
	t.Run("GroupBy", func(t *testing.T) {
		builder := Select(types.Table{Name: "orders"}).
			GroupBy(types.Field{Name: "user_id"}, types.Field{Name: "status"})

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.GroupBy) != 2 {
			t.Errorf("Expected 2 group by fields, got %d", len(ast.GroupBy))
		}
		if ast.GroupBy[0].Name != "user_id" {
			t.Errorf("Expected user_id field, got %s", ast.GroupBy[0].Name)
		}
	})

	t.Run("Having", func(t *testing.T) {
		builder := Select(types.Table{Name: "orders"}).
			GroupBy(types.Field{Name: "user_id"}).
			Having(astql.C(types.Field{Name: "total"}, types.GT, types.Param{Name: "minTotal"}))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.Having) != 1 {
			t.Errorf("Expected 1 having condition, got %d", len(ast.Having))
		}
	})
}

func TestReturning(t *testing.T) {
	t.Run("INSERT with RETURNING", func(t *testing.T) {
		builder := Insert(types.Table{Name: "users"}).
			Values(map[types.Field]types.Param{
				types.Field{Name: "name"}: types.Param{Name: "userName"},
			}).
			Returning(types.Field{Name: "id"}, types.Field{Name: "created_at"})

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.Returning) != 2 {
			t.Errorf("Expected 2 returning fields, got %d", len(ast.Returning))
		}
		if ast.Returning[0].Name != "id" {
			t.Errorf("Expected id field, got %s", ast.Returning[0].Name)
		}
	})
}

func TestAggregateBuilders(t *testing.T) {
	testCases := []struct {
		name      string
		expr      FieldExpression
		aggregate AggregateFunc
	}{
		{"Sum", Sum(types.Field{Name: "amount"}), AggSum},
		{"Avg", Avg(types.Field{Name: "amount"}), AggAvg},
		{"Min", Min(types.Field{Name: "amount"}), AggMin},
		{"Max", Max(types.Field{Name: "amount"}), AggMax},
		{"CountField", CountField(types.Field{Name: "id"}), AggCountField},
		{"CountDistinct", CountDistinct(types.Field{Name: "user_id"}), AggCountDistinct},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := Select(types.Table{Name: "orders"}).SelectExpr(tc.expr)

			ast, err := builder.Build()
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			if len(ast.FieldExpressions) != 1 {
				t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
			}
			if ast.FieldExpressions[0].Aggregate != tc.aggregate {
				t.Errorf("Expected %s aggregate, got %s", tc.aggregate, ast.FieldExpressions[0].Aggregate)
			}
		})
	}
}

func TestSelectExpressionBuilders(t *testing.T) {
	t.Run("SelectExpr", func(t *testing.T) {
		expr := FieldExpression{
			Field: types.Field{Name: "amount"},
			Alias: "total_amount",
		}

		builder := Select(types.Table{Name: "orders"}).SelectExpr(expr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}
		if ast.FieldExpressions[0].Alias != "total_amount" {
			t.Errorf("Expected alias 'total_amount', got %s", ast.FieldExpressions[0].Alias)
		}
	})

	t.Run("SelectCoalesce", func(t *testing.T) {
		coalesceExpr := Coalesce(types.Param{Name: "firstName"}, types.Param{Name: "lastName"})
		coalesceExpr.Alias = "full_name"
		builder := Select(types.Table{Name: "users"}).
			SelectCoalesce(coalesceExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}
		if ast.FieldExpressions[0].Coalesce == nil {
			t.Error("Expected Coalesce expression")
		}
		if ast.FieldExpressions[0].Alias != "full_name" {
			t.Errorf("Expected alias 'full_name', got %s", ast.FieldExpressions[0].Alias)
		}
	})

	t.Run("SelectNullIf", func(t *testing.T) {
		nullifExpr := NullIf(types.Param{Name: "status"}, types.Param{Name: "default"})
		nullifExpr.Alias = "real_status"
		builder := Select(types.Table{Name: "users"}).
			SelectNullIf(nullifExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.FieldExpressions[0].NullIf == nil {
			t.Error("Expected NullIf expression")
		}
	})

	t.Run("SelectCase", func(t *testing.T) {
		caseExpr := CaseExpression{
			WhenClauses: []WhenClause{
				{
					Condition: astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "minAge"}),
					Result:    types.Param{Name: "young"},
				},
			},
			ElseValue: func() *types.Param { p := types.Param{Name: "old"}; return &p }(),
		}

		caseExpr.Alias = "age_group"
		builder := Select(types.Table{Name: "users"}).SelectCase(caseExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.FieldExpressions[0].Case == nil {
			t.Error("Expected Case expression")
		}
	})

	t.Run("SelectMath", func(t *testing.T) {
		mathExpr := MathExpression{
			Function: MathRound,
			Field:    types.Field{Name: "price"},
		}

		mathExpr.Alias = "rounded_price"
		builder := Select(types.Table{Name: "products"}).SelectMath(mathExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.FieldExpressions[0].Math == nil {
			t.Error("Expected Math expression")
		}
	})
}

func TestBuilderDelegation(t *testing.T) {
	t.Run("Fields delegation", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"}).Fields(types.Field{Name: "id"}, types.Field{Name: "name"})

		// Test that base builder methods still work
		fields := builder.Fields(types.Field{Name: "email"})
		if fields != builder {
			t.Error("Expected fluent interface to return same builder")
		}
	})

	t.Run("Where delegation", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"}).
			Where(astql.C(types.Field{Name: "active"}, types.EQ, types.Param{Name: "isActive"}))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if ast.WhereClause == nil {
			t.Error("Expected WhereClause to be set")
		}
	})
}

func TestBuildAndMustBuild(t *testing.T) {
	t.Run("Build success", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"})

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		if ast == nil {
			t.Error("Expected AST to be returned")
		}
	})

	t.Run("MustBuild success", func(t *testing.T) {
		builder := Select(types.Table{Name: "users"})

		// Should not panic
		ast := builder.MustBuild()
		if ast == nil {
			t.Error("Expected AST to be returned")
		}
	})

	t.Run("MustBuild panic on invalid AST", func(t *testing.T) {
		// Create an invalid AST that will fail validation
		builder := Insert(types.Table{Name: "User"}).
			OnConflict(types.Field{Name: "email"}).
			DoUpdate().
			Build()

		// Remove the columns to make it invalid
		builder.pgAst.OnConflict.Columns = []types.Field{}

		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected MustBuild to panic on invalid AST")
			}
		}()

		builder.MustBuild()
	})
}

func TestAs(t *testing.T) {
	// Test the As method for adding aliases to field expressions
	sumExpr := Sum(types.Field{Name: "amount"})
	sumExpr.Alias = "total"
	builder := Select(types.Table{Name: "orders"}).SelectExpr(sumExpr)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(ast.FieldExpressions) != 1 {
		t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
	}
	if ast.FieldExpressions[0].Alias != "total" {
		t.Errorf("Expected alias 'total', got %s", ast.FieldExpressions[0].Alias)
	}
}
