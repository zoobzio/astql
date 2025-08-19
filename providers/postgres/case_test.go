package postgres

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

func TestCaseBuilder(t *testing.T) {
	t.Run("Basic CASE expression", func(t *testing.T) {
		caseBuilder := Case()

		if caseBuilder.expr == nil {
			t.Error("Expected expr to be initialized")
		}
		if len(caseBuilder.expr.WhenClauses) != 0 {
			t.Error("Expected empty WhenClauses initially")
		}
	})

	t.Run("CASE with single WHEN clause", func(t *testing.T) {
		condition := astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "minAge"})
		result := types.Param{Name: "young"}

		caseBuilder := Case().When(condition, result)

		if len(caseBuilder.expr.WhenClauses) != 1 {
			t.Errorf("Expected 1 WHEN clause, got %d", len(caseBuilder.expr.WhenClauses))
		}

		whenClause := caseBuilder.expr.WhenClauses[0]
		if whenClause.Condition != condition {
			t.Error("Expected condition to match")
		}
		if whenClause.Result != result {
			t.Error("Expected result to match")
		}
	})

	t.Run("CASE with multiple WHEN clauses", func(t *testing.T) {
		caseBuilder := Case().
			When(astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "young"}), types.Param{Name: "child"}).
			When(astql.C(types.Field{Name: "age"}, types.GE, types.Param{Name: "senior"}), types.Param{Name: "elderly"})

		if len(caseBuilder.expr.WhenClauses) != 2 {
			t.Errorf("Expected 2 WHEN clauses, got %d", len(caseBuilder.expr.WhenClauses))
		}
	})

	t.Run("CASE with ELSE clause", func(t *testing.T) {
		elseValue := types.Param{Name: "adult"}

		caseBuilder := Case().
			When(astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "young"}), types.Param{Name: "child"}).
			Else(elseValue)

		if caseBuilder.expr.ElseValue == nil {
			t.Error("Expected ElseValue to be set")
		}
		if *caseBuilder.expr.ElseValue != elseValue {
			t.Error("Expected ElseValue to match")
		}
	})

	t.Run("CASE Build method", func(t *testing.T) {
		caseExpr := Case().
			When(astql.C(types.Field{Name: "status"}, types.EQ, types.Param{Name: "active"}), types.Param{Name: "enabled"}).
			Else(types.Param{Name: "disabled"}).
			Build()

		if len(caseExpr.WhenClauses) != 1 {
			t.Errorf("Expected 1 WHEN clause, got %d", len(caseExpr.WhenClauses))
		}
		if caseExpr.ElseValue == nil {
			t.Error("Expected ElseValue to be set")
		}
	})

	t.Run("CASE As method", func(t *testing.T) {
		alias := "status_label"

		caseBuilder := Case().
			When(astql.C(types.Field{Name: "active"}, types.EQ, types.Param{Name: "true"}), types.Param{Name: "enabled"}).
			As(alias)

		caseExpr := caseBuilder.Build()

		if caseExpr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, caseExpr.Alias)
		}
	})
}

func TestCaseWithClauses(t *testing.T) {
	t.Run("Create with existing clauses", func(t *testing.T) {
		whenClauses := []WhenClause{
			{
				Condition: astql.C(types.Field{Name: "priority"}, types.EQ, types.Param{Name: "high"}),
				Result:    types.Param{Name: "urgent"},
			},
			{
				Condition: astql.C(types.Field{Name: "priority"}, types.EQ, types.Param{Name: "low"}),
				Result:    types.Param{Name: "normal"},
			},
		}

		caseExpr := CaseWithClauses(whenClauses...)

		if len(caseExpr.WhenClauses) != 2 {
			t.Errorf("Expected 2 WHEN clauses, got %d", len(caseExpr.WhenClauses))
		}

		// Verify clauses match
		for i, clause := range whenClauses {
			if caseExpr.WhenClauses[i].Condition != clause.Condition {
				t.Errorf("Expected condition %d to match", i)
			}
			if caseExpr.WhenClauses[i].Result != clause.Result {
				t.Errorf("Expected result %d to match", i)
			}
		}
	})

	t.Run("Add ELSE to existing clauses", func(t *testing.T) {
		whenClauses := []WhenClause{
			{
				Condition: astql.C(types.Field{Name: "type"}, types.EQ, types.Param{Name: "premium"}),
				Result:    types.Param{Name: "vip"},
			},
		}
		elseValue := types.Param{Name: "standard"}

		caseExpr := CaseWithClauses(whenClauses...).Else(elseValue)

		if caseExpr.ElseValue == nil {
			t.Error("Expected ElseValue to be set")
		}
		if *caseExpr.ElseValue != elseValue {
			t.Error("Expected ElseValue to match")
		}
	})
}

func TestCaseExpressionIntegration(t *testing.T) {
	t.Run("CASE in SELECT query", func(t *testing.T) {
		// This tests integration with the main query builder
		caseExpr := Case().
			When(astql.C(types.Field{Name: "age"}, types.LT, types.Param{Name: "eighteen"}), types.Param{Name: "minor"}).
			When(astql.C(types.Field{Name: "age"}, types.GE, types.Param{Name: "sixtyfive"}), types.Param{Name: "senior"}).
			Else(types.Param{Name: "adult"}).
			Build()

		caseExpr.Alias = "age_category"
		builder := Select(types.Table{Name: "users"}).
			SelectCase(caseExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}

		fieldExpr := ast.FieldExpressions[0]
		if fieldExpr.Case == nil {
			t.Error("Expected Case expression to be set")
		}
		if fieldExpr.Alias != "age_category" {
			t.Errorf("Expected alias 'age_category', got '%s'", fieldExpr.Alias)
		}
		if len(fieldExpr.Case.WhenClauses) != 2 {
			t.Errorf("Expected 2 WHEN clauses, got %d", len(fieldExpr.Case.WhenClauses))
		}
	})
}

func TestCaseFluentInterface(t *testing.T) {
	t.Run("Fluent interface returns builder", func(t *testing.T) {
		builder := Case()

		// Test that all methods return the same builder for chaining
		result1 := builder.When(astql.C(types.Field{Name: "test"}, types.EQ, types.Param{Name: "value"}), types.Param{Name: "result"})
		if result1 != builder {
			t.Error("Expected When to return the same builder")
		}

		result2 := builder.Else(types.Param{Name: "default"})
		if result2 != builder {
			t.Error("Expected Else to return the same builder")
		}
	})
}

func TestWhenClause(t *testing.T) {
	t.Run("WhenClause structure", func(t *testing.T) {
		condition := astql.C(types.Field{Name: "status"}, types.EQ, types.Param{Name: "active"})
		result := types.Param{Name: "enabled"}

		whenClause := WhenClause{
			Condition: condition,
			Result:    result,
		}

		if whenClause.Condition != condition {
			t.Error("Expected condition to be set correctly")
		}
		if whenClause.Result != result {
			t.Error("Expected result to be set correctly")
		}
	})
}

func TestCaseExpressionValidation(t *testing.T) {
	t.Run("Empty CASE expression", func(t *testing.T) {
		// Test that we can create an empty CASE (though it may not be valid SQL)
		caseExpr := Case().Build()

		if len(caseExpr.WhenClauses) != 0 {
			t.Error("Expected empty WhenClauses")
		}
		if caseExpr.ElseValue != nil {
			t.Error("Expected ElseValue to be nil")
		}
	})

	t.Run("CASE with only ELSE", func(t *testing.T) {
		// Edge case: CASE with no WHEN clauses but has ELSE
		caseExpr := Case().Else(types.Param{Name: "default"}).Build()

		if len(caseExpr.WhenClauses) != 0 {
			t.Error("Expected empty WhenClauses")
		}
		if caseExpr.ElseValue == nil {
			t.Error("Expected ElseValue to be set")
		}
	})
}
