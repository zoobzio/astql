package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

func TestFieldComparison(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("CF creates field comparison", func(t *testing.T) {
		fc := astql.CF(astql.F("age"), astql.GT, astql.F("min_age"))

		if fc.LeftField.Name != "age" {
			t.Errorf("Expected left field 'age', got '%s'", fc.LeftField.Name)
		}
		if fc.Operator != astql.GT {
			t.Errorf("Expected operator GT, got '%s'", fc.Operator)
		}
		if fc.RightField.Name != "min_age" {
			t.Errorf("Expected right field 'min_age', got '%s'", fc.RightField.Name)
		}
	})

	t.Run("Field comparison implements ConditionItem", func(t *testing.T) {
		fc := astql.CF(astql.F("age"), astql.EQ, astql.F("user_age"))

		// This should compile if FieldComparison implements ConditionItem
		var _ astql.ConditionItem = fc

		// Test it can be used in Where clause
		builder := astql.Select(astql.T("test_users")).
			Where(fc)

		if builder.GetError() != nil {
			t.Errorf("Expected no error, got: %v", builder.GetError())
		}
	})
}

func TestSubqueryConditions(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("CSub creates subquery condition with field", func(t *testing.T) {
		subBuilder := astql.Select(astql.T("test_orders")).
			Fields(astql.F("customer_id"))

		sub := astql.Sub(subBuilder)
		cond := astql.CSub(astql.F("id"), astql.IN, sub)

		if cond.Field == nil || cond.Field.Name != "id" {
			t.Error("Expected field 'id' in condition")
		}
		if cond.Operator != astql.IN {
			t.Errorf("Expected operator IN, got '%s'", cond.Operator)
		}
		if cond.Subquery.AST == nil {
			t.Error("Expected subquery AST to be set")
		}
	})

	t.Run("CSub panics with invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid operator")
			} else if err, ok := r.(error); ok {
				if err.Error() == "" {
					t.Error("Expected error message in panic")
				}
			}
		}()

		subBuilder := astql.Select(astql.T("test_orders"))
		sub := astql.Sub(subBuilder)

		// This should panic - EXISTS requires CSubExists
		_ = astql.CSub(astql.F("id"), astql.EXISTS, sub)
	})

	t.Run("CSub works with NotIn operator", func(t *testing.T) {
		subBuilder := astql.Select(astql.T("test_orders")).
			Fields(astql.F("customer_id"))

		sub := astql.Sub(subBuilder)
		cond := astql.CSub(astql.F("id"), astql.NotIn, sub)

		if cond.Operator != astql.NotIn {
			t.Errorf("Expected operator NotIn, got '%s'", cond.Operator)
		}
	})

	t.Run("CSubExists creates EXISTS subquery condition", func(t *testing.T) {
		subBuilder := astql.Select(astql.T("test_orders")).
			Where(astql.C(astql.F("customer_id"), astql.EQ, astql.P("cid")))

		sub := astql.Sub(subBuilder)
		cond := astql.CSubExists(astql.EXISTS, sub)

		if cond.Field != nil {
			t.Error("Expected no field for EXISTS condition")
		}
		if cond.Operator != astql.EXISTS {
			t.Errorf("Expected operator EXISTS, got '%s'", cond.Operator)
		}
	})

	t.Run("CSubExists works with NotExists", func(t *testing.T) {
		subBuilder := astql.Select(astql.T("test_orders"))
		sub := astql.Sub(subBuilder)
		cond := astql.CSubExists(astql.NotExists, sub)

		if cond.Operator != astql.NotExists {
			t.Errorf("Expected operator NotExists, got '%s'", cond.Operator)
		}
	})

	t.Run("CSubExists panics with invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid operator")
			}
		}()

		subBuilder := astql.Select(astql.T("test_orders"))
		sub := astql.Sub(subBuilder)

		// This should panic - IN requires CSub
		_ = astql.CSubExists(astql.IN, sub)
	})

	t.Run("Subquery conditions implement ConditionItem", func(_ *testing.T) {
		subBuilder := astql.Select(astql.T("test_orders"))
		sub := astql.Sub(subBuilder)

		cond1 := astql.CSub(astql.F("id"), astql.IN, sub)
		cond2 := astql.CSubExists(astql.EXISTS, sub)

		// These should compile if SubqueryCondition implements ConditionItem
		var _ astql.ConditionItem = cond1
		var _ astql.ConditionItem = cond2
	})
}

func TestSub(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Sub creates subquery from builder", func(t *testing.T) {
		builder := astql.Select(astql.T("test_users")).
			Fields(astql.F("id")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("status")))

		subquery := astql.Sub(builder)

		if subquery.AST == nil {
			t.Error("Expected subquery AST to be set")
		}

		// Verify the AST is the builder's AST
		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		if subquery.AST != ast {
			t.Error("Subquery AST should match builder's AST")
		}
	})

	t.Run("Sub works with complex queries", func(t *testing.T) {
		// Create a complex subquery
		builder := astql.Select(astql.T("test_orders")).
			Fields(astql.F("customer_id")).
			Where(astql.And(
				astql.C(astql.F("product_id"), astql.EQ, astql.P("pid")),
				astql.C(astql.F("id"), astql.GT, astql.P("min_id")),
			)).
			Limit(10)

		subquery := astql.Sub(builder)

		// Use it in a condition
		cond := astql.CSub(astql.F("id"), astql.IN, subquery)

		// Use in main query
		mainQuery := astql.Select(astql.T("test_customers")).
			Where(cond)

		if mainQuery.GetError() != nil {
			t.Errorf("Expected no error, got: %v", mainQuery.GetError())
		}
	})
}

func TestConditionItemInterface(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("All condition types implement ConditionItem", func(t *testing.T) {
		// Simple condition
		c1 := astql.C(astql.F("name"), astql.EQ, astql.P("name"))
		var _ astql.ConditionItem = c1

		// Condition group
		c2 := astql.And(c1, astql.C(astql.F("age"), astql.GT, astql.P("min_age")))
		var _ astql.ConditionItem = c2

		// Field comparison
		c3 := astql.CF(astql.F("age"), astql.LT, astql.F("max_age"))
		var _ astql.ConditionItem = c3

		// Subquery condition
		subBuilder := astql.Select(astql.T("test_orders"))
		sub := astql.Sub(subBuilder)
		c4 := astql.CSub(astql.F("id"), astql.IN, sub)
		var _ astql.ConditionItem = c4

		// All should work in Where clause
		builders := []*astql.Builder{
			astql.Select(astql.T("test_users")).Where(c1),
			astql.Select(astql.T("test_users")).Where(c2),
			astql.Select(astql.T("test_users")).Where(c3),
			astql.Select(astql.T("test_users")).Where(c4),
		}

		for i, b := range builders {
			if b.GetError() != nil {
				t.Errorf("Builder %d failed: %v", i, b.GetError())
			}
		}
	})

	t.Run("Nested condition groups work", func(t *testing.T) {
		// Create deeply nested conditions
		innerOr := astql.Or(
			astql.C(astql.F("age"), astql.GT, astql.P("min_age")),
			astql.C(astql.F("age"), astql.LT, astql.P("max_age")),
		)

		outerAnd := astql.And(
			astql.C(astql.F("active"), astql.EQ, astql.P("status")),
			innerOr,
			astql.CF(astql.F("created_at"), astql.LT, astql.F("updated_at")),
		)

		builder := astql.Select(astql.T("test_users")).Where(outerAnd)

		if builder.GetError() != nil {
			t.Errorf("Expected no error, got: %v", builder.GetError())
		}
	})
}
