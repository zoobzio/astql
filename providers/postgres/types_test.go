package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

func TestFieldComparison(t *testing.T) {
	t.Run("Basic field comparison", func(t *testing.T) {
		leftField := types.Field{Name: "users.id"}
		rightField := types.Field{Name: "posts.user_id"}
		operator := types.EQ

		comparison := CF(leftField, operator, rightField)

		if comparison.LeftField != leftField {
			t.Error("Expected LeftField to match")
		}
		if comparison.RightField != rightField {
			t.Error("Expected RightField to match")
		}
		if comparison.Operator != operator {
			t.Error("Expected Operator to match")
		}
	})

	t.Run("FieldComparison implements ConditionItem", func(_ *testing.T) {
		comparison := CF(types.Field{Name: "a"}, types.GT, types.Field{Name: "b"})

		// This should compile without error, proving it implements the interface
		var _ types.ConditionItem = comparison
	})
}

func TestSubqueryCondition(t *testing.T) {
	t.Run("CSub for IN/NOT IN operations", func(t *testing.T) {
		field := types.Field{Name: "user_id"}
		subquery := Subquery{AST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "active_users"},
			Fields:    []types.Field{{Name: "id"}},
		}}

		t.Run("IN subquery", func(t *testing.T) {
			condition := CSub(field, types.IN, subquery)

			if condition.Field == nil {
				t.Error("Expected Field to be set")
			}
			if *condition.Field != field {
				t.Error("Expected Field to match")
			}
			if condition.Operator != types.IN {
				t.Error("Expected Operator to be IN")
			}
			if condition.Subquery.AST != subquery.AST {
				t.Error("Expected Subquery to match")
			}
		})

		t.Run("NOT IN subquery", func(t *testing.T) {
			condition := CSub(field, types.NotIn, subquery)

			if condition.Operator != types.NotIn {
				t.Error("Expected Operator to be NOT IN")
			}
		})

		t.Run("CSub panic with invalid operator", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected panic for invalid operator")
				} else if !strings.Contains(fmt.Sprintf("%v", r), "cannot be used with CSub") {
					t.Errorf("Expected specific panic message, got: %v", r)
				}
			}()

			// This should panic - EXISTS cannot be used with CSub
			CSub(field, types.EXISTS, subquery)
		})
	})
}

func TestSubqueryConditionExists(t *testing.T) {
	subquery := Subquery{AST: &types.QueryAST{
		Operation:   types.OpSelect,
		Target:      types.Table{Name: "posts"},
		WhereClause: astql.C(types.Field{Name: "published"}, types.EQ, types.Param{Name: "true"}),
	}}

	t.Run("CSubExists for EXISTS", func(t *testing.T) {
		condition := CSubExists(types.EXISTS, subquery)

		if condition.Field != nil {
			t.Error("Expected Field to be nil for EXISTS")
		}
		if condition.Operator != types.EXISTS {
			t.Error("Expected Operator to be EXISTS")
		}
		if condition.Subquery.AST != subquery.AST {
			t.Error("Expected Subquery to match")
		}
	})

	t.Run("CSubExists for NOT EXISTS", func(t *testing.T) {
		condition := CSubExists(types.NotExists, subquery)

		if condition.Operator != types.NotExists {
			t.Error("Expected Operator to be NOT EXISTS")
		}
	})

	t.Run("CSubExists panic with invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid operator")
			} else if !strings.Contains(fmt.Sprintf("%v", r), "CSubExists only accepts EXISTS or NOT EXISTS") {
				t.Errorf("Expected specific panic message, got: %v", r)
			}
		}()

		// This should panic - IN cannot be used with CSubExists
		CSubExists(types.IN, subquery)
	})
}

func TestSubqueryConditionInterface(t *testing.T) {
	t.Run("SubqueryCondition implements ConditionItem", func(_ *testing.T) {
		subquery := Subquery{AST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "test"},
		}}

		condition := CSubExists(types.EXISTS, subquery)

		// This should compile without error, proving it implements the interface
		var _ types.ConditionItem = condition
	})
}

func TestSubqueryBuilders(t *testing.T) {
	t.Run("Sub from base builder", func(t *testing.T) {
		// This would normally use a real builder, but we'll test the panic case
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when Sub fails to build")
			}
			// Any panic is acceptable since we're passing nil
		}()

		// Create a builder that will fail validation (nil builder)
		Sub(nil)
	})

	t.Run("SubPostgres from postgres builder", func(t *testing.T) {
		// Test with a valid postgres builder
		builder := Select(types.Table{Name: "users"}).Fields(types.Field{Name: "id"})

		subquery := SubPostgres(builder)

		if subquery.AST == nil {
			t.Error("Expected AST to be set")
		}

		// Verify it's a postgres AST
		pgAST, ok := subquery.AST.(*AST)
		if !ok {
			t.Error("Expected PostgreSQL AST type")
		}
		if pgAST.Operation != types.OpSelect {
			t.Error("Expected SELECT operation")
		}
	})
}

func TestRenderContext(t *testing.T) {
	t.Run("newRenderContext", func(t *testing.T) {
		paramCallback := func(p types.Param) string { return ":" + p.Name }

		ctx := newRenderContext(paramCallback)

		if ctx.depth != 0 {
			t.Errorf("Expected depth 0, got %d", ctx.depth)
		}
		if ctx.paramPrefix != "" {
			t.Errorf("Expected empty paramPrefix, got '%s'", ctx.paramPrefix)
		}
		if ctx.usedParams == nil {
			t.Error("Expected usedParams to be initialized")
		}
		if ctx.paramCallback == nil {
			t.Error("Expected paramCallback to be set")
		}
	})

	t.Run("withSubquery creates child context", func(t *testing.T) {
		paramCallback := func(p types.Param) string { return ":" + p.Name }
		parentCtx := newRenderContext(paramCallback)

		childCtx, err := parentCtx.withSubquery()
		if err != nil {
			t.Fatalf("withSubquery failed: %v", err)
		}

		if childCtx.depth != 1 {
			t.Errorf("Expected child depth 1, got %d", childCtx.depth)
		}
		if childCtx.paramPrefix != "sq1_" {
			t.Errorf("Expected paramPrefix 'sq1_', got '%s'", childCtx.paramPrefix)
		}
		// Check that they share the same map reference (can't compare map equality directly)
		parentCtx.usedParams["test"] = true
		if _, exists := childCtx.usedParams["test"]; !exists {
			t.Error("Expected child to share usedParams map with parent")
		}
	})

	t.Run("withSubquery depth limiting", func(t *testing.T) {
		paramCallback := func(p types.Param) string { return ":" + p.Name }
		ctx := newRenderContext(paramCallback)

		// Create nested contexts up to the limit
		currentCtx := ctx
		var err error

		for i := 0; i < MaxSubqueryDepth; i++ {
			currentCtx, err = currentCtx.withSubquery()
			if err != nil {
				t.Fatalf("Unexpected error at depth %d: %v", i+1, err)
			}
		}

		// This should fail - exceeds max depth
		_, err = currentCtx.withSubquery()
		if err == nil {
			t.Error("Expected error when exceeding max subquery depth")
		}
		if !strings.Contains(err.Error(), "maximum subquery depth") {
			t.Errorf("Expected max depth error, got: %v", err)
		}
	})

	t.Run("addParam with prefix", func(t *testing.T) {
		var capturedParam types.Param
		paramCallback := func(p types.Param) string {
			capturedParam = p
			return ":" + p.Name
		}

		ctx := newRenderContext(paramCallback)
		childCtx, _ := ctx.withSubquery()

		originalParam := types.Param{Name: "testParam"}
		result := childCtx.addParam(originalParam)

		if result != ":sq1_testParam" {
			t.Errorf("Expected ':sq1_testParam', got '%s'", result)
		}
		if capturedParam.Name != "sq1_testParam" {
			t.Errorf("Expected prefixed param name 'sq1_testParam', got '%s'", capturedParam.Name)
		}
	})

	t.Run("addParam without prefix", func(t *testing.T) {
		var capturedParam types.Param
		paramCallback := func(p types.Param) string {
			capturedParam = p
			return ":" + p.Name
		}

		ctx := newRenderContext(paramCallback)

		originalParam := types.Param{Name: "testParam"}
		result := ctx.addParam(originalParam)

		if result != ":testParam" {
			t.Errorf("Expected ':testParam', got '%s'", result)
		}
		if capturedParam.Name != "testParam" {
			t.Errorf("Expected original param name 'testParam', got '%s'", capturedParam.Name)
		}
	})
}

func TestMaxSubqueryDepthConstant(t *testing.T) {
	// Verify the constant is set to a reasonable value
	if MaxSubqueryDepth != 3 {
		t.Errorf("Expected MaxSubqueryDepth to be 3, got %d", MaxSubqueryDepth)
	}
}

func TestSubqueryStructure(t *testing.T) {
	t.Run("Subquery with QueryAST", func(t *testing.T) {
		ast := &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "test"},
		}

		subquery := Subquery{AST: ast}

		if subquery.AST != ast {
			t.Error("Expected AST to be set")
		}
	})

	t.Run("Subquery with PostgreSQL AST", func(t *testing.T) {
		pgAST := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "postgres_test"},
			},
			Distinct: true,
		}

		subquery := Subquery{AST: pgAST}

		if subquery.AST != pgAST {
			t.Error("Expected PostgreSQL AST to be set")
		}
	})
}

func TestConditionItemInterfaces(t *testing.T) {
	t.Run("Interface implementations compile", func(t *testing.T) {
		// Test that our types implement the required interfaces
		var conditions []types.ConditionItem

		// FieldComparison
		fieldComp := CF(types.Field{Name: "a"}, types.EQ, types.Field{Name: "b"})
		conditions = append(conditions, fieldComp)

		// SubqueryCondition
		subquery := Subquery{AST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "test"},
		}}
		subCond := CSubExists(types.EXISTS, subquery)
		conditions = append(conditions, subCond)

		// If this compiles, the interfaces are correctly implemented
		if len(conditions) != 2 {
			t.Error("Expected 2 condition items")
		}
	})
}
