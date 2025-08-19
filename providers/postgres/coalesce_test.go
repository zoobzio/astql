package postgres

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestCoalesce(t *testing.T) {
	t.Run("Basic COALESCE with minimum parameters", func(t *testing.T) {
		value1 := types.Param{Name: "firstName"}
		value2 := types.Param{Name: "lastName"}

		expr := Coalesce(value1, value2)

		if len(expr.Values) != 2 {
			t.Errorf("Expected 2 values, got %d", len(expr.Values))
		}
		if expr.Values[0] != value1 {
			t.Error("Expected first value to match")
		}
		if expr.Values[1] != value2 {
			t.Error("Expected second value to match")
		}
		if expr.Alias != "" {
			t.Errorf("Expected empty alias, got '%s'", expr.Alias)
		}
	})

	t.Run("COALESCE with multiple parameters", func(t *testing.T) {
		values := []types.Param{
			{Name: "preferredName"},
			types.Param{Name: "firstName"},
			types.Param{Name: "lastName"},
			types.Param{Name: "defaultName"},
		}

		expr := Coalesce(values...)

		if len(expr.Values) != 4 {
			t.Errorf("Expected 4 values, got %d", len(expr.Values))
		}
		for i, value := range values {
			if expr.Values[i] != value {
				t.Errorf("Expected value %d to match", i)
			}
		}
	})

	t.Run("COALESCE with alias", func(t *testing.T) {
		alias := "display_name"

		expr := Coalesce(types.Param{Name: "first"}, types.Param{Name: "second"}).As(alias)

		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
	})

	t.Run("COALESCE panic with insufficient parameters", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when creating COALESCE with < 2 values")
			} else if !strings.Contains(r.(string), "COALESCE requires at least 2 values") {
				t.Errorf("Expected specific panic message, got: %v", r)
			}
		}()

		// This should panic
		Coalesce(types.Param{Name: "single"})
	})

	t.Run("COALESCE panic with no parameters", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when creating COALESCE with no values")
			}
		}()

		// This should panic
		Coalesce()
	})
}

func TestNullIf(t *testing.T) {
	t.Run("Basic NULLIF", func(t *testing.T) {
		value1 := types.Param{Name: "status"}
		value2 := types.Param{Name: "defaultStatus"}

		expr := NullIf(value1, value2)

		if expr.Value1 != value1 {
			t.Error("Expected Value1 to match")
		}
		if expr.Value2 != value2 {
			t.Error("Expected Value2 to match")
		}
		if expr.Alias != "" {
			t.Errorf("Expected empty alias, got '%s'", expr.Alias)
		}
	})

	t.Run("NULLIF with alias", func(t *testing.T) {
		alias := "clean_status"
		value1 := types.Param{Name: "rawStatus"}
		value2 := types.Param{Name: "emptyValue"}

		expr := NullIf(value1, value2).As(alias)

		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
		if expr.Value1 != value1 {
			t.Error("Expected Value1 to be preserved")
		}
		if expr.Value2 != value2 {
			t.Error("Expected Value2 to be preserved")
		}
	})
}

func TestCoalesceExpressionIntegration(t *testing.T) {
	t.Run("COALESCE in SELECT query", func(t *testing.T) {
		coalesceExpr := Coalesce(
			types.Param{Name: "nickname"},
			types.Param{Name: "firstName"},
			types.Param{Name: "defaultName"},
		).As("display_name")
		builder := Select(types.Table{Name: "users"}).
			SelectCoalesce(coalesceExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}

		fieldExpr := ast.FieldExpressions[0]
		if fieldExpr.Coalesce == nil {
			t.Error("Expected Coalesce expression to be set")
		}
		if fieldExpr.Alias != "display_name" {
			t.Errorf("Expected alias 'display_name', got '%s'", fieldExpr.Alias)
		}
		if len(fieldExpr.Coalesce.Values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(fieldExpr.Coalesce.Values))
		}
	})
}

func TestNullIfExpressionIntegration(t *testing.T) {
	t.Run("NULLIF in SELECT query", func(t *testing.T) {
		nullifExpr := NullIf(types.Param{Name: "status"}, types.Param{Name: "inactive"}).As("active_status")
		builder := Select(types.Table{Name: "users"}).
			SelectNullIf(nullifExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}

		fieldExpr := ast.FieldExpressions[0]
		if fieldExpr.NullIf == nil {
			t.Error("Expected NullIf expression to be set")
		}
		if fieldExpr.Alias != "active_status" {
			t.Errorf("Expected alias 'active_status', got '%s'", fieldExpr.Alias)
		}
	})
}

func TestCoalesceNullIfStructures(t *testing.T) {
	t.Run("CoalesceExpression structure", func(t *testing.T) {
		values := []types.Param{
			types.Param{Name: "val1"},
			types.Param{Name: "val2"},
		}
		alias := "test_alias"

		expr := CoalesceExpression{
			Values: values,
			Alias:  alias,
		}

		if len(expr.Values) != 2 {
			t.Errorf("Expected 2 values, got %d", len(expr.Values))
		}
		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
	})

	t.Run("NullIfExpression structure", func(t *testing.T) {
		value1 := types.Param{Name: "test1"}
		value2 := types.Param{Name: "test2"}
		alias := "null_test"

		expr := NullIfExpression{
			Value1: value1,
			Value2: value2,
			Alias:  alias,
		}

		if expr.Value1 != value1 {
			t.Error("Expected Value1 to match")
		}
		if expr.Value2 != value2 {
			t.Error("Expected Value2 to match")
		}
		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
	})
}

func TestFluentInterface(t *testing.T) {
	t.Run("COALESCE As returns CoalesceExpression", func(t *testing.T) {
		expr1 := Coalesce(types.Param{Name: "a"}, types.Param{Name: "b"})
		expr2 := expr1.As("test")

		// Should return the same expression with alias set
		if expr2.Alias != "test" {
			t.Error("Expected alias to be set")
		}
		if len(expr2.Values) != 2 {
			t.Error("Expected values to be preserved")
		}
	})

	t.Run("NULLIF As returns NullIfExpression", func(t *testing.T) {
		expr1 := NullIf(types.Param{Name: "x"}, types.Param{Name: "y"})
		expr2 := expr1.As("null_test")

		// Should return the same expression with alias set
		if expr2.Alias != "null_test" {
			t.Error("Expected alias to be set")
		}
		if expr2.Value1.Name != "x" {
			t.Error("Expected Value1 to be preserved")
		}
		if expr2.Value2.Name != "y" {
			t.Error("Expected Value2 to be preserved")
		}
	})
}

func TestCoalesceNullIfEdgeCases(t *testing.T) {
	t.Run("COALESCE with identical values", func(t *testing.T) {
		sameParam := types.Param{Name: "duplicate"}

		expr := Coalesce(sameParam, sameParam, sameParam)

		if len(expr.Values) != 3 {
			t.Errorf("Expected 3 values even if identical, got %d", len(expr.Values))
		}

		// All values should reference the same parameter
		for i, val := range expr.Values {
			if val != sameParam {
				t.Errorf("Expected value %d to be the same parameter", i)
			}
		}
	})

	t.Run("NULLIF with identical values", func(t *testing.T) {
		sameParam := types.Param{Name: "same"}

		expr := NullIf(sameParam, sameParam)

		if expr.Value1 != sameParam {
			t.Error("Expected Value1 to be the same parameter")
		}
		if expr.Value2 != sameParam {
			t.Error("Expected Value2 to be the same parameter")
		}
	})

	t.Run("COALESCE chaining As multiple times", func(t *testing.T) {
		expr := Coalesce(types.Param{Name: "a"}, types.Param{Name: "b"}).
			As("first").
			As("second")

		// Should overwrite the alias
		if expr.Alias != "second" {
			t.Errorf("Expected final alias 'second', got '%s'", expr.Alias)
		}
	})
}
