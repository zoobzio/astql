package postgres

import (
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestRound(t *testing.T) {
	t.Run("ROUND without precision", func(t *testing.T) {
		field := types.Field{Name: "price"}

		expr := Round(field)

		if expr.Function != MathRound {
			t.Errorf("Expected MathRound function, got %s", expr.Function)
		}
		if expr.Field != field {
			t.Error("Expected field to match")
		}
		if expr.Precision != nil {
			t.Error("Expected Precision to be nil")
		}
		if expr.Alias != "" {
			t.Errorf("Expected empty alias, got '%s'", expr.Alias)
		}
	})

	t.Run("ROUND with precision", func(t *testing.T) {
		field := types.Field{Name: "amount"}
		precision := types.Param{Name: "decimalPlaces"}

		expr := Round(field, precision)

		if expr.Function != MathRound {
			t.Errorf("Expected MathRound function, got %s", expr.Function)
		}
		if expr.Field != field {
			t.Error("Expected field to match")
		}
		if expr.Precision == nil {
			t.Error("Expected Precision to be set")
		}
		if *expr.Precision != precision {
			t.Error("Expected precision to match")
		}
	})

	t.Run("ROUND with multiple precision parameters", func(t *testing.T) {
		field := types.Field{Name: "value"}
		precision1 := types.Param{Name: "places1"}
		precision2 := types.Param{Name: "places2"}

		// Should only use the first precision parameter
		expr := Round(field, precision1, precision2)

		if expr.Precision == nil {
			t.Error("Expected Precision to be set")
		}
		if *expr.Precision != precision1 {
			t.Error("Expected first precision parameter to be used")
		}
	})
}

func TestFloor(t *testing.T) {
	field := types.Field{Name: "measurement"}

	expr := Floor(field)

	if expr.Function != MathFloor {
		t.Errorf("Expected MathFloor function, got %s", expr.Function)
	}
	if expr.Field != field {
		t.Error("Expected field to match")
	}
	if expr.Precision != nil {
		t.Error("Expected Precision to be nil for FLOOR")
	}
	if expr.Exponent != nil {
		t.Error("Expected Exponent to be nil for FLOOR")
	}
}

func TestCeil(t *testing.T) {
	field := types.Field{Name: "weight"}

	expr := Ceil(field)

	if expr.Function != MathCeil {
		t.Errorf("Expected MathCeil function, got %s", expr.Function)
	}
	if expr.Field != field {
		t.Error("Expected field to match")
	}
	if expr.Precision != nil {
		t.Error("Expected Precision to be nil for CEIL")
	}
	if expr.Exponent != nil {
		t.Error("Expected Exponent to be nil for CEIL")
	}
}

func TestAbs(t *testing.T) {
	field := types.Field{Name: "balance"}

	expr := Abs(field)

	if expr.Function != MathAbs {
		t.Errorf("Expected MathAbs function, got %s", expr.Function)
	}
	if expr.Field != field {
		t.Error("Expected field to match")
	}
}

func TestPower(t *testing.T) {
	t.Run("POWER with exponent", func(t *testing.T) {
		field := types.Field{Name: "base"}
		exponent := types.Param{Name: "exp"}

		expr := Power(field, exponent)

		if expr.Function != MathPower {
			t.Errorf("Expected MathPower function, got %s", expr.Function)
		}
		if expr.Field != field {
			t.Error("Expected field to match")
		}
		if expr.Exponent == nil {
			t.Error("Expected Exponent to be set")
		}
		if *expr.Exponent != exponent {
			t.Error("Expected exponent to match")
		}
	})
}

func TestSqrt(t *testing.T) {
	field := types.Field{Name: "area"}

	expr := Sqrt(field)

	if expr.Function != MathSqrt {
		t.Errorf("Expected MathSqrt function, got %s", expr.Function)
	}
	if expr.Field != field {
		t.Error("Expected field to match")
	}
	if expr.Precision != nil {
		t.Error("Expected Precision to be nil for SQRT")
	}
	if expr.Exponent != nil {
		t.Error("Expected Exponent to be nil for SQRT")
	}
}

func TestMathExpressionAs(t *testing.T) {
	t.Run("ROUND with alias", func(t *testing.T) {
		alias := "rounded_price"

		expr := Round(types.Field{Name: "price"}).As(alias)

		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
		// Verify other fields are preserved
		if expr.Function != MathRound {
			t.Error("Expected function to be preserved")
		}
	})

	t.Run("POWER with alias", func(t *testing.T) {
		alias := "squared"
		exponent := types.Param{Name: "two"}

		expr := Power(types.Field{Name: "value"}, exponent).As(alias)

		if expr.Alias != alias {
			t.Errorf("Expected alias '%s', got '%s'", alias, expr.Alias)
		}
		if expr.Exponent == nil || *expr.Exponent != exponent {
			t.Error("Expected exponent to be preserved")
		}
	})
}

func TestMathExpressionIntegration(t *testing.T) {
	t.Run("Math expression in SELECT query", func(t *testing.T) {
		mathExpr := Round(types.Field{Name: "price"}, types.Param{Name: "precision"})

		mathExpr.Alias = "rounded_price"
		builder := Select(types.Table{Name: "products"}).
			SelectMath(mathExpr)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
		}

		fieldExpr := ast.FieldExpressions[0]
		if fieldExpr.Math == nil {
			t.Error("Expected Math expression to be set")
		}
		if fieldExpr.Alias != "rounded_price" {
			t.Errorf("Expected alias 'rounded_price', got '%s'", fieldExpr.Alias)
		}
		if fieldExpr.Math.Function != MathRound {
			t.Error("Expected ROUND function")
		}
	})

	t.Run("Multiple math expressions", func(t *testing.T) {
		builder := Select(types.Table{Name: "measurements"}).
			SelectMath(Round(types.Field{Name: "value1"}).As("rounded")).
			SelectMath(Floor(types.Field{Name: "value2"}).As("floored")).
			SelectMath(Ceil(types.Field{Name: "value3"}).As("ceiled"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		if len(ast.FieldExpressions) != 3 {
			t.Errorf("Expected 3 field expressions, got %d", len(ast.FieldExpressions))
		}

		// Verify each expression
		expectedFuncs := []MathFunc{MathRound, MathFloor, MathCeil}
		expectedAliases := []string{"rounded", "floored", "ceiled"}

		for i, expr := range ast.FieldExpressions {
			if expr.Math == nil {
				t.Errorf("Expected Math expression %d to be set", i)
				continue
			}
			if expr.Math.Function != expectedFuncs[i] {
				t.Errorf("Expected function %s, got %s", expectedFuncs[i], expr.Math.Function)
			}
			if expr.Alias != expectedAliases[i] {
				t.Errorf("Expected alias '%s', got '%s'", expectedAliases[i], expr.Alias)
			}
		}
	})
}

func TestMathFuncConstants(t *testing.T) {
	// Test that all math function constants are properly defined
	expectedFuncs := map[MathFunc]string{
		MathRound: "ROUND",
		MathFloor: "FLOOR",
		MathCeil:  "CEIL",
		MathAbs:   "ABS",
		MathPower: "POWER",
		MathSqrt:  "SQRT",
	}

	for mathFunc, expectedValue := range expectedFuncs {
		if string(mathFunc) != expectedValue {
			t.Errorf("Expected %s constant to be '%s', got '%s'", expectedValue, expectedValue, string(mathFunc))
		}
	}
}

func TestMathExpressionStructure(t *testing.T) {
	t.Run("MathExpression with all fields", func(t *testing.T) {
		field := types.Field{Name: "test_field"}
		precision := types.Param{Name: "precision_param"}
		exponent := types.Param{Name: "exponent_param"}
		alias := "test_alias"

		expr := MathExpression{
			Function:  MathRound,
			Field:     field,
			Precision: &precision,
			Exponent:  &exponent,
			Alias:     alias,
		}

		if expr.Function != MathRound {
			t.Error("Expected function to be set")
		}
		if expr.Field != field {
			t.Error("Expected field to be set")
		}
		if expr.Precision == nil || *expr.Precision != precision {
			t.Error("Expected precision to be set")
		}
		if expr.Exponent == nil || *expr.Exponent != exponent {
			t.Error("Expected exponent to be set")
		}
		if expr.Alias != alias {
			t.Error("Expected alias to be set")
		}
	})

	t.Run("MathExpression with minimal fields", func(t *testing.T) {
		field := types.Field{Name: "simple"}

		expr := MathExpression{
			Function: MathFloor,
			Field:    field,
		}

		if expr.Function != MathFloor {
			t.Error("Expected function to be set")
		}
		if expr.Field != field {
			t.Error("Expected field to be set")
		}
		if expr.Precision != nil {
			t.Error("Expected precision to be nil")
		}
		if expr.Exponent != nil {
			t.Error("Expected exponent to be nil")
		}
		if expr.Alias != "" {
			t.Error("Expected alias to be empty")
		}
	})
}

func TestMathFluentInterface(t *testing.T) {
	t.Run("As returns same expression with alias", func(t *testing.T) {
		originalExpr := Round(types.Field{Name: "price"})
		aliasedExpr := originalExpr.As("rounded")

		// Should be the same expression with alias added
		if aliasedExpr.Function != originalExpr.Function {
			t.Error("Expected function to be preserved")
		}
		if aliasedExpr.Field != originalExpr.Field {
			t.Error("Expected field to be preserved")
		}
		if aliasedExpr.Alias != "rounded" {
			t.Error("Expected alias to be set")
		}
	})

	t.Run("Chaining As calls", func(t *testing.T) {
		expr := Abs(types.Field{Name: "balance"}).
			As("first_alias").
			As("final_alias")

		// Should use the last alias
		if expr.Alias != "final_alias" {
			t.Errorf("Expected final alias 'final_alias', got '%s'", expr.Alias)
		}
	})
}
