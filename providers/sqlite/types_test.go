package sqlite

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/sentinel"
)

func TestSQLiteTypes(t *testing.T) {
	// Register test structs using sentinel
	sentinel.Inspect[User]()

	t.Run("CF field comparison", func(t *testing.T) {
		leftField := astql.F("age")
		rightField := astql.F("id")

		fc := CF(leftField, astql.LT, rightField)
		if fc.LeftField.Name != "age" {
			t.Errorf("Expected left field 'age', got '%s'", fc.LeftField.Name)
		}
		if fc.RightField.Name != "id" {
			t.Errorf("Expected right field 'id', got '%s'", fc.RightField.Name)
		}
		if fc.Operator != astql.LT {
			t.Errorf("Expected operator LT, got %s", fc.Operator)
		}
	})

	t.Run("TryCF field comparison", func(t *testing.T) {
		// Valid case with valid operator
		fc, err := TryCF(astql.F("age"), astql.LT, astql.F("id"))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if fc.LeftField.Name != "age" {
			t.Errorf("Expected left field 'age', got '%s'", fc.LeftField.Name)
		}
		if fc.RightField.Name != "id" {
			t.Errorf("Expected right field 'id', got '%s'", fc.RightField.Name)
		}
		if fc.Operator != astql.LT {
			t.Errorf("Expected operator LT, got %s", fc.Operator)
		}

		// Invalid operator for field comparison (LIKE is not supported)
		_, err = TryCF(astql.F("name"), astql.LIKE, astql.F("email"))
		if err == nil {
			t.Error("Expected error for unsupported operator LIKE in field comparison")
		}
		if err != nil && !strings.Contains(err.Error(), "not supported") {
			t.Errorf("Expected error to mention 'not supported', got: %v", err)
		}
	})

	t.Run("FieldComparison IsConditionItem", func(_ *testing.T) {
		fc := FieldComparison{
			LeftField:  astql.F("id"),
			Operator:   astql.EQ,
			RightField: astql.F("id"),
		}
		fc.IsConditionItem()
		// Just verify it compiles and doesn't panic
	})
}
