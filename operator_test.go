package astql

import (
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestOperatorConstants(t *testing.T) {
	tests := []struct {
		name     string
		operator types.Operator
		expected string
	}{
		// Basic comparison operators
		{"Equal", types.EQ, "="},
		{"NotEqual", types.NE, "!="},
		{"GreaterThan", types.GT, ">"},
		{"GreaterOrEqual", types.GE, ">="},
		{"LessThan", types.LT, "<"},
		{"LessOrEqual", types.LE, "<="},

		// Extended operators
		{"In", types.IN, "IN"},
		{"NotIn", types.NotIn, "NOT IN"},
		{"Like", types.LIKE, "LIKE"},
		{"NotLike", types.NotLike, "NOT LIKE"},
		{"IsNull", types.IsNull, "IS NULL"},
		{"IsNotNull", types.IsNotNull, "IS NOT NULL"},
		{"Exists", types.EXISTS, "EXISTS"},
		{"NotExists", types.NotExists, "NOT EXISTS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.operator) != tt.expected {
				t.Errorf("Expected %s to be '%s', got '%s'", tt.name, tt.expected, string(tt.operator))
			}
		})
	}
}

func TestOperatorType(t *testing.T) {
	// Ensure Operator can be used as a string
	var op types.Operator = "TEST"
	// Type assertion to Operator, not string
	if _, ok := interface{}(op).(types.Operator); !ok {
		t.Error("Should be able to assert to Operator type")
	}
	// Can convert to string
	s := string(op)
	if s != "TEST" {
		t.Error("Should be able to convert Operator to string")
	}
}
