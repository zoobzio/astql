package sqlite

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// FieldComparison represents a comparison between two fields.
// This is commonly used in JOIN conditions.
type FieldComparison struct {
	LeftField  types.Field
	Operator   types.Operator
	RightField types.Field
}

// Implement ConditionItem interface.
func (FieldComparison) IsConditionItem() {}

// CF creates a field-to-field comparison.
// Used for JOIN conditions like: CF(F("u.id"), EQ, F("o.user_id")).
func CF(left types.Field, op types.Operator, right types.Field) FieldComparison {
	// Validate operator is appropriate for field comparison
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE:
		// These are valid for field comparisons
	default:
		panic(fmt.Errorf("operator %s not supported for field comparisons", op))
	}

	return FieldComparison{
		LeftField:  left,
		Operator:   op,
		RightField: right,
	}
}

// TryCF creates a field-to-field comparison, returning an error if invalid.
func TryCF(left types.Field, op types.Operator, right types.Field) (FieldComparison, error) {
	// Validate operator is appropriate for field comparison
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE:
		// These are valid for field comparisons
	default:
		return FieldComparison{}, fmt.Errorf("operator %s not supported for field comparisons", op)
	}

	return FieldComparison{
		LeftField:  left,
		Operator:   op,
		RightField: right,
	}, nil
}
