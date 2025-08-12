package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

func TestParamValidation(t *testing.T) {
	t.Run("Valid parameter names", func(t *testing.T) {
		validNames := []string{
			"userId",
			"user_id",
			"firstName",
			"age",
			"minAge",
			"max_value",
			"param123",
			"p1",
		}

		for _, name := range validNames {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Valid param name '%s' caused panic: %v", name, r)
					}
				}()

				param := astql.P(name)
				if param.Name != name {
					t.Errorf("Expected param name '%s', got '%s'", name, param.Name)
				}
			}()
		}
	})

	t.Run("Invalid parameter names", func(t *testing.T) {
		invalidNames := []struct {
			name   string
			reason string
		}{
			{"", "empty string"},
			{"123param", "starts with number"},
			{"_param", "starts with underscore"},
			{"param-name", "contains hyphen"},
			{"param name", "contains space"},
			{"param;", "contains semicolon"},
			{"param'", "contains quote"},
			{"param--", "contains SQL comment"},
			{"select", "SQL keyword"},
			{"SELECT", "SQL keyword uppercase"},
			{"drop", "dangerous SQL keyword"},
			{"table", "SQL keyword"},
			{"where", "SQL keyword"},
			{"union", "SQL keyword"},
			{"param/*test*/", "contains SQL comment"},
			{`param"test"`, "contains quotes"},
			{"param\\", "contains backslash"},
		}

		for _, test := range invalidNames {
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Invalid param name '%s' (%s) did not panic", test.name, test.reason)
					}
				}()

				astql.P(test.name)
			}()
		}
	})

	t.Run("Positional parameters don't need validation", func(t *testing.T) {
		// These should all work without validation
		p1 := astql.P1()
		p2 := astql.P2()
		p3 := astql.P3()

		if p1.Type != astql.ParamPositional || p1.Index != 1 {
			t.Error("P1() not working correctly")
		}
		if p2.Type != astql.ParamPositional || p2.Index != 2 {
			t.Error("P2() not working correctly")
		}
		if p3.Type != astql.ParamPositional || p3.Index != 3 {
			t.Error("P3() not working correctly")
		}
	})

	t.Run("Positional parameters P4 and P5", func(t *testing.T) {
		// Test P4
		p4 := astql.P4()
		if p4.Type != astql.ParamPositional {
			t.Errorf("Expected positional parameter type, got %v", p4.Type)
		}
		if p4.Index != 4 {
			t.Errorf("Expected index 4, got %d", p4.Index)
		}

		// Test P5
		p5 := astql.P5()
		if p5.Type != astql.ParamPositional {
			t.Errorf("Expected positional parameter type, got %v", p5.Type)
		}
		if p5.Index != 5 {
			t.Errorf("Expected index 5, got %d", p5.Index)
		}
	})

	t.Run("All positional parameters have correct indices", func(t *testing.T) {
		params := []astql.Param{
			astql.P1(),
			astql.P2(),
			astql.P3(),
			astql.P4(),
			astql.P5(),
		}

		for i, p := range params {
			expectedIndex := i + 1
			if p.Index != expectedIndex {
				t.Errorf("P%d() returned index %d, expected %d", expectedIndex, p.Index, expectedIndex)
			}
			if p.Type != astql.ParamPositional {
				t.Errorf("P%d() returned type %v, expected ParamPositional", expectedIndex, p.Type)
			}
		}
	})
}
