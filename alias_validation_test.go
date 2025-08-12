package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

func TestAliasValidation(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Table alias must be single lowercase letter", func(t *testing.T) {
		// Valid single letter aliases
		validAliases := []string{"a", "b", "z", "u", "m"}
		for _, alias := range validAliases {
			table := astql.T("test_users", alias)
			if table.Alias != alias {
				t.Errorf("Expected alias %s, got %s", alias, table.Alias)
			}
		}

		// Invalid aliases should panic
		invalidAliases := []struct {
			alias string
			desc  string
		}{
			{"A", "uppercase letter"},
			{"ab", "multiple letters"},
			{"1", "number"},
			{"u1", "letter and number"},
			{"u_", "with underscore"},
			{"", "empty string"},
			{"u; DROP TABLE", "SQL injection"},
			{"'", "quote"},
		}

		for _, test := range invalidAliases {
			t.Run(test.desc, func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for alias '%s' (%s)", test.alias, test.desc)
					}
				}()

				astql.T("test_users", test.alias)
			})
		}
	})

	t.Run("WithTable validates aliases and table names", func(t *testing.T) {
		// Valid cases
		field := astql.F("name")

		// Single letter alias - OK
		f1 := field.WithTable("u")
		if f1.Table != "u" {
			t.Errorf("Expected table 'u', got '%s'", f1.Table)
		}

		// Valid table name - OK
		f2 := field.WithTable("test_users")
		if f2.Table != "test_users" {
			t.Errorf("Expected table 'test_users', got '%s'", f2.Table)
		}

		// Invalid cases should panic
		invalidRefs := []string{
			"U",             // uppercase
			"ab",            // two letters
			"u1",            // letter+number
			"invalid_table", // unregistered table
			"u; DROP TABLE", // injection
		}

		for _, ref := range invalidRefs {
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for WithTable('%s')", ref)
					}
				}()

				field.WithTable(ref)
			}()
		}
	})

	t.Run("Field aliases from struct tags", func(_ *testing.T) {
		// These aliases are defined in TestUser struct tags
		// This test just verifies they exist in the registry
		// The actual validation happens when using F() with these aliases

		// For now, this is a placeholder test
		// In a real implementation, we'd test that these aliases work
		// when building queries
	})

	// PostgreSQL-specific alias tests have been moved to providers/postgres package
}
