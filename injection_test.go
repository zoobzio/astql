package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

// TestSQLInjectionProtection verifies that ASTQL prevents SQL injection attempts.
func TestSQLInjectionProtection(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Field injection attempts", func(t *testing.T) {
		// These should all panic because the field names aren't registered
		injectionAttempts := []struct {
			name      string
			fieldName string
		}{
			{"DROP TABLE", "email; DROP TABLE users; --"},
			{"Union injection", "id UNION SELECT * FROM passwords"},
			{"OR 1=1", "id OR 1=1"},
			{"Subquery injection", "id FROM (SELECT * FROM admin_users)"},
			{"Comment injection", "id/**/OR/**/1=1"},
			{"Stacked queries", "id; DELETE FROM users"},
			{"Backtick injection", "id` FROM users; DROP TABLE users; --"},
			{"Quote injection", "id' OR '1'='1"},
			{"Double quote injection", `id" OR "1"="1`},
			{"Null byte injection", "id\x00 OR 1=1"},
			{"Unicode injection", "id\u0027 OR 1=1"},
			{"Case bypass", "ID OR 1=1"},
			{"Whitespace tricks", "id\nOR\n1=1"},
			{"Function injection", "id) OR SLEEP(10)--"},
			{"Nested comments", "id/**/OR/**/1/**/=/**/1"},
		}

		for _, attempt := range injectionAttempts {
			t.Run(attempt.name, func(t *testing.T) {
				// Test in Fields
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Logf("✓ Fields: Blocked '%s': %v", attempt.fieldName, r)
						} else {
							t.Errorf("✗ Fields: Failed to block '%s'", attempt.fieldName)
						}
					}()

					astql.F(attempt.fieldName)
				}()

				// Test in WHERE condition
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Logf("✓ WHERE: Blocked '%s': %v", attempt.fieldName, r)
						} else {
							t.Errorf("✗ WHERE: Failed to block '%s'", attempt.fieldName)
						}
					}()

					astql.Select(astql.T("test_users")).
						Where(astql.C(astql.F(attempt.fieldName), astql.EQ, astql.P("value"))).
						MustBuild()
				}()
			})
		}
	})

	t.Run("Table injection attempts", func(t *testing.T) {
		injectionAttempts := []struct {
			name      string
			tableName string
		}{
			{"DROP TABLE", "users; DROP TABLE admin; --"},
			{"Union injection", "users UNION SELECT * FROM passwords"},
			{"Subquery", "(SELECT * FROM admin_users)"},
			{"Comment injection", "users/**/DROP/**/TABLE/**/admin"},
			{"Stacked queries", "users; DELETE FROM users"},
			{"Backtick injection", "users` DROP TABLE admin; --"},
			{"Function injection", "users); TRUNCATE TABLE admin; --"},
			{"Case tricks", "USERS WHERE 1=1--"},
		}

		for _, attempt := range injectionAttempts {
			t.Run(attempt.name, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("✓ Table: Blocked '%s': %v", attempt.tableName, r)
					} else {
						t.Errorf("✗ Table: Failed to block '%s'", attempt.tableName)
					}
				}()

				astql.T(attempt.tableName)
			})
		}
	})

	t.Run("Parameter names are validated", func(t *testing.T) {
		// Parameter names must be valid identifiers
		validParams := []string{
			"userInput",
			"emailAddress",
			"user_id",
			"param123",
		}

		for _, param := range validParams {
			// Valid parameter names should work
			p := astql.P(param)
			if p.Name != param {
				t.Errorf("Parameter name should be '%s', got '%s'", param, p.Name)
			}
		}

		// Invalid parameter names should panic
		invalidParams := []string{
			"DROP TABLE users",
			"1=1",
			"'; DELETE FROM users; --",
			"select",
			"table",
		}

		for _, param := range invalidParams {
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for invalid param name '%s'", param)
					}
				}()
				astql.P(param)
			}()
		}

		// Build a query with safe parameter name
		query := astql.Select(astql.T("test_users")).
			Where(astql.C(astql.F("email"), astql.EQ, astql.P("userEmail"))).
			MustBuild()

		// The AST should contain the parameter name
		cond := query.WhereClause.(astql.Condition)
		if cond.Value.Name != "userEmail" {
			t.Error("Parameter name was modified")
		}
	})

	// Schema-based injection tests have been moved to providers/postgres/schema_test.go
	// since schemas are now provider-specific

	t.Run("Field registry validation", func(t *testing.T) {
		// These are the only valid fields from TestUser
		validFields := []string{"id", "name", "email", "age"}

		for _, field := range validFields {
			// Should not panic
			f := astql.F(field)
			if f.Name != field {
				t.Errorf("Expected field name '%s', got '%s'", field, f.Name)
			}
		}

		// Any field not in the registry should panic
		invalidFields := []string{
			"password",
			"admin",
			"secret",
			"users",
			"SELECT",
			"DROP",
		}

		for _, field := range invalidFields {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("✓ Registry: Blocked unregistered field '%s'", field)
					} else {
						t.Errorf("✗ Registry: Failed to block unregistered field '%s'", field)
					}
				}()

				astql.F(field)
			}()
		}
	})
}

// TestParameterIsolation verifies that user values never contaminate the AST.
func TestParameterIsolation(t *testing.T) {
	astql.SetupTestModels()

	t.Run("AST contains only parameter names", func(t *testing.T) {
		// Build a query with parameters
		query := astql.Select(astql.T("test_users")).
			Where(
				astql.And(
					astql.C(astql.F("email"), astql.EQ, astql.P("userEmail")),
					astql.C(astql.F("age"), astql.GT, astql.P("minAge")),
				),
			).
			MustBuild()

		// Check that the AST only contains parameter names, not values
		group := query.WhereClause.(astql.ConditionGroup)

		cond1 := group.Conditions[0].(astql.Condition)
		if cond1.Value.Name != "userEmail" {
			t.Errorf("Expected parameter name 'userEmail', got '%s'", cond1.Value.Name)
		}

		cond2 := group.Conditions[1].(astql.Condition)
		if cond2.Value.Name != "minAge" {
			t.Errorf("Expected parameter name 'minAge', got '%s'", cond2.Value.Name)
		}
	})

	t.Run("INSERT values are parameter names only", func(t *testing.T) {
		query := astql.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"):  astql.P("userName"),
				astql.F("email"): astql.P("userEmail"),
			}).
			MustBuild()

		// Verify only parameter names are stored
		for field, param := range query.Values[0] {
			if param.Type != astql.ParamNamed {
				t.Errorf("Expected named parameter for field %s", field.Name)
			}
			if param.Name == "" {
				t.Errorf("Parameter name should not be empty for field %s", field.Name)
			}
		}
	})

	t.Run("UPDATE values are parameter names only", func(t *testing.T) {
		query := astql.Update(astql.T("test_users")).
			Set(astql.F("name"), astql.P("newName")).
			Set(astql.F("email"), astql.P("newEmail")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			MustBuild()

		// Verify only parameter names are stored
		for field, param := range query.Updates {
			if param.Type != astql.ParamNamed {
				t.Errorf("Expected named parameter for field %s", field.Name)
			}
			if param.Name == "" {
				t.Errorf("Parameter name should not be empty for field %s", field.Name)
			}
		}
	})
}
