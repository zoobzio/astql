package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

// TestPostgresSQLInjectionProtection verifies PostgreSQL extensions maintain injection protection.
func TestPostgresSQLInjectionProtection(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("JOIN injection attempts", func(t *testing.T) {
		// Try to inject through table name in JOIN
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ JOIN Table: Blocked injection: %v", r)
			} else {
				t.Error("✗ JOIN Table: Failed to block injection")
			}
		}()

		postgres.Select(astql.T("test_users")).
			Join(
				astql.T("users; DROP TABLE admin; --"),
				astql.C(astql.F("id"), astql.EQ, astql.P("id")),
			).
			MustBuild()
	})

	t.Run("GROUP BY injection attempts", func(t *testing.T) {
		// Try to inject through field in GROUP BY
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ GROUP BY: Blocked injection: %v", r)
			} else {
				t.Error("✗ GROUP BY: Failed to block injection")
			}
		}()

		postgres.Select(astql.T("test_users")).
			GroupBy(astql.F("age; DROP TABLE users; --")).
			MustBuild()
	})

	t.Run("HAVING injection attempts", func(t *testing.T) {
		// Try to inject through field in HAVING
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ HAVING: Blocked injection: %v", r)
			} else {
				t.Error("✗ HAVING: Failed to block injection")
			}
		}()

		postgres.Select(astql.T("test_users")).
			GroupBy(astql.F("age")).
			Having(astql.C(astql.F("1=1; DELETE FROM users; --"), astql.GT, astql.P("value"))).
			MustBuild()
	})

	t.Run("RETURNING injection attempts", func(t *testing.T) {
		// Try to inject through field in RETURNING
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ RETURNING: Blocked injection: %v", r)
			} else {
				t.Error("✗ RETURNING: Failed to block injection")
			}
		}()

		postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"): astql.P("name"),
			}).
			Returning(astql.F("id; DROP TABLE users; --")).
			MustBuild()
	})

	t.Run("ON CONFLICT injection attempts", func(t *testing.T) {
		// Try to inject through field in ON CONFLICT
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ ON CONFLICT: Blocked injection: %v", r)
			} else {
				t.Error("✗ ON CONFLICT: Failed to block injection")
			}
		}()

		postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("email"): astql.P("email"),
			}).
			OnConflict(astql.F("email); DROP TABLE users; --")).
			DoNothing().
			MustBuild()
	})

	t.Run("Aggregate function injection attempts", func(t *testing.T) {
		// Try to inject through field in aggregate
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ Aggregate: Blocked injection: %v", r)
			} else {
				t.Error("✗ Aggregate: Failed to block injection")
			}
		}()

		postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.Sum(astql.F("age); DROP TABLE users; --"))).
			MustBuild()
	})

	t.Run("Complex nested injection", func(t *testing.T) {
		// Try multiple injection points
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ Complex: Blocked injection at some point: %v", r)
			} else {
				t.Error("✗ Complex: Failed to block any injection")
			}
		}()

		postgres.Select(astql.T("test_users", "u")).
			Distinct().
			Fields(astql.F("name")).
			Join(
				astql.T("test_users", "m"),
				astql.C(astql.F("id' OR '1'='1"), astql.EQ, astql.P("id")),
			).
			GroupBy(astql.F("age")).
			Having(astql.C(astql.F("COUNT(*); --"), astql.GT, astql.P("min"))).
			MustBuild()
	})

	t.Run("Parameter isolation in PostgreSQL features", func(t *testing.T) {
		// Ensure parameters remain isolated in all PostgreSQL features
		query := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("email"): astql.P("userEmail"),
				astql.F("name"):  astql.P("userName"),
			}).
			OnConflict(astql.F("email")).
			DoUpdate(map[astql.Field]astql.Param{
				astql.F("name"):       astql.P("newName"),
				astql.F("updated_at"): astql.P("timestamp"),
			}).
			Returning(astql.F("id")).
			MustBuild()

		// Check that only parameter names are stored, not values
		for _, param := range query.Values[0] {
			if param.Type != astql.ParamNamed || param.Name == "" {
				t.Error("Values should only contain parameter references")
			}
		}

		for _, param := range query.OnConflict.Updates {
			if param.Type != astql.ParamNamed || param.Name == "" {
				t.Error("ON CONFLICT updates should only contain parameter references")
			}
		}
	})
}
