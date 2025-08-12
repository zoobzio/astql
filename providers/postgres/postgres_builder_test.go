package postgres_test

import (
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"
)

func TestPostgresBuilder(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("SELECT with DISTINCT", func(t *testing.T) {
		query := postgres.Select(astql.T("test_users")).
			Distinct().
			Fields(astql.F("email")).
			MustBuild()

		if !query.Distinct {
			t.Error("Expected DISTINCT to be true")
		}
	})

	t.Run("SELECT with JOIN", func(t *testing.T) {
		query := postgres.Select(astql.T("test_users", "u")).
			Fields(astql.F("name").WithTable("u")).
			Join(
				astql.T("test_users", "m"),
				astql.C(
					astql.F("id").WithTable("u"),
					astql.EQ,
					// For now using param, but we'll need field references later
					astql.P("managerId"),
				),
			).
			MustBuild()

		if len(query.Joins) != 1 {
			t.Errorf("Expected 1 join, got %d", len(query.Joins))
		}

		if query.Joins[0].Type != postgres.InnerJoin {
			t.Errorf("Expected INNER JOIN, got %s", query.Joins[0].Type)
		}
	})

	t.Run("SELECT with GROUP BY and HAVING", func(t *testing.T) {
		query := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.CountField(astql.F("id")).As("user_count")).
			Fields(astql.F("age")).
			GroupBy(astql.F("age")).
			Having(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			MustBuild()

		if len(query.GroupBy) != 1 {
			t.Errorf("Expected 1 GROUP BY field, got %d", len(query.GroupBy))
		}

		if len(query.Having) != 1 {
			t.Errorf("Expected 1 HAVING condition, got %d", len(query.Having))
		}

		if len(query.FieldExpressions) != 1 {
			t.Errorf("Expected 1 field expression, got %d", len(query.FieldExpressions))
		}
	})

	t.Run("SELECT with aggregates", func(t *testing.T) {
		query := postgres.Select(astql.T("test_users")).
			SelectExpr(postgres.Sum(astql.F("age")).As("total_age")).
			SelectExpr(postgres.Avg(astql.F("age")).As("avg_age")).
			SelectExpr(postgres.Min(astql.F("age")).As("min_age")).
			SelectExpr(postgres.Max(astql.F("age")).As("max_age")).
			MustBuild()

		if len(query.FieldExpressions) != 4 {
			t.Errorf("Expected 4 field expressions, got %d", len(query.FieldExpressions))
		}

		// Check aggregates
		expectedAggs := []postgres.AggregateFunc{
			postgres.AggSum,
			postgres.AggAvg,
			postgres.AggMin,
			postgres.AggMax,
		}

		for i, expr := range query.FieldExpressions {
			if expr.Aggregate != expectedAggs[i] {
				t.Errorf("Expression %d: expected %s, got %s", i, expectedAggs[i], expr.Aggregate)
			}
		}
	})

	t.Run("INSERT with RETURNING", func(t *testing.T) {
		query := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"):  astql.P("name"),
				astql.F("email"): astql.P("email"),
			}).
			Returning(astql.F("id"), astql.F("created_at")).
			MustBuild()

		if len(query.Returning) != 2 {
			t.Errorf("Expected 2 RETURNING fields, got %d", len(query.Returning))
		}
	})

	t.Run("INSERT with ON CONFLICT DO NOTHING", func(t *testing.T) {
		query := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("email"): astql.P("email"),
				astql.F("name"):  astql.P("name"),
			}).
			OnConflict(astql.F("email")).
			DoNothing().
			MustBuild()

		if query.OnConflict == nil {
			t.Fatal("Expected ON CONFLICT clause")
		}

		if query.OnConflict.Action != postgres.DoNothing {
			t.Errorf("Expected DO NOTHING, got %s", query.OnConflict.Action)
		}
	})

	t.Run("INSERT with ON CONFLICT DO UPDATE", func(t *testing.T) {
		query := postgres.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("email"): astql.P("email"),
				astql.F("name"):  astql.P("name"),
			}).
			OnConflict(astql.F("email")).
			DoUpdate(map[astql.Field]astql.Param{
				astql.F("name"):       astql.P("newName"),
				astql.F("updated_at"): astql.P("now"),
			}).
			Returning(astql.F("id")).
			MustBuild()

		if query.OnConflict == nil {
			t.Fatal("Expected ON CONFLICT clause")
		}

		if query.OnConflict.Action != postgres.DoUpdate {
			t.Errorf("Expected DO UPDATE, got %s", query.OnConflict.Action)
		}

		if len(query.OnConflict.Updates) != 2 {
			t.Errorf("Expected 2 updates, got %d", len(query.OnConflict.Updates))
		}
	})

	t.Run("UPDATE with RETURNING", func(t *testing.T) {
		query := postgres.Update(astql.T("test_users")).
			Set(astql.F("name"), astql.P("newName")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			Returning(astql.F("id"), astql.F("updated_at")).
			MustBuild()

		if len(query.Returning) != 2 {
			t.Errorf("Expected 2 RETURNING fields, got %d", len(query.Returning))
		}
	})

	t.Run("DELETE with RETURNING", func(t *testing.T) {
		query := postgres.Delete(astql.T("test_users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			Returning(astql.F("id"), astql.F("deleted_at")).
			MustBuild()

		if len(query.Returning) != 2 {
			t.Errorf("Expected 2 RETURNING fields, got %d", len(query.Returning))
		}
	})

	t.Run("Complex query with multiple features", func(t *testing.T) {
		query := postgres.Select(astql.T("test_users", "u")).
			Distinct().
			Fields(astql.F("age")).
			SelectExpr(postgres.CountField(astql.F("id")).As("count")).
			LeftJoin(
				astql.T("test_users", "m"),
				astql.C(astql.F("manager_id").WithTable("u"), astql.EQ, astql.P("managerId")),
			).
			Where(astql.C(astql.F("active").WithTable("u"), astql.EQ, astql.P("isActive"))).
			GroupBy(astql.F("age")).
			Having(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			OrderBy(astql.F("age"), astql.DESC).
			Limit(10).
			MustBuild()

		if !query.Distinct {
			t.Error("Expected DISTINCT")
		}
		if len(query.Joins) != 1 {
			t.Error("Expected 1 JOIN")
		}
		if len(query.GroupBy) != 1 {
			t.Error("Expected 1 GROUP BY")
		}
		if len(query.Having) != 1 {
			t.Error("Expected 1 HAVING")
		}
	})

	t.Run("Invalid operations fail", func(t *testing.T) {
		// DISTINCT on INSERT should fail
		_, err := postgres.Insert(astql.T("test_users")).
			Distinct().
			Build()
		if err == nil {
			t.Error("Expected error for DISTINCT on INSERT")
		}

		// HAVING without GROUP BY should fail
		_, err = postgres.Select(astql.T("test_users")).
			Having(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			Build()
		if err == nil {
			t.Error("Expected error for HAVING without GROUP BY")
		}

		// ON CONFLICT on SELECT should fail
		_, err = postgres.Select(astql.T("test_users")).
			OnConflict(astql.F("email")).
			DoNothing().
			Build()
		if err == nil {
			t.Error("Expected error for ON CONFLICT on SELECT")
		}
	})
}
