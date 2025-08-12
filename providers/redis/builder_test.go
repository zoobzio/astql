package redis_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/redis"
)

func TestRedisBuilder(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Update operation", func(t *testing.T) {
		// Redis doesn't support UPDATE in the traditional sense
		builder := redis.Update(astql.T("test_sessions"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported UPDATE operation")
		}

		// Build should return the error
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected Build to return error for UPDATE")
		}
		if err.Error() != "UPDATE not supported by Redis provider" {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})

	t.Run("Unlisten operation", func(t *testing.T) {
		// Redis doesn't support UNLISTEN
		builder := redis.Unlisten(astql.T("test_sessions"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported UNLISTEN operation")
		}

		_, err := builder.Build()
		if err == nil {
			t.Error("Expected Build to return error for UNLISTEN")
		}
	})

	t.Run("WhereField method", func(t *testing.T) {
		// WhereField is not supported for Redis
		builder := redis.Select(astql.T("test_sessions")).
			WhereField(astql.F("id"), astql.EQ, astql.P("user_id"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported WhereField method")
		}
	})

	t.Run("Set method", func(t *testing.T) {
		// Set is not supported for Redis
		builder := redis.Select(astql.T("test_sessions")).
			Set(astql.F("data"), astql.P("new_data"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported Set method")
		}
	})

	t.Run("OrderBy method", func(t *testing.T) {
		// OrderBy is not supported for Redis
		builder := redis.Select(astql.T("test_sessions")).
			OrderBy(astql.F("id"), astql.ASC)

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported OrderBy method")
		}
	})

	t.Run("MustBuild panics on error", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic from MustBuild with error")
			}
		}()

		// Create a builder with an error
		builder := redis.Update(astql.T("test_sessions")) // This sets an error

		// MustBuild should panic
		builder.MustBuild()
	})

	t.Run("Builder method chaining", func(t *testing.T) {
		builder := redis.Select(astql.T("test_sessions"))

		// Chain valid operations
		b1 := builder.Where(astql.C(astql.F("id"), astql.EQ, astql.P("session_id")))
		b2 := b1.Fields(astql.F("id"), astql.F("data"))
		b3 := b2.Limit(10)
		b4 := b3.Offset(5)

		// All should be the same instance
		if b1 != builder || b2 != builder || b3 != builder || b4 != builder {
			t.Error("Builder methods should return the same builder instance")
		}

		// Should build successfully
		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ast.Limit == nil || *ast.Limit != 10 {
			t.Error("Expected limit to be 10")
		}
		if ast.Offset == nil || *ast.Offset != 5 {
			t.Error("Expected offset to be 5")
		}
	})

	t.Run("Builder with TTL", func(t *testing.T) {
		builder := redis.Insert(astql.T("test_sessions")).
			Values(map[astql.Field]astql.Param{
				astql.F("id"):   astql.P("session_id"),
				astql.F("data"): astql.P("session_data"),
			}).
			WithTTL(astql.P("ttl_seconds"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ast.TTLParam == nil {
			t.Fatal("Expected TTL parameter to be set")
		}
		if ast.TTLParam.Name != "ttl_seconds" {
			t.Errorf("Expected TTL param name 'ttl_seconds', got %q", ast.TTLParam.Name)
		}
	})

	t.Run("Builder with Score for sorted sets", func(t *testing.T) {
		builder := redis.Insert(astql.T("test_leaderboard")).
			Values(map[astql.Field]astql.Param{
				astql.F("user_id"): astql.P("player_id"),
			}).
			WithScore(astql.P("player_score"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ast.ScoreParam == nil {
			t.Fatal("Expected Score parameter to be set")
		}
		if ast.ScoreParam.Name != "player_score" {
			t.Errorf("Expected Score param name 'player_score', got %q", ast.ScoreParam.Name)
		}
	})

	t.Run("Error propagation in builder", func(t *testing.T) {
		// Start with an operation that sets an error
		builder := redis.Update(astql.T("test_sessions"))

		// Try to chain more operations
		builder.
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("id"))).
			Fields(astql.F("data")).
			WithTTL(astql.P("ttl"))

		// Original error should persist
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected error to persist through method chaining")
		}
		if err.Error() != "UPDATE not supported by Redis provider" {
			t.Errorf("Expected original error message, got: %v", err)
		}
	})
}
