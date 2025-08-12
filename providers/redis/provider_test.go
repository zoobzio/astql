package redis_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/redis"
)

func TestRedisProvider(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	// Create and configure provider
	provider := redis.NewProvider()

	// Register table configurations
	provider.RegisterTable("test_users", redis.TableConfig{
		KeyPattern: "users:{id}",
		DataType:   redis.TypeHash,
		IDField:    "id",
	})

	provider.RegisterTable("test_sessions", redis.TableConfig{
		KeyPattern: "session:{id}",
		DataType:   redis.TypeString,
		IDField:    "id",
	})

	provider.RegisterTable("test_active_users", redis.TableConfig{
		KeyPattern: "active_users",
		DataType:   redis.TypeSet,
		IDField:    "user_id",
	})

	provider.RegisterTable("test_leaderboards", redis.TableConfig{
		KeyPattern: "leaderboard",
		DataType:   redis.TypeZSet,
		IDField:    "user_id",
	})

	t.Run("SELECT from Hash", func(t *testing.T) {
		builder := redis.Select(astql.T("test_users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("user_id")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "HGETALL users::user_id"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("SELECT specific fields from Hash", func(t *testing.T) {
		builder := redis.Select(astql.T("test_users")).
			Fields(astql.F("name"), astql.F("email")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("user_id")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "HMGET users::user_id name email"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("INSERT into Hash", func(t *testing.T) {
		builder := redis.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("id"):    astql.P("user_id"),
				astql.F("name"):  astql.P("name"),
				astql.F("email"): astql.P("email"),
			})

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Order might vary, so just check it contains the right command
		if !contains(result.SQL, "HSET users::user_id") {
			t.Errorf("Expected HSET command, got: %s", result.SQL)
		}
		if !contains(result.SQL, "name :name") {
			t.Errorf("Expected name field, got: %s", result.SQL)
		}
		if !contains(result.SQL, "email :email") {
			t.Errorf("Expected email field, got: %s", result.SQL)
		}
	})

	t.Run("DELETE key", func(t *testing.T) {
		builder := redis.Delete(astql.T("test_users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("user_id")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "DEL users::user_id"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("INSERT into String with TTL", func(t *testing.T) {
		builder := redis.Insert(astql.T("test_sessions")).
			Values(map[astql.Field]astql.Param{
				astql.F("id"):   astql.P("session_id"),
				astql.F("data"): astql.P("session_data"),
			}).
			WithTTL(astql.P("ttl"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Should use SETEX for TTL
		if !contains(result.SQL, "SETEX session::session_id") {
			t.Errorf("Expected SETEX command, got: %s", result.SQL)
		}
	})

	t.Run("COUNT Set members", func(t *testing.T) {
		builder := redis.Count(astql.T("test_active_users"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Without WHERE, should count all keys
		expected := "KEYS active_users"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("SELECT from Sorted Set with LIMIT", func(t *testing.T) {
		builder := redis.Select(astql.T("test_leaderboards")).
			Limit(10).
			Offset(0)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "ZRANGE leaderboard 0 9 WITHSCORES"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("LISTEN/NOTIFY operations", func(t *testing.T) {
		// Test LISTEN (SUBSCRIBE)
		listenBuilder := redis.Listen(astql.T("test_users"))
		listenAst, err := listenBuilder.Build()
		if err != nil {
			t.Fatalf("Failed to build LISTEN: %v", err)
		}

		result, err := provider.Render(listenAst)
		if err != nil {
			t.Fatalf("Failed to render LISTEN: %v", err)
		}

		expected := "SUBSCRIBE test_users_changes"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}

		// Test NOTIFY (PUBLISH)
		notifyBuilder := redis.Notify(astql.T("test_users"), astql.P("message"))
		notifyAst, err := notifyBuilder.Build()
		if err != nil {
			t.Fatalf("Failed to build NOTIFY: %v", err)
		}

		result, err = provider.Render(notifyAst)
		if err != nil {
			t.Fatalf("Failed to render NOTIFY: %v", err)
		}

		expected = "PUBLISH test_users_changes :message"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("Unsupported operations fail gracefully", func(t *testing.T) {
		// Redis doesn't support JOINs
		builder := redis.Select(astql.T("test_users"))

		// Try to add a JOIN (would need to be done at base level)
		// For now, just test that complex WHERE fails
		complexWhere := astql.And(
			astql.C(astql.F("id"), astql.EQ, astql.P("id1")),
			astql.C(astql.F("name"), astql.LIKE, astql.P("pattern")),
		)
		builder = builder.Where(complexWhere)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		_, err = provider.Render(ast)
		if err == nil {
			t.Error("Expected error for complex WHERE clause")
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
