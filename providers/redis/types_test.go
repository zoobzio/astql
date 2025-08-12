package redis_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/redis"
)

func TestRedisTypes(t *testing.T) {
	t.Run("Data type constants", func(t *testing.T) {
		// Test that data types are properly defined
		dataTypes := []redis.Type{
			redis.TypeHash,
			redis.TypeSet,
			redis.TypeZSet,
		}

		expectedValues := []string{"HASH", "SET", "ZSET"}

		for i, dt := range dataTypes {
			if string(dt) != expectedValues[i] {
				t.Errorf("Type %v has value %q, expected %q", dt, string(dt), expectedValues[i])
			}
		}
	})

	t.Run("TableConfig structure", func(t *testing.T) {
		config := redis.TableConfig{
			DataType:   redis.TypeHash,
			KeyPattern: "session:{id}",
			IDField:    "id",
		}

		if config.DataType != redis.TypeHash {
			t.Errorf("Expected data type 'HASH', got %q", config.DataType)
		}
		if config.KeyPattern != "session:{id}" {
			t.Errorf("Expected key pattern 'session:{id}', got %q", config.KeyPattern)
		}
		if config.IDField != "id" {
			t.Errorf("Expected ID field 'id', got %q", config.IDField)
		}
	})

	t.Run("NewAST creates AST from base QueryAST", func(t *testing.T) {
		// Setup test models
		astql.SetupTestModels()

		// Create a base AST
		baseBuilder := astql.Select(astql.T("test_sessions"))
		baseAST := baseBuilder.GetAST()

		// Create Redis AST
		redisAST := redis.NewAST(baseAST)

		if redisAST.QueryAST != baseAST {
			t.Error("NewAST should embed the base QueryAST")
		}

		// Verify Redis-specific fields are initialized
		if redisAST.TTLParam != nil {
			t.Error("TTLParam should be nil by default")
		}
		if redisAST.ScoreParam != nil {
			t.Error("ScoreParam should be nil by default")
		}
	})

	t.Run("AST with TTL parameter", func(t *testing.T) {
		// Setup test models
		astql.SetupTestModels()

		baseAST := &astql.QueryAST{
			Operation: astql.OpInsert,
			Target:    astql.T("test_sessions"),
		}

		redisAST := redis.NewAST(baseAST)

		// Set TTL parameter
		ttlParam := astql.P("ttl_seconds")
		redisAST.TTLParam = &ttlParam

		if redisAST.TTLParam == nil {
			t.Fatal("TTLParam should not be nil")
		}

		if redisAST.TTLParam.Name != "ttl_seconds" {
			t.Errorf("Expected TTL param name 'ttl_seconds', got %q", redisAST.TTLParam.Name)
		}
	})

	t.Run("AST with Score parameter for sorted sets", func(t *testing.T) {
		// Setup test models
		astql.SetupTestModels()

		baseAST := &astql.QueryAST{
			Operation: astql.OpInsert,
			Target:    astql.T("test_leaderboard"),
		}

		redisAST := redis.NewAST(baseAST)

		// Set Score parameter
		scoreParam := astql.P("player_score")
		redisAST.ScoreParam = &scoreParam

		if redisAST.ScoreParam == nil {
			t.Fatal("ScoreParam should not be nil")
		}

		if redisAST.ScoreParam.Name != "player_score" {
			t.Errorf("Expected Score param name 'player_score', got %q", redisAST.ScoreParam.Name)
		}
	})

	t.Run("Multiple table configs", func(t *testing.T) {
		// Test different data type configurations
		configs := []redis.TableConfig{
			{
				DataType:   redis.TypeHash,
				KeyPattern: "user:{id}",
				IDField:    "id",
			},
			{
				DataType:   redis.TypeSet,
				KeyPattern: "active_users:{set_id}",
				IDField:    "user_id",
			},
			{
				DataType:   redis.TypeZSet,
				KeyPattern: "leaderboard:{board_id}",
				IDField:    "player_id",
			},
		}

		for _, config := range configs {
			// Each config should maintain its values
			switch config.DataType {
			case redis.TypeHash:
				if config.KeyPattern != "user:{id}" {
					t.Error("Hash config has wrong pattern")
				}
			case redis.TypeSet:
				if config.KeyPattern != "active_users:{set_id}" {
					t.Error("Set config has wrong pattern")
				}
			case redis.TypeZSet:
				if config.KeyPattern != "leaderboard:{board_id}" {
					t.Error("ZSet config has wrong pattern")
				}
			default:
				t.Errorf("Unknown data type: %v", config.DataType)
			}
		}
	})
}
