package redis_test

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/redis"
)

func ExampleProvider() {
	// Setup
	astql.SetupTestModels()
	provider := redis.NewProvider()

	// Configure how tables map to Redis data structures
	provider.RegisterTable("test_users", redis.TableConfig{
		KeyPattern: "users:{id}",   // Redis key pattern
		DataType:   redis.TypeHash, // Store as Redis Hash
		IDField:    "id",           // Field containing the key
	})

	provider.RegisterTable("test_sessions", redis.TableConfig{
		KeyPattern: "session:{id}",
		DataType:   redis.TypeString, // Store as simple key-value
		IDField:    "id",
		// TTL is now handled via WithTTL() method
	})

	// Example 1: SELECT from Hash
	query1 := redis.Select(astql.T("test_users")).
		Fields(astql.F("name"), astql.F("email")).
		Where(astql.C(astql.F("id"), astql.EQ, astql.P("user_id")))

	ast1, _ := query1.Build()
	result1, _ := provider.Render(ast1)
	fmt.Println("Hash GET:", result1.SQL)

	// Example 2: INSERT into Hash
	query2 := redis.Insert(astql.T("test_users")).
		Values(map[astql.Field]astql.Param{
			astql.F("id"):    astql.P("user_id"),
			astql.F("name"):  astql.P("name"),
			astql.F("email"): astql.P("email"),
		})

	ast2, _ := query2.Build()
	result2, _ := provider.Render(ast2)
	fmt.Println("Hash SET:", contains(result2.SQL, "HSET"))

	// Example 3: INSERT with TTL
	query3 := redis.Insert(astql.T("test_sessions")).
		Values(map[astql.Field]astql.Param{
			astql.F("id"):   astql.P("session_id"),
			astql.F("data"): astql.P("session_data"),
		}).
		WithTTL(astql.P("ttl")) // Safe TTL parameter

	ast3, _ := query3.Build()
	result3, _ := provider.Render(ast3)
	fmt.Println("String SET with TTL:", contains(result3.SQL, "SETEX"))

	// Example 4: Pub/Sub operations
	listenQuery := redis.Listen(astql.T("test_users"))
	listenAst, _ := listenQuery.Build()
	listenResult, _ := provider.Render(listenAst)
	fmt.Println("Subscribe:", listenResult.SQL)

	notifyQuery := redis.Notify(astql.T("test_users"), astql.P("message"))
	notifyAst, _ := notifyQuery.Build()
	notifyResult, _ := provider.Render(notifyAst)
	fmt.Println("Publish:", notifyResult.SQL)

	// Output:
	// Hash GET: HMGET users::user_id name email
	// Hash SET: true
	// String SET with TTL: true
	// Subscribe: SUBSCRIBE test_users_changes
	// Publish: PUBLISH test_users_changes :message
}

func ExampleProvider_differentDataTypes() {
	// Setup
	astql.SetupTestModels()
	provider := redis.NewProvider()

	// Configure different Redis data types
	provider.RegisterTable("test_active_users", redis.TableConfig{
		KeyPattern: "active_users", // Single set for all active users
		DataType:   redis.TypeSet,
		IDField:    "user_id",
	})

	provider.RegisterTable("test_leaderboards", redis.TableConfig{
		KeyPattern: "leaderboard", // Single sorted set
		DataType:   redis.TypeZSet,
		IDField:    "user_id",
		// Score is now handled via WithScore() method
	})

	// Example: Query a sorted set with pagination
	query := redis.Select(astql.T("test_leaderboards")).
		Limit(10). // Top 10
		Offset(0)  // Starting from rank 0

	ast, _ := query.Build()
	result, _ := provider.Render(ast)
	fmt.Println("Sorted Set Range:", result.SQL)

	// Output:
	// Sorted Set Range: ZRANGE leaderboard 0 9 WITHSCORES
}

func ExampleProvider_sortedSetWithScore() {
	// Setup
	astql.SetupTestModels()
	provider := redis.NewProvider()

	provider.RegisterTable("test_leaderboards", redis.TableConfig{
		KeyPattern: "game:leaderboard",
		DataType:   redis.TypeZSet,
		IDField:    "user_id",
	})

	// Example: Insert player score into sorted set
	query := redis.Insert(astql.T("test_leaderboards")).
		Values(map[astql.Field]astql.Param{
			astql.F("user_id"): astql.P("player"),
		}).
		WithScore(astql.P("score")) // Safe score parameter

	ast, _ := query.Build()
	result, _ := provider.Render(ast)
	fmt.Println("Sorted Set Add:", result.SQL)
	fmt.Println("Required Params:", result.RequiredParams)

	// Output:
	// Sorted Set Add: ZADD game:leaderboard 1 :player
	// Required Params: [player score]
}
