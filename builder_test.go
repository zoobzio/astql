package astql_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/zoobzio/astql"
)

func TestBasicBuilder(t *testing.T) {
	// Setup test models for field validation
	astql.SetupTestModels()

	t.Run("SELECT query", func(t *testing.T) {
		query := astql.Select(astql.T("test_users")).
			Fields(astql.F("id"), astql.F("name"), astql.F("email")).
			Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			OrderBy(astql.F("name"), astql.ASC).
			Limit(10).
			MustBuild()

		if query.Operation != astql.OpSelect {
			t.Errorf("Expected SELECT operation, got %s", query.Operation)
		}

		if len(query.Fields) != 3 {
			t.Errorf("Expected 3 fields, got %d", len(query.Fields))
		}

		if query.Target.Name != "test_users" {
			t.Errorf("Expected table 'test_users', got '%s'", query.Target.Name)
		}
	})

	t.Run("SELECT * query", func(t *testing.T) {
		// Not calling Fields() means SELECT *
		query := astql.Select(astql.T("test_users")).
			Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			MustBuild()

		if query.Operation != astql.OpSelect {
			t.Errorf("Expected SELECT operation, got %s", query.Operation)
		}

		if query.Fields != nil {
			t.Errorf("Expected nil fields for SELECT *, got %d fields", len(query.Fields))
		}
	})

	t.Run("INSERT query", func(t *testing.T) {
		query := astql.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"):  astql.P("name"),
				astql.F("email"): astql.P("email"),
				astql.F("age"):   astql.P("age"),
			}).
			MustBuild()

		if query.Operation != astql.OpInsert {
			t.Errorf("Expected INSERT operation, got %s", query.Operation)
		}

		if len(query.Values) != 1 {
			t.Errorf("Expected 1 value set, got %d", len(query.Values))
		}

		if len(query.Values[0]) != 3 {
			t.Errorf("Expected 3 fields in value set, got %d", len(query.Values[0]))
		}
	})

	t.Run("UPDATE query", func(t *testing.T) {
		query := astql.Update(astql.T("test_users")).
			Set(astql.F("name"), astql.P("newName")).
			Set(astql.F("email"), astql.P("newEmail")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			MustBuild()

		if query.Operation != astql.OpUpdate {
			t.Errorf("Expected UPDATE operation, got %s", query.Operation)
		}

		if len(query.Updates) != 2 {
			t.Errorf("Expected 2 updates, got %d", len(query.Updates))
		}
	})

	t.Run("DELETE query", func(t *testing.T) {
		query := astql.Delete(astql.T("test_users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("userId"))).
			MustBuild()

		if query.Operation != astql.OpDelete {
			t.Errorf("Expected DELETE operation, got %s", query.Operation)
		}
	})

	t.Run("Complex WHERE with AND/OR", func(t *testing.T) {
		query := astql.Select(astql.T("test_users")).
			Where(
				astql.Or(
					astql.C(astql.F("age"), astql.GT, astql.P("minAge")),
					astql.And(
						astql.C(astql.F("name"), astql.LIKE, astql.P("namePattern")),
						astql.C(astql.F("email"), astql.NotLike, astql.P("emailPattern")),
					),
				),
			).
			MustBuild()

		if query.WhereClause == nil {
			t.Fatal("Expected WHERE clause")
		}

		// Check it's an OR group at the top level
		group, ok := query.WhereClause.(astql.ConditionGroup)
		if !ok {
			t.Fatal("Expected ConditionGroup at top level")
		}

		if group.Logic != astql.OR {
			t.Errorf("Expected OR logic, got %s", group.Logic)
		}

		if len(group.Conditions) != 2 {
			t.Errorf("Expected 2 conditions in OR group, got %d", len(group.Conditions))
		}
	})

	t.Run("Invalid field panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid field")
			}
		}()

		// This should panic because "invalid_field" is not registered
		astql.F("invalid_field")
	})

	t.Run("Invalid table panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid table")
			}
		}()

		// This should panic because "invalid_table" is not registered
		astql.T("invalid_table")
	})

	t.Run("COUNT query", func(t *testing.T) {
		// Simple count
		query := astql.Count(astql.T("test_users")).
			MustBuild()

		if query.Operation != astql.OpCount {
			t.Errorf("Expected COUNT operation, got %s", query.Operation)
		}

		// Count with WHERE
		query2 := astql.Count(astql.T("test_users")).
			Where(astql.C(astql.F("age"), astql.GT, astql.P("minAge"))).
			MustBuild()

		if query2.WhereClause == nil {
			t.Error("Expected WHERE clause in COUNT query")
		}
	})

	t.Run("LISTEN query", func(t *testing.T) {
		query := astql.Listen(astql.T("test_users")).
			MustBuild()

		if query.Operation != astql.OpListen {
			t.Errorf("Expected LISTEN operation, got %s", query.Operation)
		}

		if query.Target.Name != "test_users" {
			t.Errorf("Expected table 'test_users', got '%s'", query.Target.Name)
		}
	})

	t.Run("NOTIFY query", func(t *testing.T) {
		query := astql.Notify(astql.T("test_users"), astql.P("payload")).
			MustBuild()

		if query.Operation != astql.OpNotify {
			t.Errorf("Expected NOTIFY operation, got %s", query.Operation)
		}

		if query.NotifyPayload == nil {
			t.Fatal("Expected notify payload")
		}

		if query.NotifyPayload.Name != "payload" {
			t.Errorf("Expected payload param name 'payload', got '%s'", query.NotifyPayload.Name)
		}
	})

	t.Run("UNLISTEN query", func(t *testing.T) {
		query := astql.Unlisten(astql.T("test_users")).
			MustBuild()

		if query.Operation != astql.OpUnlisten {
			t.Errorf("Expected UNLISTEN operation, got %s", query.Operation)
		}

		if query.Target.Name != "test_users" {
			t.Errorf("Expected table 'test_users', got '%s'", query.Target.Name)
		}
	})

	t.Run("Builder accessor methods", func(t *testing.T) {
		builder := astql.Select(astql.T("test_users"))

		// Test GetAST
		ast := builder.GetAST()
		if ast == nil {
			t.Fatal("GetAST returned nil")
		}
		if ast.Operation != astql.OpSelect {
			t.Errorf("Expected SELECT operation, got %s", ast.Operation)
		}

		// Test GetError (should be nil for valid query)
		if err := builder.GetError(); err != nil {
			t.Errorf("GetError returned unexpected error: %v", err)
		}

		// Test SetError
		testErr := fmt.Errorf("test error")
		builder.SetError(testErr)
		if !errors.Is(builder.GetError(), testErr) {
			t.Error("SetError did not set the error correctly")
		}

		// Build should return the set error
		_, err := builder.Build()
		if !errors.Is(err, testErr) {
			t.Errorf("Expected Build to return set error, got: %v", err)
		}
	})

	t.Run("WhereField method", func(t *testing.T) {
		// Test with valid conditions
		query := astql.Select(astql.T("test_users")).
			WhereField(astql.F("age"), astql.GT, astql.P("min_age")).
			MustBuild()

		if query.WhereClause == nil {
			t.Error("Expected WHERE clause")
		}

		// Test with AND combination
		query2 := astql.Select(astql.T("test_users")).
			WhereField(astql.F("age"), astql.GT, astql.P("min_age")).
			WhereField(astql.F("age"), astql.LT, astql.P("max_age")).
			MustBuild()

		// Should create AND group
		group, ok := query2.WhereClause.(astql.ConditionGroup)
		if !ok {
			t.Fatal("Expected ConditionGroup for multiple WhereField calls")
		}
		if group.Logic != astql.AND {
			t.Errorf("Expected AND logic, got %s", group.Logic)
		}
	})

	t.Run("Offset method", func(t *testing.T) {
		query := astql.Select(astql.T("test_users")).
			Limit(10).
			Offset(20).
			MustBuild()

		if query.Offset == nil {
			t.Fatal("Expected offset to be set")
		}

		if *query.Offset != 20 {
			t.Errorf("Expected offset 20, got %d", *query.Offset)
		}
	})

	t.Run("Multiple Values sets", func(t *testing.T) {
		// Test multiple value sets for batch insert
		builder := astql.Insert(astql.T("test_users")).
			Values(map[astql.Field]astql.Param{
				astql.F("name"): astql.P("name1"),
				astql.F("age"):  astql.P("age1"),
			}).
			Values(map[astql.Field]astql.Param{
				astql.F("name"): astql.P("name2"),
				astql.F("age"):  astql.P("age2"),
			})

		query := builder.MustBuild()

		if len(query.Values) != 2 {
			t.Errorf("Expected 2 value sets, got %d", len(query.Values))
		}
	})
}
