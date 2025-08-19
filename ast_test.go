package astql

import (
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestDirectionConstants(t *testing.T) {
	tests := []struct {
		name      string
		direction types.Direction
		expected  string
	}{
		{"Ascending", types.ASC, "ASC"},
		{"Descending", types.DESC, "DESC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.direction) != tt.expected {
				t.Errorf("Expected %s to be '%s', got '%s'", tt.name, tt.expected, string(tt.direction))
			}
		})
	}
}

func TestOrderBy(t *testing.T) {
	// Register test structs
	SetupTest(t)

	// Create a field for testing
	field := F("name")

	tests := []struct {
		name      string
		orderBy   types.OrderBy
		wantField string
		wantDir   types.Direction
	}{
		{
			name: "OrderBy with ASC",
			orderBy: types.OrderBy{
				Field:     field,
				Direction: types.ASC,
			},
			wantField: "name",
			wantDir:   types.ASC,
		},
		{
			name: "OrderBy with DESC",
			orderBy: types.OrderBy{
				Field:     field,
				Direction: types.DESC,
			},
			wantField: "name",
			wantDir:   types.DESC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.orderBy.Field.Name != tt.wantField {
				t.Errorf("Expected field name '%s', got '%s'", tt.wantField, tt.orderBy.Field.Name)
			}
			if tt.orderBy.Direction != tt.wantDir {
				t.Errorf("Expected direction '%s', got '%s'", tt.wantDir, tt.orderBy.Direction)
			}
		})
	}
}

func TestQueryAST(t *testing.T) {
	t.Run("Initialize empty QueryAST", func(t *testing.T) {
		ast := &types.QueryAST{}

		if ast.Operation != "" {
			t.Error("Expected empty Operation")
		}
		if ast.Fields != nil {
			t.Error("Expected nil Fields slice")
		}
		if ast.WhereClause != nil {
			t.Error("Expected nil WhereClause")
		}
		if ast.Limit != nil {
			t.Error("Expected nil Limit")
		}
		if ast.Offset != nil {
			t.Error("Expected nil Offset")
		}
		if ast.Updates != nil {
			t.Error("Expected nil Updates map")
		}
		if ast.Values != nil {
			t.Error("Expected nil Values slice")
		}
		if ast.NotifyPayload != nil {
			t.Error("Expected nil NotifyPayload")
		}
	})

	t.Run("QueryAST with values", func(t *testing.T) {
		// Register test structs
		SetupTest(t)

		limit := 10
		offset := 20
		field := F("name")
		param := P("test_param")

		ast := &types.QueryAST{
			Operation: types.OpSelect,
			Target:    T("users"), // Use a registered table
			Fields:    []types.Field{field},
			Limit:     &limit,
			Offset:    &offset,
			Updates:   map[types.Field]types.Param{field: param},
		}

		if ast.Operation != types.OpSelect {
			t.Errorf("Expected OpSelect, got %s", ast.Operation)
		}
		if ast.Target.Name != "users" {
			t.Errorf("Expected target 'users', got '%s'", ast.Target.Name)
		}
		if len(ast.Fields) != 1 {
			t.Errorf("Expected 1 field, got %d", len(ast.Fields))
		}
		if *ast.Limit != 10 {
			t.Errorf("Expected limit 10, got %d", *ast.Limit)
		}
		if *ast.Offset != 20 {
			t.Errorf("Expected offset 20, got %d", *ast.Offset)
		}
		if len(ast.Updates) != 1 {
			t.Errorf("Expected 1 update, got %d", len(ast.Updates))
		}
	})
}
