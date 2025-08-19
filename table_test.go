package astql

import (
	"strings"
	"testing"
)

func TestTable(t *testing.T) {
	// Register test structs
	SetupTest(t)

	t.Run("Valid table without alias", func(t *testing.T) {
		table := T("users")
		if table.Name != "users" {
			t.Errorf("Expected table name 'users', got '%s'", table.Name)
		}
		if table.Alias != "" {
			t.Errorf("Expected empty alias, got '%s'", table.Alias)
		}
	})

	t.Run("Valid table with alias", func(t *testing.T) {
		table := T("users", "u")
		if table.Name != "users" {
			t.Errorf("Expected table name 'users', got '%s'", table.Name)
		}
		if table.Alias != "u" {
			t.Errorf("Expected alias 'u', got '%s'", table.Alias)
		}
	})

	t.Run("Multiple aliases only use first", func(t *testing.T) {
		table := T("users", "u", "x", "y")
		if table.Alias != "u" {
			t.Errorf("Expected alias 'u', got '%s'", table.Alias)
		}
	})
}

func TestTInvalidCases(t *testing.T) {
	// Register test structs
	SetupTest(t)

	//nolint:govet // field alignment not needed for test structs
	tests := []struct {
		name      string
		tableName string
		alias     []string
		wantErr   string
	}{
		{
			"Invalid table name",
			"invalid_table",
			nil,
			"invalid table",
		},
		{
			"Empty table name",
			"",
			nil,
			"invalid table",
		},
		{
			"Invalid alias - uppercase",
			"users",
			[]string{"U"},
			"table alias must be single lowercase letter",
		},
		{
			"Invalid alias - number",
			"users",
			[]string{"1"},
			"table alias must be single lowercase letter",
		},
		{
			"Invalid alias - special char",
			"users",
			[]string{"!"},
			"table alias must be single lowercase letter",
		},
		{
			"Invalid alias - multiple letters",
			"users",
			[]string{"ab"},
			"table alias must be single lowercase letter",
		},
		{
			"Invalid alias - empty",
			"users",
			[]string{""},
			"table alias must be single lowercase letter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					errMsg := r.(error).Error()
					if !strings.Contains(errMsg, tt.wantErr) {
						t.Errorf("Expected error containing '%s', got '%s'", tt.wantErr, errMsg)
					}
				} else {
					t.Errorf("Expected panic for %s", tt.name)
				}
			}()
			T(tt.tableName, tt.alias...)
		})
	}
}

func TestTryT(t *testing.T) {
	// Register test structs
	SetupTest(t)

	t.Run("Valid cases", func(t *testing.T) {
		//nolint:govet // field alignment not needed for test structs
		tests := []struct {
			name      string
			tableName string
			alias     []string
			wantName  string
			wantAlias string
		}{
			{"Without alias", "users", nil, "users", ""},
			{"With valid alias", "users", []string{"u"}, "users", "u"},
			{"Valid single letter aliases", "users", []string{"a"}, "users", "a"},
			{"Another valid alias", "users", []string{"z"}, "users", "z"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				table, err := TryT(tt.tableName, tt.alias...)
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if table.Name != tt.wantName {
					t.Errorf("Expected name '%s', got '%s'", tt.wantName, table.Name)
				}
				if table.Alias != tt.wantAlias {
					t.Errorf("Expected alias '%s', got '%s'", tt.wantAlias, table.Alias)
				}
			})
		}
	})

	t.Run("Invalid cases", func(t *testing.T) {
		//nolint:govet // field alignment not needed for test structs
		tests := []struct {
			name      string
			tableName string
			alias     []string
			wantErr   string
		}{
			{"Invalid table", "invalid_table", nil, "invalid table"},
			{"Invalid alias uppercase", "users", []string{"U"}, "table alias must be single lowercase letter"},
			{"Invalid alias multi-char", "users", []string{"usr"}, "table alias must be single lowercase letter"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := TryT(tt.tableName, tt.alias...)
				if err == nil {
					t.Error("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.wantErr, err.Error())
				}
			})
		}
	})
}

func TestIsValidTableAlias(t *testing.T) {
	tests := []struct {
		name  string
		alias string
		want  bool
	}{
		// Valid cases
		{"Single lowercase a", "a", true},
		{"Single lowercase z", "z", true},
		{"Single lowercase m", "m", true},

		// Invalid cases
		{"Uppercase letter", "A", false},
		{"Number", "1", false},
		{"Special character", "!", false},
		{"Multiple letters", "ab", false},
		{"Empty string", "", false},
		{"Space", " ", false},
		{"Underscore", "_", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidTableAlias(tt.alias); got != tt.want {
				t.Errorf("isValidTableAlias(%q) = %v, want %v", tt.alias, got, tt.want)
			}
		})
	}
}
