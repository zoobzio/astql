package astql

import (
	"strings"
	"testing"
)

func TestP(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid cases
		{"Simple name", "userId", false},
		{"With underscore", "user_id", false},
		{"With numbers", "user123", false},
		{"Mixed case", "UserID", false},
		{"Long name", "very_long_parameter_name_123", false},

		// Invalid cases
		{"Empty string", "", true},
		{"Starts with number", "123user", true},
		{"Starts with underscore", "_user", true},
		{"Contains space", "user id", true},
		{"Contains dash", "user-id", true},
		{"Contains dot", "user.id", true},
		{"SQL injection attempt", "user'; DROP TABLE--", true},
		{"Contains comment", "user/*comment*/", true},
		{"Contains quote", "user'id", true},
		{"Contains double quote", "user\"id", true},
		{"Contains semicolon", "user;id", true},
		{"Contains backslash", "user\\id", true},

		// SQL keywords
		{"SELECT keyword", "select", true},
		{"INSERT keyword", "insert", true},
		{"UPDATE keyword", "update", true},
		{"DELETE keyword", "delete", true},
		{"WHERE keyword", "where", true},
		{"FROM keyword", "from", true},
		{"AND keyword", "and", true},
		{"OR keyword", "or", true},
		{"DROP keyword", "drop", true},
		{"CREATE keyword", "create", true},
		{"ALTER keyword", "alter", true},
		{"TABLE keyword", "table", true},
		{"UNION keyword", "union", true},
		{"JOIN keyword", "join", true},
		{"NULL keyword", "null", true},
		{"TRUE keyword", "true", true},
		{"FALSE keyword", "false", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("P(%q) should have panicked", tt.input)
					}
				}()
				P(tt.input)
			} else {
				param := P(tt.input)
				if param.Name != tt.input {
					t.Errorf("Expected name '%s', got '%s'", tt.input, param.Name)
				}
			}
		})
	}
}

func TestIsValidParamName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// Valid cases
		{"Simple lowercase", "userid", true},
		{"Simple uppercase", "USERID", true},
		{"Mixed case", "userId", true},
		{"With numbers", "user123", true},
		{"With underscore", "user_id", true},
		{"Complex valid", "user_name_123", true},

		// Invalid - empty or bad start
		{"Empty", "", false},
		{"Starts with number", "1user", false},
		{"Starts with underscore", "_user", false},

		// Invalid - special characters
		{"With space", "user id", false},
		{"With dash", "user-id", false},
		{"With dot", "user.id", false},
		{"With semicolon", "user;", false},
		{"With quote", "user'", false},
		{"With double quote", "user\"", false},
		{"With comment start", "user--", false},
		{"With comment block", "user/*", false},
		{"With backslash", "user\\", false},

		// Invalid - SQL keywords (case insensitive)
		{"select lowercase", "select", false},
		{"SELECT uppercase", "SELECT", false},
		{"Select mixed", "Select", false},
		{"where", "where", false},
		{"from", "from", false},
		{"insert", "insert", false},
		{"update", "update", false},
		{"delete", "delete", false},
		{"drop", "drop", false},
		{"create", "create", false},
		{"alter", "alter", false},
		{"table", "table", false},
		{"and", "and", false},
		{"or", "or", false},
		{"not", "not", false},
		{"null", "null", false},
		{"true", "true", false},
		{"false", "false", false},
		{"union", "union", false},
		{"join", "join", false},
		{"having", "having", false},
		{"group", "group", false},
		{"order", "order", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidParamName(tt.input)
			if got != tt.want {
				t.Errorf("isValidParamName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidParamNameCaseInsensitive(t *testing.T) {
	// Test that SQL keywords are rejected regardless of case
	keywords := []string{"SELECT", "select", "SeLeCt", "INSERT", "insert", "WHERE", "where"}

	for _, keyword := range keywords {
		if isValidParamName(keyword) {
			t.Errorf("isValidParamName(%q) should return false for SQL keyword", keyword)
		}
	}
}

func TestPPanicMessage(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := r.(error).Error()
			if !strings.Contains(errMsg, "invalid parameter name") {
				t.Errorf("Expected error message to contain 'invalid parameter name', got: %s", errMsg)
			}
			if !strings.Contains(errMsg, "must be alphanumeric with underscores, starting with letter") {
				t.Errorf("Expected error message to contain validation rules, got: %s", errMsg)
			}
		} else {
			t.Error("Expected P() to panic with invalid input")
		}
	}()

	P("123invalid") // Should panic
}

func TestTryP(t *testing.T) {
	tests := []struct {
		name    string
		errMsg  string
		input   string
		wantErr bool
	}{
		// Valid cases
		{"Simple name", "", "userId", false},
		{"With underscore", "", "user_id", false},
		{"With numbers", "", "user123", false},
		{"Mixed case", "", "UserID", false},

		// Invalid cases
		{"Empty string", "invalid parameter name", "", true},
		{"Starts with number", "invalid parameter name", "123user", true},
		{"SQL keyword", "invalid parameter name", "select", true},
		{"Contains space", "invalid parameter name", "user id", true},
		{"Contains SQL injection", "invalid parameter name", "user'; DROP TABLE--", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			param, err := TryP(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("TryP(%q) expected error, got nil", tt.input)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("TryP(%q) error = %v, want error containing %q", tt.input, err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("TryP(%q) unexpected error: %v", tt.input, err)
				}
				if param.Name != tt.input {
					t.Errorf("TryP(%q) = %v, want %v", tt.input, param.Name, tt.input)
				}
			}
		})
	}
}
