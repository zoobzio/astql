package astql

import (
	"strings"
	"testing"

	"github.com/zoobzio/sentinel"
)

func TestField(t *testing.T) {
	// Register test structs
	SetupTest(t)

	t.Run("Valid field creation", func(t *testing.T) {
		field := F("id")
		if field.Name != "id" {
			t.Errorf("Expected field name 'id', got '%s'", field.Name)
		}
		if field.Table != "" {
			t.Errorf("Expected empty table, got '%s'", field.Table)
		}
	})

	t.Run("Field with table", func(t *testing.T) {
		field := F("id").WithTable("u")
		if field.Name != "id" {
			t.Errorf("Expected field name 'id', got '%s'", field.Name)
		}
		if field.Table != "u" {
			t.Errorf("Expected table 'u', got '%s'", field.Table)
		}
	})

	t.Run("Field with alias", func(t *testing.T) {
		field := F("id").WithTable("u")
		if field.Name != "id" {
			t.Errorf("Expected field name 'id', got '%s'", field.Name)
		}
		if field.Table != "u" {
			t.Errorf("Expected table alias 'u', got '%s'", field.Table)
		}
	})
}

func TestFInvalidCases(t *testing.T) {
	// Register test structs
	SetupTest(t)

	defer func() {
		if r := recover(); r != nil {
			errMsg := r.(error).Error()
			if !strings.Contains(errMsg, "invalid field") {
				t.Errorf("Expected 'invalid field' error, got '%s'", errMsg)
			}
		} else {
			t.Error("Expected panic for invalid field")
		}
	}()

	F("unknown_field")
}

func TestWithTableInvalidCases(t *testing.T) {
	// Register test structs
	SetupTest(t)

	tests := []struct {
		name      string
		tableArg  string
		wantPanic string
	}{
		{
			"Invalid alias - uppercase",
			"U",
			"WithTable requires single-letter alias",
		},
		{
			"Invalid alias - multiple letters",
			"usr",
			"WithTable requires single-letter alias",
		},
		{
			"Invalid table name",
			"unknown_table",
			"WithTable requires single-letter alias",
		},
		{
			"Empty string",
			"",
			"WithTable requires single-letter alias",
		},
		{
			"Number",
			"1",
			"WithTable requires single-letter alias",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					errMsg := r.(error).Error()
					if !strings.Contains(errMsg, tt.wantPanic) {
						t.Errorf("Expected error containing '%s', got '%s'", tt.wantPanic, errMsg)
					}
				} else {
					t.Errorf("Expected panic for %s", tt.name)
				}
			}()
			F("id").WithTable(tt.tableArg)
		})
	}
}

func TestTryF(t *testing.T) {
	// Register test structs
	SetupTest(t)

	t.Run("Valid field", func(t *testing.T) {
		field, err := TryF("id")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if field.Name != "id" {
			t.Errorf("Expected field name 'id', got '%s'", field.Name)
		}
	})

	t.Run("Invalid field", func(t *testing.T) {
		_, err := TryF("unknown_field")
		if err == nil {
			t.Error("Expected error for unknown field")
		}
		if !strings.Contains(err.Error(), "invalid field") {
			t.Errorf("Expected 'invalid field' error, got '%v'", err)
		}
	})
}

func TestValidateField(t *testing.T) {
	// Register test structs for more comprehensive validation testing
	SetupTest(t)

	// Define a struct with various field names
	type TestStruct struct {
		Name         string `db:"name"`
		CustomerName string `db:"customer_name"`
		ID           int    `db:"id"`
		OrderID      int    `db:"order_id"`
	}

	// The SetupTest above should have initialized sentinel
	// Just inspect the struct - no registration needed
	sentinel.Inspect[TestStruct]()

	tests := []struct {
		name    string
		field   string
		wantErr bool
	}{
		// Valid cases
		{"Simple field", "id", false},
		{"Field with underscore", "customer_name", false},
		{"Field with table prefix", "c.name", false},
		{"Field with AS alias", "c.name AS customer_name", false},

		// Invalid cases - these won't be registered by sentinel
		{"Unknown field", "unknown", true},
		{"Empty field", "", true},
		{"Field with invalid prefix", "x.unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateField(tt.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateField(%q) error = %v, wantErr %v", tt.field, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTable(t *testing.T) {
	// Register test structs - this will register tables users, orders, products
	SetupTest(t)

	tests := []struct {
		name    string
		table   string
		wantErr bool
	}{
		{"Valid table", "User", false},
		{"Another valid table", "Order", false},
		{"Unknown table", "unknown", true},
		{"Empty table", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTable(tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTable(%q) error = %v, wantErr %v", tt.table, err, tt.wantErr)
			}
		})
	}
}

func TestValidateFieldAlias(t *testing.T) {
	// This test is no longer needed as field aliases are not separately tracked
	// They are validated as part of field validation
	t.Skip("Field aliases are validated as part of field validation")
}

func TestIsValidSQLIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// Valid cases
		{"Simple lowercase", "userid", true},
		{"Simple uppercase", "USERID", true},
		{"Mixed case", "userId", true},
		{"With underscore", "user_id", true},
		{"With numbers", "user123", true},
		{"Starting with underscore", "_user", true},
		{"Complex valid", "user_name_123", true},
		{"Common field names", "created_at", true},
		{"Updated field", "updated_at", true},
		{"Deleted field", "deleted_by", true},

		// Invalid cases
		{"Empty", "", false},
		{"Starts with number", "1user", false},
		{"Contains space", "user id", false},
		{"Contains semicolon", "user;", false},
		{"Contains SQL comment", "user--comment", false},
		{"Contains quote", "user'", false},
		{"SQL injection attempt", "'; DROP TABLE--", false},
		{"Contains OR with spaces", "user or 1=1", false},
		{"Drop table attempt", "drop table users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidSQLIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("isValidSQLIdentifier(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFindAS(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"With AS", "field AS alias", 5},
		{"Without AS", "field", -1},
		{"Multiple AS", "field AS alias AS other", 5},
		{"AS at start", " AS alias", 0},
		{"Lowercase as", "field as alias", -1},
		{"AS without spaces", "fieldASalias", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findAS(tt.input)
			if got != tt.want {
				t.Errorf("findAS(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestLastDotIndex(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"With single dot", "table.field", 5},
		{"Multiple dots", "schema.table.field", 12},
		{"No dot", "field", -1},
		{"Dot at end", "field.", 5},
		{"Dot at start", ".field", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lastDotIndex(tt.input)
			if got != tt.want {
				t.Errorf("lastDotIndex(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestTypeNameToTableName(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		want     string
	}{
		{"Simple type", "User", "users"},
		{"Camel case", "OrderItem", "order_items"},
		{"Multiple words", "CustomerOrderDetail", "customer_order_details"},
		{"Single letter", "A", "as"},
		{"All caps", "API", "a_p_is"},
		{"With number", "User2", "user2s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := typeNameToTableName(tt.typeName)
			if got != tt.want {
				t.Errorf("typeNameToTableName(%q) = %v, want %v", tt.typeName, got, tt.want)
			}
		})
	}
}
