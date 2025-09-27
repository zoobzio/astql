package astql

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/sentinel"
)

// TryF creates a validated field reference, returning an error if invalid.
func TryF(name string) (types.Field, error) {
	// Validate field name against Sentinel registry
	if err := ValidateField(name); err != nil {
		return types.Field{}, fmt.Errorf("invalid field: %w", err)
	}

	return types.Field{Name: name}, nil
}

// F creates a validated field reference.
func F(name string) types.Field {
	f, err := TryF(name)
	if err != nil {
		panic(err)
	}
	return f
}

// validateTableOrAlias validates both table names and aliases.
// This is used as the validator callback for types.Field.WithTable.
func validateTableOrAlias(tableOrAlias string) error {
	// Must be either:
	// 1. A single lowercase letter (table alias), OR
	// 2. A registered table name
	if isValidTableAlias(tableOrAlias) {
		// It's a valid single-letter alias
		return nil
	}
	if err := validateTable(tableOrAlias); err == nil {
		// It's a valid table name
		return nil
	}
	return fmt.Errorf("WithTable requires single-letter alias (a-z) or valid table name, got: %s", tableOrAlias)
}

// isValidTableAlias checks if a string is a valid single-letter table alias.
func isValidTableAlias(alias string) bool {
	return len(alias) == 1 && alias[0] >= 'a' && alias[0] <= 'z'
}

// No global state - we query sentinel directly

// Returns error if field was not found in any scanned struct.
func validateField(field string) error {
	// Handle SQL expressions with AS aliases like "c.name AS customer_name"
	if asIndex := findAS(field); asIndex != -1 {
		field = field[:asIndex]
	}

	// Handle table aliases like "o.id" by extracting just the field name
	fieldName := field
	if dotIndex := lastDotIndex(field); dotIndex != -1 {
		fieldName = field[dotIndex+1:]
	}

	// Query sentinel's schema directly
	schema := sentinel.Schema()
	for _, metadata := range schema {
		for _, f := range metadata.Fields {
			if dbTag, ok := f.Tags["db"]; ok && dbTag == fieldName {
				return nil // Found it
			}
		}
	}

	return fmt.Errorf("field '%s' not found - ensure struct is scanned with sentinel.Inspect[T]()", field)
}

// findAS finds the position of " AS " in a string.
func findAS(s string) int {
	for i := 0; i < len(s)-3; i++ {
		if s[i] == ' ' && s[i+1] == 'A' && s[i+2] == 'S' && s[i+3] == ' ' {
			return i
		}
	}
	return -1
}

// lastDotIndex finds the last dot in a string.
func lastDotIndex(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}

// ValidateField is the exported version for use by other packages.
func ValidateField(field string) error {
	return validateField(field)
}

// Returns error if table was not found in any scanned struct.
func validateTable(table string) error {
	// Query sentinel's schema directly
	schema := sentinel.Schema()
	for typeName := range schema {
		// Extract struct name from fully qualified type name
		if idx := strings.LastIndex(typeName, "."); idx >= 0 {
			typeName = typeName[idx+1:]
		}
		// Use exact struct name as table name - no conversion
		if typeName == table {
			return nil // Found it
		}
	}

	return fmt.Errorf("table '%s' not found - ensure struct is scanned with sentinel.Inspect[T]()", table)
}

// ValidateTable is the exported version for use by other packages.
func ValidateTable(table string) error {
	return validateTable(table)
}

// validateFieldAlias checks if a field alias is allowed in queries.
func validateFieldAlias(alias string) error {
	// Query sentinel's schema directly for alias tags
	schema := sentinel.Schema()
	for _, metadata := range schema {
		for _, f := range metadata.Fields {
			if aliasTag, ok := f.Tags["alias"]; ok {
				// Split comma-separated aliases
				aliases := strings.Split(aliasTag, ",")
				for _, a := range aliases {
					a = strings.TrimSpace(a)
					if a == alias {
						return nil // Found it
					}
				}
			}
		}
	}

	return fmt.Errorf("field alias '%s' not found - ensure struct has alias tag", alias)
}

// ValidateFieldAlias is the exported version for use by other packages.
func ValidateFieldAlias(alias string) error {
	return validateFieldAlias(alias)
}

// Only allows alphanumeric characters and underscores, must start with letter or underscore.
func isValidSQLIdentifier(s string) bool {
	if s == "" {
		return false
	}

	// Must start with letter or underscore
	first := s[0]
	if !((first >= 'a' && first <= 'z') ||
		(first >= 'A' && first <= 'Z') ||
		first == '_') {
		return false
	}

	// Rest must be alphanumeric or underscore
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_') {
			return false
		}
	}

	// Check for actual SQL injection patterns, not just keywords
	// We want to allow legitimate names like "updated_at", "deleted_at", "created_by"
	lower := strings.ToLower(s)

	// Look for SQL injection indicators
	suspiciousPatterns := []string{
		";",     // Statement separator
		"--",    // SQL comment
		"/*",    // SQL comment start
		"*/",    // SQL comment end
		"'",     // Single quote
		"\"",    // Double quote
		"`",     // Backtick
		"\\",    // Escape character
		" or ",  // SQL OR with spaces
		" and ", // SQL AND with spaces
		"drop table",
		"delete from",
		"insert into",
		"update set",
		"select ",
		"union all",
		"union select",
	}

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	// Also reject if it contains spaces (except for checking specific patterns above)
	if strings.Contains(s, " ") {
		return false
	}

	return true
}

// e.g., "User" -> "users", "OrderItem" -> "order_items".
func typeNameToTableName(typeName string) string {
	// Simple pluralization - add 's' to lowercase
	// In a real system, you might want more sophisticated pluralization
	// or use a struct tag to specify the table name
	result := ""
	for i, ch := range typeName {
		if i > 0 && ch >= 'A' && ch <= 'Z' {
			// Add underscore before uppercase letters (except first)
			result += "_"
		}
		result += string(ch)
	}

	// Convert to lowercase and add 's' for simple pluralization
	result = strings.ToLower(result) + "s"

	return result
}

// Set up table validator on init.
func init() {
	// Set up the table validator for types.Field.WithTable
	types.SetTableValidator(validateTableOrAlias)
}
