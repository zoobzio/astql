package astql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/zoobzio/pipz"
	"github.com/zoobzio/sentinel"
	"github.com/zoobzio/zlog"
)

// Fields are registered automatically when Sentinel extracts metadata from structs.
var validFields = sync.Map{}

// Aliases are registered from the 'alias' struct tag.
var validFieldAliases = sync.Map{}

// Tables are registered automatically when Sentinel extracts metadata from structs.
var validTables = sync.Map{}

// This is called automatically when Sentinel processes structs with db tags.
func RegisterValidFields(fields []string) {
	for _, field := range fields {
		validFields.Store(field, true)
	}
}

// RegisterValidFieldAliases adds field aliases to the allowed list.
func RegisterValidFieldAliases(aliases []string) {
	for _, alias := range aliases {
		validFieldAliases.Store(alias, true)
	}
}

// This is called automatically when Sentinel processes structs.
func RegisterValidTable(tableName string) {
	validTables.Store(tableName, true)
}

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

	// Don't allow aggregate functions through string fields
	// They should use FieldsWithAggregates() instead

	if _, exists := validFields.Load(fieldName); !exists {
		return fmt.Errorf("field '%s' not found - ensure struct is scanned with sentinel.Inspect[T]()", field)
	}
	return nil
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
	if _, exists := validTables.Load(table); !exists {
		return fmt.Errorf("table '%s' not found - ensure struct is scanned with sentinel.Inspect[T]()", table)
	}
	return nil
}

// ValidateTable is the exported version for use by other packages.
func ValidateTable(table string) error {
	return validateTable(table)
}

// validateFieldAlias checks if a field alias is allowed in queries.
func validateFieldAlias(alias string) error {
	if _, exists := validFieldAliases.Load(alias); !exists {
		return fmt.Errorf("field alias '%s' not found - ensure struct has alias tag", alias)
	}
	return nil
}

// ValidateFieldAlias is the exported version for use by other packages.
func ValidateFieldAlias(alias string) error {
	return validateFieldAlias(alias)
}

// and registers them as valid field names for queries.
func extractDBFields(metadata sentinel.ModelMetadata) {
	var dbFields []string
	for _, field := range metadata.Fields {
		if dbTag, exists := field.Tags["db"]; exists && dbTag != "-" {
			// Validate that the db tag is a safe identifier
			if !isValidSQLIdentifier(dbTag) {
				// Log warning and skip malicious field
				fmt.Printf("WARNING: Skipping field %s with unsafe db tag: %s\n", field.Name, dbTag)
				continue
			}
			// Use the db tag value as the field name (e.g. db:"user_id" -> "user_id")
			dbFields = append(dbFields, dbTag)

			// Also process alias tag if present
			if aliasTag, exists := field.Tags["alias"]; exists && aliasTag != "" {
				// Split comma-separated aliases
				aliases := strings.Split(aliasTag, ",")
				for _, alias := range aliases {
					alias = strings.TrimSpace(alias)
					if isValidSQLIdentifier(alias) {
						validFieldAliases.Store(alias, true)
						zlog.Debug("Registered field alias", zlog.String("alias", alias), zlog.String("field", field.Name))
					} else {
						fmt.Printf("WARNING: Skipping unsafe alias '%s' for field %s\n", alias, field.Name)
					}
				}
			}
		}
	}
	if len(dbFields) > 0 {
		RegisterValidFields(dbFields)
	}
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

// and automatically registers valid field names and table names.
var fieldExtractionHook = pipz.Apply[zlog.Event[sentinel.ExtractionEvent]]("astql-fields", func(_ context.Context, event zlog.Event[sentinel.ExtractionEvent]) (zlog.Event[sentinel.ExtractionEvent], error) {
	// Extract field names from the metadata
	extractDBFields(event.Data.Metadata)

	// Extract and register table name from type name
	tableName := typeNameToTableName(event.Data.TypeName)
	// Validate the generated table name
	if isValidSQLIdentifier(tableName) {
		RegisterValidTable(tableName)
	} else {
		fmt.Printf("WARNING: Skipping type %s - generated unsafe table name: %s\n", event.Data.TypeName, tableName)
	}

	return event, nil
})

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

// when structs are scanned.
func init() {
	// Use the typed logger API to hook into METADATA_EXTRACTED events
	sentinel.Logger.Extraction.Hook("METADATA_EXTRACTED", fieldExtractionHook)
}
