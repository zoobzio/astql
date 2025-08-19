package astql

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql/internal/types"
)

// TryP creates a validated parameter reference, returning an error if invalid.
func TryP(name string) (types.Param, error) {
	// Validate parameter name
	if !isValidParamName(name) {
		return types.Param{}, fmt.Errorf("invalid parameter name '%s': must be alphanumeric with underscores, starting with letter", name)
	}

	return types.Param{Name: name}, nil
}

// P is the primary way to reference user values in queries.
func P(name string) types.Param {
	p, err := TryP(name)
	if err != nil {
		panic(err)
	}
	return p
}

// Only allows alphanumeric characters and underscores, must start with letter.
func isValidParamName(name string) bool {
	if name == "" {
		return false
	}

	// Must start with letter (not underscore for params)
	first := name[0]
	if !((first >= 'a' && first <= 'z') ||
		(first >= 'A' && first <= 'Z')) {
		return false
	}

	// Rest must be alphanumeric or underscore
	for i := 1; i < len(name); i++ {
		ch := name[i]
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_') {
			return false
		}
	}

	// Check for suspicious patterns in parameter names
	lower := strings.ToLower(name)

	// Reject SQL keywords that could be confusing
	sqlKeywords := []string{
		"select", "insert", "update", "delete", "drop",
		"create", "alter", "table", "from", "where",
		"and", "or", "not", "null", "true", "false",
		"union", "join", "having", "group", "order",
	}

	for _, keyword := range sqlKeywords {
		if lower == keyword {
			return false
		}
	}

	// Also reject if it contains any special SQL patterns
	suspiciousPatterns := []string{
		"--", "/*", "*/", "'", "\"", ";", "\\",
	}

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(name, pattern) {
			return false
		}
	}

	return true
}
