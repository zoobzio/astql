package astql

import (
	"fmt"
	"strings"
)

// ParamType represents the type of parameter reference.
type ParamType int

const (
	ParamNamed ParamType = iota
	ParamPositional
)

// Param represents a parameter reference in a query.
type Param struct {
	Name  string
	Type  ParamType
	Index int
}

// This is the primary way to reference user values in queries.
func P(name string) Param {
	// Validate parameter name
	if !isValidParamName(name) {
		panic(fmt.Errorf("invalid parameter name '%s': must be alphanumeric with underscores, starting with letter", name))
	}

	return Param{
		Type: ParamNamed,
		Name: name,
	}
}

// P1, P2, etc. are shortcuts for positional parameters.
func P1() Param { return Param{Type: ParamPositional, Index: 1} }
func P2() Param { return Param{Type: ParamPositional, Index: 2} }
func P3() Param { return Param{Type: ParamPositional, Index: 3} }
func P4() Param { return Param{Type: ParamPositional, Index: 4} }
func P5() Param { return Param{Type: ParamPositional, Index: 5} }

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
