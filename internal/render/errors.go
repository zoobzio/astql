package render

import "fmt"

// UnsupportedFeatureError indicates a feature not supported by the dialect.
type UnsupportedFeatureError struct {
	Feature string
	Dialect string
	Hint    string
}

func (e UnsupportedFeatureError) Error() string {
	if e.Hint != "" {
		return fmt.Sprintf("%s: %s is not supported: %s", e.Dialect, e.Feature, e.Hint)
	}
	return fmt.Sprintf("%s: %s is not supported", e.Dialect, e.Feature)
}

// NewUnsupportedFeatureError creates a new unsupported feature error.
func NewUnsupportedFeatureError(dialect, feature string, hint ...string) error {
	err := UnsupportedFeatureError{Feature: feature, Dialect: dialect}
	if len(hint) > 0 {
		err.Hint = hint[0]
	}
	return err
}
