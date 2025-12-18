package render

import (
	"errors"
	"testing"
)

func TestUnsupportedFeatureError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      UnsupportedFeatureError
		expected string
	}{
		{
			name: "without hint",
			err: UnsupportedFeatureError{
				Feature: "DISTINCT ON",
				Dialect: "MySQL",
			},
			expected: "MySQL: DISTINCT ON is not supported",
		},
		{
			name: "with hint",
			err: UnsupportedFeatureError{
				Feature: "RETURNING",
				Dialect: "SQLite",
				Hint:    "use a separate SELECT query",
			},
			expected: "SQLite: RETURNING is not supported: use a separate SELECT query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNewUnsupportedFeatureError(t *testing.T) {
	t.Run("without hint", func(t *testing.T) {
		err := NewUnsupportedFeatureError("MySQL", "DISTINCT ON")
		var ufErr UnsupportedFeatureError
		if !errors.As(err, &ufErr) {
			t.Fatal("expected UnsupportedFeatureError")
		}
		if ufErr.Dialect != "MySQL" {
			t.Errorf("Dialect = %q, want %q", ufErr.Dialect, "MySQL")
		}
		if ufErr.Feature != "DISTINCT ON" {
			t.Errorf("Feature = %q, want %q", ufErr.Feature, "DISTINCT ON")
		}
		if ufErr.Hint != "" {
			t.Errorf("Hint = %q, want empty", ufErr.Hint)
		}
	})

	t.Run("with hint", func(t *testing.T) {
		err := NewUnsupportedFeatureError("SQLite", "RETURNING", "use SELECT instead")
		var ufErr UnsupportedFeatureError
		if !errors.As(err, &ufErr) {
			t.Fatal("expected UnsupportedFeatureError")
		}
		if ufErr.Hint != "use SELECT instead" {
			t.Errorf("Hint = %q, want %q", ufErr.Hint, "use SELECT instead")
		}
	})
}
