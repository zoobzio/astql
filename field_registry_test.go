package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

func TestFieldAliases(t *testing.T) {
	// Setup test models - this registers aliases
	astql.SetupTestModels()

	t.Run("ValidateFieldAlias accepts registered aliases", func(t *testing.T) {
		// Test valid aliases from TestUser
		validAliases := []struct {
			alias     string
			wantField string
		}{
			{"user_name", "name"},
			{"full_name", "name"},
			{"email_address", "email"},
			{"user_id", "id"},
			{"user_age", "age"},
			{"status", "active"},
		}

		for _, tc := range validAliases {
			err := astql.ValidateFieldAlias(tc.alias)
			if err != nil {
				t.Errorf("ValidateFieldAlias(%q) failed: %v", tc.alias, err)
			}
		}
	})

	t.Run("ValidateFieldAlias rejects unknown aliases", func(t *testing.T) {
		invalidAliases := []string{
			"unknown_alias",
			"not_registered",
			"fake_field",
			"",
		}

		for _, alias := range invalidAliases {
			err := astql.ValidateFieldAlias(alias)
			if err == nil {
				t.Errorf("ValidateFieldAlias(%q) should have failed", alias)
			}
		}
	})

	t.Run("RegisterValidFieldAliases adds new aliases", func(t *testing.T) {
		// Register a new alias
		aliases := []string{"custom_alias"}

		astql.RegisterValidFieldAliases(aliases)

		// Should now be valid
		err := astql.ValidateFieldAlias("custom_alias")
		if err != nil {
			t.Errorf("ValidateFieldAlias failed after registration: %v", err)
		}
	})

	t.Run("Field with alias validates correctly", func(t *testing.T) {
		// Using an alias should work
		err := astql.ValidateField("user_name")
		if err != nil {
			t.Errorf("ValidateField with alias failed: %v", err)
		}
	})
}
