package astql

import (
	"fmt"

	"github.com/zoobzio/astql/internal/types"
)

// TryT creates a validated table reference, returning an error if invalid.
func TryT(name string, alias ...string) (types.Table, error) {
	// Validate table name against Sentinel registry
	if err := ValidateTable(name); err != nil {
		return types.Table{}, fmt.Errorf("invalid table: %w", err)
	}

	t := types.Table{Name: name}
	if len(alias) > 0 {
		// Enforce single lowercase letter for aliases
		if !isValidTableAlias(alias[0]) {
			return types.Table{}, fmt.Errorf("table alias must be single lowercase letter (a-z), got: %s", alias[0])
		}
		t.Alias = alias[0]
	}
	return t, nil
}

// T creates a validated table reference.
func T(name string, alias ...string) types.Table {
	table, err := TryT(name, alias...)
	if err != nil {
		panic(err)
	}
	return table
}
