package types

// Table represents a validated table reference.
// This is exported from the internal package so providers can use it,
// but external users cannot import this package.
type Table struct {
	Name  string
	Alias string
}

// GetName returns the table name.
func (t Table) GetName() string {
	return t.Name
}

// GetAlias returns the table alias.
func (t Table) GetAlias() string {
	return t.Alias
}
