package types

// Field represents a validated field reference.
// This is exported from the internal package so providers can use it,
// but external users cannot import this package.
type Field struct {
	Name  string // The field name (required)
	Table string // Optional table/alias prefix
}

// TableValidator is a function that validates table names and aliases.
type TableValidator func(string) error

// Global table validator - set by the main package.
var validateTable TableValidator

// GetName returns the field name.
func (f Field) GetName() string {
	return f.Name
}

// GetTable returns the table/alias prefix.
func (f Field) GetTable() string {
	return f.Table
}

// WithTable sets the table/alias prefix for a field with validation.
func (f Field) WithTable(tableOrAlias string) Field {
	// Validate table/alias
	if validateTable != nil {
		if err := validateTable(tableOrAlias); err != nil {
			panic(err)
		}
	}

	f.Table = tableOrAlias
	return f
}

// SetTableValidator sets the global table validator function.
// This is called by the main package during initialization.
func SetTableValidator(validator TableValidator) {
	validateTable = validator
}
