package types

// Param represents a parameter reference in a query.
// All parameters are named parameters.
// This is exported from the internal package so providers can use it,
// but external users cannot import this package.
type Param struct {
	Name string
}

// GetName returns the parameter name.
func (p Param) GetName() string {
	return p.Name
}
