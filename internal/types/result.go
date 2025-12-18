package types

// QueryResult contains the rendered SQL and required parameters.
type QueryResult struct {
	SQL            string
	RequiredParams []string
}
