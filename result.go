package astql

// QueryResult contains the rendered SQL query and required parameters.
type QueryResult struct {
	SQL            string
	RequiredParams []string
}
