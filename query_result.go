package astql

import (
	"strings"

	"github.com/zoobzio/sentinel"
)

// QueryResult contains the rendered SQL query and metadata about its execution.
type QueryResult struct {
	Metadata       QueryMetadata
	SQL            string
	RequiredParams []string
}

// QueryMetadata provides information about what the query operates on.
type QueryMetadata struct {
	Operation      Operation
	ResultType     ResultType
	ScanType       string
	Table          TableMetadata
	ReturnedFields []FieldMetadata
	ModifiedFields []FieldMetadata
}

// TableMetadata contains information about a table in the query.
type TableMetadata struct {
	Name     string
	Alias    string
	TypeName string          // The Go type name this table represents
	Fields   []FieldMetadata // All fields in this table
}

// FieldMetadata contains information about a field.
type FieldMetadata struct {
	Tags       map[string]string
	Name       string
	Table      string
	DataType   string
	IsNullable bool
}

// ResultType indicates what kind of result to expect.
type ResultType string

const (
	ResultSingle   ResultType = "single"   // Expecting 0 or 1 row
	ResultMultiple ResultType = "multiple" // Expecting 0 to many rows
	ResultCount    ResultType = "count"    // Expecting a count
	ResultAffected ResultType = "affected" // Rows affected (INSERT/UPDATE/DELETE)
)

// TODO: Move to postgres package
/*
// extractQueryMetadata analyzes the AST and Sentinel data to build metadata
func extractQueryMetadata(ast *PostgresAST) QueryMetadata {
	meta := QueryMetadata{
		Operation: ast.Operation,
		Table: TableMetadata{
			Name:  ast.Target.Name,
			Alias: ast.Target.Alias,
		},
	}

	// Try to get the type name from table name
	typeName := tableNameToTypeName(ast.Target.Name)
	meta.Table.TypeName = typeName
	meta.ScanType = typeName

	// Determine result type
	switch ast.Operation {
	case OpSelect:
		if ast.Limit != nil && *ast.Limit == 1 {
			meta.ResultType = ResultSingle
		} else {
			meta.ResultType = ResultMultiple
		}

		// Extract returned fields
		meta.ReturnedFields = extractReturnedFields(ast)

	case OpCount:
		meta.ResultType = ResultCount

	case OpInsert, OpUpdate, OpDelete:
		meta.ResultType = ResultAffected

		// For INSERT/UPDATE, track modified fields
		if ast.Operation == OpInsert && len(ast.Values) > 0 {
			for field := range ast.Values[0] {
				meta.ModifiedFields = append(meta.ModifiedFields, fieldToMetadata(field))
			}
		} else if ast.Operation == OpUpdate {
			for field := range ast.Updates {
				meta.ModifiedFields = append(meta.ModifiedFields, fieldToMetadata(field))
			}
		}

		// If RETURNING clause exists, those are the returned fields
		if len(ast.Returning) > 0 {
			meta.ResultType = ResultSingle // RETURNING typically returns the affected row(s)
			for _, field := range ast.Returning {
				meta.ReturnedFields = append(meta.ReturnedFields, fieldToMetadata(field))
			}
		}
	}

	return meta
}

// extractReturnedFields determines what fields will be returned by a SELECT
func extractReturnedFields(ast *PostgresAST) []FieldMetadata {
	var fields []FieldMetadata

	// Regular fields
	if len(ast.Fields) == 0 && len(ast.FieldExpressions) == 0 {
		// SELECT * - we need to get all fields from Sentinel
		typeName := tableNameToTypeName(ast.Target.Name)
		if metadata, exists := sentinel.GetCachedMetadata(typeName); exists {
			for _, field := range metadata.Fields {
				if dbTag, ok := field.Tags["db"]; ok && dbTag != "-" {
					fields = append(fields, FieldMetadata{
						Name:       dbTag,
						DataType:   field.Type,
						IsNullable: strings.HasPrefix(field.Type, "*"), // Pointers are nullable
						Tags:       field.Tags,
					})
				}
			}
		}
	} else {
		// Specific fields
		for _, field := range ast.Fields {
			fields = append(fields, fieldToMetadata(field))
		}

		// Field expressions (aggregates)
		for _, expr := range ast.FieldExpressions {
			fieldMeta := fieldToMetadata(expr.Field)
			if expr.Alias != "" {
				fieldMeta.Name = expr.Alias
			}
			if expr.Aggregate != "" {
				fieldMeta.DataType = "numeric" // Aggregates typically return numeric types
			}
			fields = append(fields, fieldMeta)
		}
	}

	return fields
}
*/

// e.g., "test_users" -> "TestUser".
func tableNameToTypeName(tableName string) string {
	// Remove common suffixes
	name := strings.TrimSuffix(tableName, "s")

	// Convert snake_case to PascalCase
	parts := strings.Split(name, "_")
	for i, part := range parts {
		if part != "" {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}

	return strings.Join(parts, "")
}

// GetTableMetadata returns metadata for a table based on its name.
func GetTableMetadata(tableName string) *TableMetadata {
	typeName := tableNameToTypeName(tableName)

	meta := &TableMetadata{
		Name:     tableName,
		TypeName: typeName,
	}

	// Try to enrich with Sentinel data
	if metadata, exists := sentinel.GetCachedMetadata(typeName); exists {
		// We have metadata for this type
		fields := make([]FieldMetadata, 0)
		for _, field := range metadata.Fields {
			if dbTag, ok := field.Tags["db"]; ok && dbTag != "-" {
				fields = append(fields, FieldMetadata{
					Name:       dbTag,
					DataType:   field.Type,
					IsNullable: strings.HasPrefix(field.Type, "*"),
					Tags:       field.Tags,
				})
			}
		}
		meta.Fields = fields
	}

	return meta
}

// GetFieldMetadata returns metadata for a specific field in a table.
func GetFieldMetadata(tableName string, fieldName string) *FieldMetadata {
	typeName := tableNameToTypeName(tableName)

	// Try to find the field in Sentinel metadata
	if metadata, exists := sentinel.GetCachedMetadata(typeName); exists {
		for _, field := range metadata.Fields {
			if dbTag, ok := field.Tags["db"]; ok && dbTag == fieldName {
				return &FieldMetadata{
					Name:       fieldName,
					DataType:   field.Type,
					IsNullable: strings.HasPrefix(field.Type, "*"),
					Tags:       field.Tags,
				}
			}
		}
	}

	// Fallback - return basic metadata without type info
	return &FieldMetadata{
		Name: fieldName,
	}
}
