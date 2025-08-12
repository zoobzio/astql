package astql_test

import (
	"testing"

	"github.com/zoobzio/astql"
)

func TestQueryResultMetadata(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("GetTableMetadata returns correct metadata", func(t *testing.T) {
		result := &astql.QueryResult{
			Metadata: astql.QueryMetadata{
				Operation: astql.OpSelect,
				Table: astql.TableMetadata{
					Name: "test_users",
				},
			},
		}

		tableMeta := astql.GetTableMetadata(result.Metadata.Table.Name)

		if tableMeta.Name != "test_users" {
			t.Errorf("Expected table name 'test_users', got '%s'", tableMeta.Name)
		}

		// Type should be derived from table name
		expectedType := "TestUser"
		if tableMeta.TypeName != expectedType {
			t.Errorf("Expected type '%s', got '%s'", expectedType, tableMeta.TypeName)
		}
	})

	t.Run("GetTableMetadata handles various table name formats", func(t *testing.T) {
		testCases := []struct {
			tableName    string
			expectedType string
		}{
			{"test_users", "TestUser"},
			{"test_customers", "TestCustomer"},
			{"test_orders", "TestOrder"},
			{"test_products", "TestProduct"},
			{"simple_table", "SimpleTable"},
			{"another_test_table", "AnotherTestTable"},
		}

		for _, tc := range testCases {
			result := &astql.QueryResult{
				Metadata: astql.QueryMetadata{
					Table: astql.TableMetadata{
						Name: tc.tableName,
					},
				},
			}

			tableMeta := astql.GetTableMetadata(result.Metadata.Table.Name)
			if tableMeta.TypeName != tc.expectedType {
				t.Errorf("For table '%s', expected type '%s', got '%s'",
					tc.tableName, tc.expectedType, tableMeta.TypeName)
			}
		}
	})

	t.Run("GetFieldMetadata returns field metadata", func(t *testing.T) {
		result := &astql.QueryResult{
			Metadata: astql.QueryMetadata{
				Table: astql.TableMetadata{
					Name:     "test_users",
					TypeName: "TestUser",
				},
			},
		}

		// Get metadata for a known field
		fieldMeta := astql.GetFieldMetadata(result.Metadata.Table.Name, "name")

		if fieldMeta == nil {
			t.Fatal("Expected field metadata, got nil")
		}

		if fieldMeta.Name != "name" {
			t.Errorf("Expected field name 'name', got '%s'", fieldMeta.Name)
		}

		// Check that aliases are populated from tags
		if aliasTag, ok := fieldMeta.Tags["alias"]; ok {
			expected := "user_name,full_name,manager_name,display_name,unique_names"
			if aliasTag != expected {
				t.Errorf("Expected alias tag '%s', got '%s'", expected, aliasTag)
			}
		} else {
			t.Error("Expected alias tag to be present")
		}
	})

	t.Run("GetFieldMetadata returns nil for unknown field", func(t *testing.T) {
		result := &astql.QueryResult{
			Metadata: astql.QueryMetadata{
				Table: astql.TableMetadata{
					Name:     "test_users",
					TypeName: "TestUser",
				},
			},
		}

		fieldMeta := astql.GetFieldMetadata(result.Metadata.Table.Name, "unknown_field")

		if fieldMeta != nil {
			t.Error("Expected nil for unknown field")
		}
	})

	t.Run("GetFieldMetadata handles field with no aliases", func(t *testing.T) {
		result := &astql.QueryResult{
			Metadata: astql.QueryMetadata{
				Table: astql.TableMetadata{
					Name:     "test_users",
					TypeName: "TestUser",
				},
			},
		}

		// created_at has no aliases
		fieldMeta := astql.GetFieldMetadata(result.Metadata.Table.Name, "created_at")

		if fieldMeta == nil {
			t.Fatal("Expected field metadata, got nil")
		}

		// Check that alias tag is not present
		if aliasTag, ok := fieldMeta.Tags["alias"]; ok {
			t.Errorf("Expected no alias tag, got '%s'", aliasTag)
		}
	})

	t.Run("GetFieldMetadata handles empty table metadata", func(t *testing.T) {
		result := &astql.QueryResult{
			Metadata: astql.QueryMetadata{
				Table: astql.TableMetadata{
					// Empty table metadata
				},
			},
		}

		fieldMeta := astql.GetFieldMetadata(result.Metadata.Table.Name, "name")

		// Should return nil when table metadata is incomplete
		if fieldMeta != nil {
			t.Error("Expected nil when table metadata is empty")
		}
	})
}
