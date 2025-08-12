package minio_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/minio"
	"gopkg.in/yaml.v3"
)

func TestMinIOSchemaQueries(t *testing.T) {
	astql.SetupTestModels()
	provider := minio.NewProvider()

	// Register table configurations
	provider.RegisterTable("test_documents", minio.TableConfig{
		Bucket:     "docs",
		PathPrefix: "files/",
		IDField:    "key",
		Region:     minio.RegionUSEast1,
	})

	provider.RegisterTable("test_images", minio.TableConfig{
		Bucket:  "images",
		IDField: "path",
		Region:  minio.RegionUSEast1,
	})

	t.Run("SELECT with prefix using schema", func(t *testing.T) {
		yamlSchema := `
operation: SELECT
table: test_documents
where:
  field: key
  operator: LIKE
  param: prefix_pattern
`
		var schema minio.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := minio.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "ListObjects docs") {
			t.Errorf("Expected ListObjects docs, got: %s", result.SQL)
		}
		if !contains(result.SQL, "prefix=files/:prefix_pattern") {
			t.Errorf("Expected prefix parameter, got: %s", result.SQL)
		}
	})

	t.Run("INSERT with content using schema", func(t *testing.T) {
		yamlSchema := `
operation: INSERT
table: test_documents
values:
  - key: document_key
    content_type: mime_type
content_param: pdf_data
`
		var schema minio.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := minio.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "PutObject docs files/:document_key") {
			t.Errorf("Expected PutObject with key, got: %s", result.SQL)
		}
		if !contains(result.SQL, "contentType=:mime_type") {
			t.Errorf("Expected content type, got: %s", result.SQL)
		}
		if !contains(result.SQL, "content=:pdf_data") {
			t.Errorf("Expected content param, got: %s", result.SQL)
		}

		// Check required params includes content
		hasParam := false
		for _, param := range result.RequiredParams {
			if param == "pdf_data" {
				hasParam = true
				break
			}
		}
		if !hasParam {
			t.Errorf("Expected pdf_data in required params, got: %v", result.RequiredParams)
		}
	})

	t.Run("LISTEN with event types using schema", func(t *testing.T) {
		yamlSchema := `
operation: LISTEN
table: test_images
event_types:
  - "s3:ObjectCreated:Put"
  - "s3:ObjectRemoved:Delete"
`
		var schema minio.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		ast, err := minio.BuildFromSchema(&schema)
		if err != nil {
			t.Fatalf("Failed to build from schema: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "ListenBucketNotification images") {
			t.Errorf("Expected ListenBucketNotification, got: %s", result.SQL)
		}
		if !contains(result.SQL, "events=[s3:ObjectCreated:Put s3:ObjectRemoved:Delete]") {
			t.Errorf("Expected event types, got: %s", result.SQL)
		}
	})

	t.Run("Invalid field in schema returns error", func(t *testing.T) {
		yamlSchema := `
operation: INSERT
table: test_documents
values:
  - key: doc_key
    invalid_field: value
`
		var schema minio.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		_, err := minio.BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for invalid field")
		}
		if !contains(err.Error(), "invalid_field") {
			t.Errorf("Error should mention invalid field: %v", err)
		}
	})

	t.Run("Invalid event type returns error", func(t *testing.T) {
		yamlSchema := `
operation: LISTEN
table: test_images
event_types:
  - "custom:Event:Type"
`
		var schema minio.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlSchema), &schema); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		_, err := minio.BuildFromSchema(&schema)
		if err == nil {
			t.Error("Expected error for invalid event type")
		}
		if !contains(err.Error(), "unknown event type") {
			t.Errorf("Error should mention unknown event type: %v", err)
		}
	})

	t.Run("AI-friendly error messages", func(t *testing.T) {
		// Test unsupported operator
		schema := &minio.QuerySchema{
			Operation: "SELECT",
			Table:     "test_documents",
			Where: &minio.ConditionSchema{
				Field:    "key",
				Operator: ">", // MinIO doesn't support comparison
				Param:    "value",
			},
		}

		_, err := minio.BuildFromSchema(schema)
		if err == nil {
			t.Error("Expected error for unsupported operator")
		}
		if !contains(err.Error(), "only supports = and LIKE") {
			t.Errorf("Error should explain supported operators: %v", err)
		}
	})
}
