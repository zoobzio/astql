package minio_test

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/minio"
)

func TestMinIOProvider(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	// Create and configure provider
	provider := minio.NewProvider()

	// Register table configurations
	provider.RegisterTable("test_documents", minio.TableConfig{
		Bucket:     "documents",
		PathPrefix: "uploads/",
		IDField:    "key",
		Region:     minio.RegionUSEast1,
	})

	provider.RegisterTable("test_images", minio.TableConfig{
		Bucket:  "images",
		IDField: "path",
		Region:  minio.RegionUSEast1,
	})

	t.Run("SELECT objects with prefix", func(t *testing.T) {
		builder := minio.Select(astql.T("test_documents"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "ListObjects documents prefix=uploads/"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("SELECT with LIKE for prefix filter", func(t *testing.T) {
		builder := minio.Select(astql.T("test_images")).
			Where(astql.C(astql.F("path"), astql.LIKE, astql.P("prefix")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "ListObjects images") {
			t.Errorf("Expected ListObjects command, got: %s", result.SQL)
		}
		if !contains(result.SQL, "prefix=:prefix") {
			t.Errorf("Expected prefix parameter, got: %s", result.SQL)
		}
	})

	t.Run("INSERT object with content", func(t *testing.T) {
		builder := minio.Insert(astql.T("test_documents")).
			Values(map[astql.Field]astql.Param{
				astql.F("key"):          astql.P("filename"),
				astql.F("content_type"): astql.P("mime_type"),
			}).
			WithContent(astql.P("file_data"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "PutObject documents uploads/:filename") {
			t.Errorf("Expected PutObject with key, got: %s", result.SQL)
		}
		if !contains(result.SQL, "contentType=:mime_type") {
			t.Errorf("Expected contentType, got: %s", result.SQL)
		}
		if !contains(result.SQL, "content=:file_data") {
			t.Errorf("Expected content parameter, got: %s", result.SQL)
		}

		// Check required params
		hasAllParams := containsParam(result.RequiredParams, "filename") &&
			containsParam(result.RequiredParams, "mime_type") &&
			containsParam(result.RequiredParams, "file_data")
		if !hasAllParams {
			t.Errorf("Missing required params: %v", result.RequiredParams)
		}
	})

	t.Run("INSERT with custom metadata", func(t *testing.T) {
		builder := minio.Insert(astql.T("test_images")).
			Values(map[astql.Field]astql.Param{
				astql.F("path"):   astql.P("image_path"),
				astql.F("width"):  astql.P("img_width"),
				astql.F("height"): astql.P("img_height"),
			}).
			WithContent(astql.P("image_data"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		// Custom fields become x-amz-meta-{fieldname}
		if !contains(result.SQL, "x-amz-meta-width=:img_width") {
			t.Errorf("Expected width metadata, got: %s", result.SQL)
		}
		if !contains(result.SQL, "x-amz-meta-height=:img_height") {
			t.Errorf("Expected height metadata, got: %s", result.SQL)
		}
	})

	t.Run("DELETE object", func(t *testing.T) {
		builder := minio.Delete(astql.T("test_documents")).
			Where(astql.C(astql.F("key"), astql.EQ, astql.P("doc_key")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		expected := "RemoveObject documents uploads/:doc_key"
		if result.SQL != expected {
			t.Errorf("Expected: %s\nGot: %s", expected, result.SQL)
		}
	})

	t.Run("LISTEN for bucket notifications", func(t *testing.T) {
		builder := minio.Listen(astql.T("test_images")).
			WithEventTypes(
				minio.EventObjectCreatedPut,
				minio.EventObjectRemovedDelete,
			)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "ListenBucketNotification images") {
			t.Errorf("Expected ListenBucketNotification, got: %s", result.SQL)
		}
		if !contains(result.SQL, "events=") {
			t.Errorf("Expected events list, got: %s", result.SQL)
		}
	})

	t.Run("COUNT objects", func(t *testing.T) {
		builder := minio.Count(astql.T("test_documents"))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render: %v", err)
		}

		if !contains(result.SQL, "ListObjects documents") {
			t.Errorf("Expected ListObjects, got: %s", result.SQL)
		}
		if !contains(result.SQL, "count=true") {
			t.Errorf("Expected count flag, got: %s", result.SQL)
		}
	})

	t.Run("Unsupported operations fail gracefully", func(t *testing.T) {
		// MinIO doesn't support NOTIFY (notifications are automatic)
		// We'll test this by creating a valid AST and then manually setting
		// the operation to NOTIFY to bypass builder validation
		ast := &minio.AST{
			QueryAST: &astql.QueryAST{
				Operation:     astql.OpNotify,
				Target:        astql.T("test_documents"),
				NotifyPayload: &astql.Param{Name: "message"},
			},
		}

		_, err := provider.Render(ast)
		if err == nil {
			t.Error("Expected error for NOTIFY operation")
		}
		if !contains(err.Error(), "NOTIFY not supported") {
			t.Errorf("Wrong error message: %v", err)
		}
	})

	t.Run("Complex WHERE fails gracefully", func(t *testing.T) {
		builder := minio.Select(astql.T("test_documents")).
			Where(astql.And(
				astql.C(astql.F("key"), astql.EQ, astql.P("key1")),
				astql.C(astql.F("content_type"), astql.EQ, astql.P("type")),
			))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build: %v", err)
		}

		_, err = provider.Render(ast)
		if err == nil {
			t.Error("Expected error for complex WHERE")
		}
	})
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func containsParam(params []string, param string) bool {
	for _, p := range params {
		if p == param {
			return true
		}
	}
	return false
}
