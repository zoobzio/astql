package minio_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/minio"
)

func TestMinioBuilder(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("Update operation", func(t *testing.T) {
		// MinIO doesn't support UPDATE, should set error
		builder := minio.Update(astql.T("test_documents"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported UPDATE operation")
		}

		// Build should return the error
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected Build to return error for UPDATE")
		}
	})

	t.Run("Unlisten operation", func(t *testing.T) {
		// MinIO doesn't support UNLISTEN, should set error
		builder := minio.Unlisten(astql.T("test_documents"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported UNLISTEN operation")
		}

		// Build should return the error
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected Build to return error for UNLISTEN")
		}
	})

	t.Run("Set method", func(t *testing.T) {
		// Set is not supported for MinIO
		builder := minio.Select(astql.T("test_documents")).
			Set(astql.F("content_type"), astql.P("type"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported Set method")
		}
	})

	t.Run("Fields method", func(t *testing.T) {
		// Fields is not supported for MinIO (object storage returns full objects)
		builder := minio.Select(astql.T("test_documents")).
			Fields(astql.F("key"), astql.F("content_type"))

		if builder.GetError() == nil {
			t.Error("Expected error for unsupported Fields method")
		}
	})

	t.Run("Limit method", func(t *testing.T) {
		// Limit is supported for listing objects
		builder := minio.Select(astql.T("test_documents")).
			Limit(100)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ast.Limit == nil || *ast.Limit != 100 {
			t.Error("Expected limit to be set to 100")
		}
	})

	t.Run("Offset method", func(t *testing.T) {
		// Offset is supported as continuation token
		builder := minio.Select(astql.T("test_documents")).
			Offset(50)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ast.Offset == nil || *ast.Offset != 50 {
			t.Error("Expected offset to be set to 50")
		}
	})

	t.Run("Builder with error propagation", func(t *testing.T) {
		// Once an error is set, subsequent operations should not change it
		builder := minio.Update(astql.T("test_documents")) // This sets an error

		// Try more operations
		builder.Where(astql.C(astql.F("key"), astql.EQ, astql.P("key")))
		builder.Values(map[astql.Field]astql.Param{
			astql.F("key"): astql.P("key"),
		})

		// Original error should still be there
		_, err := builder.Build()
		if err == nil {
			t.Error("Expected original error to persist")
		}
		if err.Error() != "UPDATE not supported by MinIO provider" {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})

	t.Run("All builder methods return same builder", func(t *testing.T) {
		builder := minio.Select(astql.T("test_documents"))

		// All these should return the same builder instance
		b1 := builder.Where(astql.C(astql.F("key"), astql.EQ, astql.P("key")))
		b2 := b1.Limit(10)
		b3 := b2.Offset(5)

		// They should all be the same instance
		if b1 != builder || b2 != builder || b3 != builder {
			t.Error("Builder methods should return the same builder instance")
		}
	})
}
