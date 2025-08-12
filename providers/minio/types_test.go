package minio_test

import (
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/minio"
)

func TestMinioTypes(t *testing.T) {
	t.Run("Event type constants are defined", func(t *testing.T) {
		// Test that event types have expected values
		eventTypes := map[minio.EventType]string{
			minio.EventObjectCreatedAll:                 "s3:ObjectCreated:*",
			minio.EventObjectCreatedPut:                 "s3:ObjectCreated:Put",
			minio.EventObjectCreatedPost:                "s3:ObjectCreated:Post",
			minio.EventObjectCreatedCopy:                "s3:ObjectCreated:Copy",
			minio.EventObjectCreatedMultipart:           "s3:ObjectCreated:CompleteMultipartUpload",
			minio.EventObjectRemovedAll:                 "s3:ObjectRemoved:*",
			minio.EventObjectRemovedDelete:              "s3:ObjectRemoved:Delete",
			minio.EventObjectRemovedDeleteMarkerCreated: "s3:ObjectRemoved:DeleteMarkerCreated",
			minio.EventObjectAccessedAll:                "s3:ObjectAccessed:*",
			minio.EventObjectAccessedGet:                "s3:ObjectAccessed:Get",
			minio.EventObjectAccessedHead:               "s3:ObjectAccessed:Head",
		}

		for eventType, expected := range eventTypes {
			if string(eventType) != expected {
				t.Errorf("EventType %v has value %q, expected %q", eventType, string(eventType), expected)
			}
		}
	})

	t.Run("Region constants are defined", func(t *testing.T) {
		// Test that regions have expected values
		regions := map[minio.Region]string{
			minio.RegionUSEast1:      "us-east-1",
			minio.RegionUSWest1:      "us-west-1",
			minio.RegionUSWest2:      "us-west-2",
			minio.RegionEUWest1:      "eu-west-1",
			minio.RegionEUCentral1:   "eu-central-1",
			minio.RegionAPSoutheast1: "ap-southeast-1",
			minio.RegionAPNortheast1: "ap-northeast-1",
		}

		for region, expected := range regions {
			if string(region) != expected {
				t.Errorf("Region %v has value %q, expected %q", region, string(region), expected)
			}
		}
	})

	t.Run("TableConfig structure", func(t *testing.T) {
		config := minio.TableConfig{
			Bucket:     "test-bucket",
			PathPrefix: "documents/",
			IDField:    "key",
		}

		if config.Bucket != "test-bucket" {
			t.Errorf("Expected bucket 'test-bucket', got %q", config.Bucket)
		}
		if config.PathPrefix != "documents/" {
			t.Errorf("Expected path prefix 'documents/', got %q", config.PathPrefix)
		}
		if config.IDField != "key" {
			t.Errorf("Expected ID field 'key', got %q", config.IDField)
		}
	})

	t.Run("NewAST creates AST from base QueryAST", func(t *testing.T) {
		// Setup test models
		astql.SetupTestModels()

		// Create a base AST
		baseBuilder := astql.Select(astql.T("test_documents"))
		baseAST := baseBuilder.GetAST()

		// Create MinIO AST
		minioAST := minio.NewAST(baseAST)

		if minioAST.QueryAST != baseAST {
			t.Error("NewAST should embed the base QueryAST")
		}

		// Verify AST has MinIO-specific fields
		if minioAST.EventTypes != nil {
			t.Error("EventTypes should be nil by default")
		}
	})

	t.Run("AST with event filter", func(t *testing.T) {
		// Setup test models
		astql.SetupTestModels()

		baseAST := &astql.QueryAST{
			Operation: astql.OpListen,
			Target:    astql.T("test_documents"),
		}

		minioAST := minio.NewAST(baseAST)

		// Set event types
		minioAST.EventTypes = []minio.EventType{minio.EventObjectCreatedPut}

		if minioAST.EventTypes == nil {
			t.Fatal("EventTypes should not be nil")
		}

		if len(minioAST.EventTypes) != 1 || minioAST.EventTypes[0] != minio.EventObjectCreatedPut {
			t.Errorf("Expected EventObjectCreatedPut, got %v", minioAST.EventTypes)
		}
	})
}
