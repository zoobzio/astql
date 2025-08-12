package minio_test

import (
	"fmt"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/minio"
)

func ExampleProvider() {
	// Setup
	astql.SetupTestModels()
	provider := minio.NewProvider()

	// Configure how tables map to MinIO buckets
	provider.RegisterTable("test_documents", minio.TableConfig{
		Bucket:     "company-docs",
		PathPrefix: "2024/",
		IDField:    "key",
		Region:     minio.RegionUSEast1,
	})

	// Example 1: List objects with prefix
	query1 := minio.Select(astql.T("test_documents")).
		Where(astql.C(astql.F("key"), astql.LIKE, astql.P("search_prefix")))

	ast1, _ := query1.Build()
	result1, _ := provider.Render(ast1)
	fmt.Println("List Objects:", result1.SQL)

	// Example 2: Upload document
	query2 := minio.Insert(astql.T("test_documents")).
		Values(map[astql.Field]astql.Param{
			astql.F("key"):          astql.P("doc_key"),
			astql.F("content_type"): astql.P("mime_type"),
		}).
		WithContent(astql.P("document_data"))

	ast2, _ := query2.Build()
	result2, _ := provider.Render(ast2)
	fmt.Println("Upload Document:", contains(result2.SQL, "PutObject"))

	// Example 3: Delete object
	query3 := minio.Delete(astql.T("test_documents")).
		Where(astql.C(astql.F("key"), astql.EQ, astql.P("doc_to_delete")))

	ast3, _ := query3.Build()
	result3, _ := provider.Render(ast3)
	fmt.Println("Delete Object:", result3.SQL)

	// Example 4: Listen for upload events
	query4 := minio.Listen(astql.T("test_documents")).
		WithEventTypes(minio.EventObjectCreatedPut)

	ast4, _ := query4.Build()
	result4, _ := provider.Render(ast4)
	fmt.Println("Listen Events:", contains(result4.SQL, "ListenBucketNotification"))

	// Output:
	// List Objects: ListObjects company-docs prefix=2024/:search_prefix
	// Upload Document: true
	// Delete Object: RemoveObject company-docs 2024/:doc_to_delete
	// Listen Events: true
}

func ExampleProvider_imageStorage() {
	// Setup
	astql.SetupTestModels()
	provider := minio.NewProvider()

	provider.RegisterTable("test_images", minio.TableConfig{
		Bucket:  "user-images",
		IDField: "path",
		Region:  minio.RegionUSWest2,
	})

	// Example: Upload image with metadata
	query := minio.Insert(astql.T("test_images")).
		Values(map[astql.Field]astql.Param{
			astql.F("path"):         astql.P("image_path"),
			astql.F("content_type"): astql.P("mime"),
			astql.F("width"):        astql.P("w"),
			astql.F("height"):       astql.P("h"),
		}).
		WithContent(astql.P("image_bytes"))

	ast, _ := query.Build()
	result, _ := provider.Render(ast)
	fmt.Println("Upload Image:", contains(result.SQL, "PutObject"))
	fmt.Println("Has Metadata:", contains(result.SQL, "x-amz-meta-"))
	fmt.Println("Required Params:", len(result.RequiredParams))

	// Output:
	// Upload Image: true
	// Has Metadata: true
	// Required Params: 5
}
