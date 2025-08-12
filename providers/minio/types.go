package minio

import (
	"github.com/zoobzio/astql"
)

// EventType represents MinIO notification event types.
type EventType string

// MinIO event types - fully enumerated, no arbitrary strings.
const (
	EventObjectCreatedAll       EventType = "s3:ObjectCreated:*"
	EventObjectCreatedPut       EventType = "s3:ObjectCreated:Put"
	EventObjectCreatedPost      EventType = "s3:ObjectCreated:Post"
	EventObjectCreatedCopy      EventType = "s3:ObjectCreated:Copy"
	EventObjectCreatedMultipart EventType = "s3:ObjectCreated:CompleteMultipartUpload"

	EventObjectRemovedAll                 EventType = "s3:ObjectRemoved:*"
	EventObjectRemovedDelete              EventType = "s3:ObjectRemoved:Delete"
	EventObjectRemovedDeleteMarkerCreated EventType = "s3:ObjectRemoved:DeleteMarkerCreated"

	EventObjectAccessedAll  EventType = "s3:ObjectAccessed:*"
	EventObjectAccessedGet  EventType = "s3:ObjectAccessed:Get"
	EventObjectAccessedHead EventType = "s3:ObjectAccessed:Head"
)

// Region represents MinIO/S3 regions.
type Region string

// Common regions - enumerated, not arbitrary.
const (
	RegionUSEast1      Region = "us-east-1"
	RegionUSWest1      Region = "us-west-1"
	RegionUSWest2      Region = "us-west-2"
	RegionEUWest1      Region = "eu-west-1"
	RegionEUCentral1   Region = "eu-central-1"
	RegionAPSoutheast1 Region = "ap-southeast-1"
	RegionAPNortheast1 Region = "ap-northeast-1"
)

// TableConfig defines how an ASTQL table maps to MinIO buckets.
type TableConfig struct {
	// Bucket name
	Bucket string

	// Optional path prefix for objects
	PathPrefix string

	// IDField specifies which field contains the object key
	IDField string

	// Region for the bucket
	Region Region
}

// AST extends the base QueryAST with MinIO-specific features.
type AST struct {
	*astql.QueryAST

	// MinIO-specific fields
	TableConfig  *TableConfig
	ContentParam *astql.Param // Parameter containing object content
	EventTypes   []EventType  // For LISTEN operations
}

// NewAST creates a MinIO AST from a base QueryAST.
func NewAST(baseAST *astql.QueryAST) *AST {
	return &AST{
		QueryAST: baseAST,
	}
}

// Validate checks if the AST is valid for MinIO operations.
func (ast *AST) Validate() error {
	// First validate the base AST
	if err := ast.QueryAST.Validate(); err != nil {
		return err
	}

	// Add MinIO-specific validation here

	return nil
}
