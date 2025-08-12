package minio

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// Provider renders MinIO operations from ASTQL AST.
type Provider struct {
	// TableConfigs maps table names to their MinIO configurations.
	TableConfigs map[string]TableConfig
}

// NewProvider creates a new MinIO provider.
func NewProvider() *Provider {
	return &Provider{
		TableConfigs: make(map[string]TableConfig),
	}
}

// RegisterTable configures how a table maps to MinIO.
func (p *Provider) RegisterTable(tableName string, config TableConfig) {
	p.TableConfigs[tableName] = config
}

// OperationResult represents a MinIO operation.
//
//nolint:govet // field alignment not critical for operation results
type OperationResult struct {
	Operation    string
	Bucket       string
	Key          string
	Args         map[string]interface{}
	ContentParam *astql.Param
}

// Render converts an AST to MinIO operations.
func (p *Provider) Render(ast *AST) (*astql.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	// Get table configuration
	config, exists := p.TableConfigs[ast.Target.Name]
	if !exists {
		return nil, fmt.Errorf("table '%s' not registered for MinIO", ast.Target.Name)
	}
	ast.TableConfig = &config

	// Render based on operation
	var result *OperationResult
	var err error

	switch ast.Operation {
	case astql.OpSelect:
		result, err = p.renderSelect(ast)
	case astql.OpInsert:
		result, err = p.renderInsert(ast)
	case astql.OpUpdate:
		result, err = p.renderUpdate(ast)
	case astql.OpDelete:
		result, err = p.renderDelete(ast)
	case astql.OpCount:
		result, err = p.renderCount(ast)
	case astql.OpListen:
		result, err = p.renderListen(ast)
	case astql.OpNotify:
		return nil, fmt.Errorf("NOTIFY not supported for MinIO (notifications are automatic)")
	case astql.OpUnlisten:
		result, err = p.renderUnlisten(ast)
	default:
		return nil, fmt.Errorf("unsupported operation for MinIO: %s", ast.Operation)
	}

	if err != nil {
		return nil, err
	}

	// Convert to QueryResult
	sql := encodeOperation(result)

	return &astql.QueryResult{
		SQL:            sql,
		RequiredParams: extractRequiredParams(ast),
		Metadata: astql.QueryMetadata{
			Operation: ast.Operation,
			Table: astql.TableMetadata{
				Name: ast.Target.Name,
			},
		},
	}, nil
}

func (p *Provider) renderSelect(ast *AST) (*OperationResult, error) {
	// Extract prefix from WHERE clause if present
	prefix := ast.TableConfig.PathPrefix
	if ast.WhereClause != nil {
		keyPrefix, err := p.extractPrefix(ast)
		if err != nil {
			return nil, err
		}
		prefix = combinePrefix(prefix, keyPrefix)
	}

	return &OperationResult{
		Operation: "ListObjects",
		Bucket:    ast.TableConfig.Bucket,
		Args: map[string]interface{}{
			"prefix": prefix,
		},
	}, nil
}

func (p *Provider) renderInsert(ast *AST) (*OperationResult, error) {
	if len(ast.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires values")
	}

	// MinIO only supports single object insertion
	if len(ast.Values) > 1 {
		return nil, fmt.Errorf("MinIO INSERT only supports single object")
	}

	valueSet := ast.Values[0]

	// Extract object key
	key, err := p.extractKeyFromValues(ast, valueSet)
	if err != nil {
		return nil, err
	}

	// Build args from other fields
	args := make(map[string]interface{})
	for field, param := range valueSet {
		if field.Name != ast.TableConfig.IDField {
			// Only known metadata fields are allowed
			switch field.Name {
			case "content_type":
				args["contentType"] = fmt.Sprintf(":%s", param.Name)
			case "cache_control":
				args["cacheControl"] = fmt.Sprintf(":%s", param.Name)
			case "content_encoding":
				args["contentEncoding"] = fmt.Sprintf(":%s", param.Name)
			case "content_disposition":
				args["contentDisposition"] = fmt.Sprintf(":%s", param.Name)
			default:
				// User metadata must use validated fields
				// Store as x-amz-meta-{fieldname}
				args[fmt.Sprintf("x-amz-meta-%s", field.Name)] = fmt.Sprintf(":%s", param.Name)
			}
		}
	}

	return &OperationResult{
		Operation:    "PutObject",
		Bucket:       ast.TableConfig.Bucket,
		Key:          key,
		Args:         args,
		ContentParam: ast.ContentParam,
	}, nil
}

func (p *Provider) renderUpdate(ast *AST) (*OperationResult, error) {
	// MinIO doesn't have true UPDATE - objects are immutable
	// Convert UPDATE to PUT (full replacement)
	if len(ast.Updates) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one field")
	}

	// Extract key from WHERE clause
	key, err := p.extractKey(ast)
	if err != nil {
		return nil, err
	}

	// Build args from updates
	args := make(map[string]interface{})
	for field, param := range ast.Updates {
		switch field.Name {
		case "content_type":
			args["contentType"] = fmt.Sprintf(":%s", param.Name)
		case "cache_control":
			args["cacheControl"] = fmt.Sprintf(":%s", param.Name)
		default:
			args[fmt.Sprintf("x-amz-meta-%s", field.Name)] = fmt.Sprintf(":%s", param.Name)
		}
	}

	return &OperationResult{
		Operation:    "PutObject",
		Bucket:       ast.TableConfig.Bucket,
		Key:          key,
		Args:         args,
		ContentParam: ast.ContentParam,
	}, nil
}

func (p *Provider) renderDelete(ast *AST) (*OperationResult, error) {
	// Extract key from WHERE clause
	key, err := p.extractKey(ast)
	if err != nil {
		return nil, err
	}

	return &OperationResult{
		Operation: "RemoveObject",
		Bucket:    ast.TableConfig.Bucket,
		Key:       key,
	}, nil
}

func (p *Provider) renderCount(ast *AST) (*OperationResult, error) {
	// Similar to SELECT but with counting
	prefix := ast.TableConfig.PathPrefix
	if ast.WhereClause != nil {
		keyPrefix, err := p.extractPrefix(ast)
		if err != nil {
			return nil, err
		}
		prefix = combinePrefix(prefix, keyPrefix)
	}

	return &OperationResult{
		Operation: "ListObjects",
		Bucket:    ast.TableConfig.Bucket,
		Args: map[string]interface{}{
			"prefix": prefix,
			"count":  true,
		},
	}, nil
}

func (*Provider) renderListen(ast *AST) (*OperationResult, error) {
	// MinIO notifications require configuration
	args := make(map[string]interface{})

	// Add event types if specified
	if len(ast.EventTypes) > 0 {
		events := make([]string, len(ast.EventTypes))
		for i, evt := range ast.EventTypes {
			events[i] = string(evt)
		}
		args["events"] = events
	}

	// Add prefix filter if specified
	if ast.TableConfig.PathPrefix != "" {
		args["prefix"] = ast.TableConfig.PathPrefix
	}

	return &OperationResult{
		Operation: "ListenBucketNotification",
		Bucket:    ast.TableConfig.Bucket,
		Args:      args,
	}, nil
}

func (*Provider) renderUnlisten(ast *AST) (*OperationResult, error) {
	return &OperationResult{
		Operation: "RemoveAllBucketNotification",
		Bucket:    ast.TableConfig.Bucket,
	}, nil
}

// extractKey extracts the object key from WHERE clause.
func (*Provider) extractKey(ast *AST) (string, error) {
	if ast.WhereClause == nil {
		return "", fmt.Errorf("MinIO operations require WHERE clause with %s field", ast.TableConfig.IDField)
	}

	// Only support simple equality on ID field
	cond, ok := ast.WhereClause.(astql.Condition)
	if !ok {
		return "", fmt.Errorf("MinIO only supports simple equality conditions")
	}

	if cond.Field.Name != ast.TableConfig.IDField {
		return "", fmt.Errorf("MinIO WHERE clause must use %s field", ast.TableConfig.IDField)
	}

	if cond.Operator != astql.EQ {
		return "", fmt.Errorf("MinIO only supports equality (=) operator for exact key match")
	}

	// Build key with prefix
	key := combinePrefix(ast.TableConfig.PathPrefix, fmt.Sprintf(":%s", cond.Value.Name))

	return key, nil
}

// extractPrefix extracts prefix for LIKE operations.
func (*Provider) extractPrefix(ast *AST) (string, error) {
	cond, ok := ast.WhereClause.(astql.Condition)
	if !ok {
		return "", fmt.Errorf("MinIO SELECT only supports simple conditions, not complex WHERE")
	}

	if cond.Field.Name != ast.TableConfig.IDField {
		return "", fmt.Errorf("WHERE must use %s field", ast.TableConfig.IDField)
	}

	if cond.Operator == astql.LIKE {
		// For LIKE, the parameter contains the prefix pattern
		return fmt.Sprintf(":%s", cond.Value.Name), nil
	}

	if cond.Operator == astql.EQ {
		// For equality, treat as exact prefix match
		return fmt.Sprintf(":%s", cond.Value.Name), nil
	}

	return "", fmt.Errorf("MinIO SELECT only supports = and LIKE operators")
}

// extractKeyFromValues extracts the object key from value set.
func (*Provider) extractKeyFromValues(ast *AST, values map[astql.Field]astql.Param) (string, error) {
	// Find ID field in values
	var idParam *astql.Param
	for field, param := range values {
		if field.Name == ast.TableConfig.IDField {
			idParam = &param
			break
		}
	}

	if idParam == nil {
		return "", fmt.Errorf("MinIO INSERT requires %s field", ast.TableConfig.IDField)
	}

	// Build key with prefix
	key := combinePrefix(ast.TableConfig.PathPrefix, fmt.Sprintf(":%s", idParam.Name))

	return key, nil
}

// combinePrefix safely combines path prefixes.
func combinePrefix(prefix, suffix string) string {
	if prefix == "" {
		return suffix
	}
	if suffix == "" {
		return prefix
	}
	// Ensure single slash between parts
	return strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(suffix, "/")
}

// encodeOperation converts MinIO operation to string representation.
func encodeOperation(op *OperationResult) string {
	parts := []string{op.Operation, op.Bucket}

	if op.Key != "" {
		parts = append(parts, op.Key)
	}

	// Add args
	for k, v := range op.Args {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}

	if op.ContentParam != nil {
		parts = append(parts, fmt.Sprintf("content=:%s", op.ContentParam.Name))
	}

	return strings.Join(parts, " ")
}

// extractRequiredParams extracts parameter names from the AST.
func extractRequiredParams(ast *AST) []string {
	params := make(map[string]bool)

	// From WHERE clause
	if ast.WhereClause != nil {
		if cond, ok := ast.WhereClause.(astql.Condition); ok {
			params[cond.Value.Name] = true
		}
	}

	// From VALUES (INSERT)
	for _, valueSet := range ast.Values {
		for _, param := range valueSet {
			params[param.Name] = true
		}
	}

	// From UPDATES (UPDATE)
	for _, param := range ast.Updates {
		params[param.Name] = true
	}

	// From content parameter
	if ast.ContentParam != nil {
		params[ast.ContentParam.Name] = true
	}

	// Convert to slice
	result := make([]string, 0, len(params))
	for param := range params {
		result = append(result, param)
	}

	return result
}
