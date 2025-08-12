package opensearch

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// Provider renders OpenSearch queries from ASTQL AST.
type Provider struct {
	// TableConfigs maps table names to their OpenSearch configurations.
	// The table name itself (validated through Sentinel) is used as the index name.
	TableConfigs map[string]TableConfig
}

// NewProvider creates a new OpenSearch provider.
func NewProvider() *Provider {
	return &Provider{
		TableConfigs: make(map[string]TableConfig),
	}
}

// RegisterTable configures how a table maps to OpenSearch.
func (p *Provider) RegisterTable(tableName string, config TableConfig) {
	p.TableConfigs[tableName] = config
}

// QueryResult represents an OpenSearch query structure.
//
//nolint:govet // field alignment not critical for query results
type QueryResult struct {
	Index     string
	Operation string
	Query     map[string]interface{}
	Document  map[string]interface{}
	Sort      []map[string]interface{}
	From      *int
	Size      *int
	Source    interface{} // Can be bool, []string, or map
	Highlight map[string]interface{}
	MinScore  *float64
	Refresh   *RefreshPolicy
}

// Render converts an AST to OpenSearch query.
func (p *Provider) Render(ast *AST) (*astql.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	// Get table configuration
	config, exists := p.TableConfigs[ast.Target.Name]
	if !exists {
		return nil, fmt.Errorf("table '%s' not registered for OpenSearch", ast.Target.Name)
	}
	ast.TableConfig = &config

	// Render based on operation
	var result *QueryResult
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
	default:
		return nil, fmt.Errorf("unsupported operation for OpenSearch: %s", ast.Operation)
	}

	if err != nil {
		return nil, err
	}

	// Convert to QueryResult
	sql := encodeQuery(result)

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

func (p *Provider) renderSelect(ast *AST) (*QueryResult, error) {
	result := &QueryResult{
		Index:     ast.Target.Name, // Use validated table name directly
		Operation: "_search",
	}

	// Build query from WHERE clause
	if ast.WhereClause != nil {
		query, err := p.buildQuery(ast.WhereClause)
		if err != nil {
			return nil, err
		}
		result.Query = map[string]interface{}{"query": query}
	} else {
		// Match all documents
		result.Query = map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}

	// Add sorting
	if len(ast.Ordering) > 0 {
		sort := make([]map[string]interface{}, 0, len(ast.Ordering))
		for _, orderItem := range ast.Ordering {
			sortMap := map[string]interface{}{
				orderItem.Field.Name: map[string]interface{}{
					"order": strings.ToLower(string(orderItem.Direction)),
				},
			}
			sort = append(sort, sortMap)
		}
		result.Sort = sort
	}

	// Add pagination
	if ast.Offset != nil {
		result.From = ast.Offset
	}
	if ast.Limit != nil {
		result.Size = ast.Limit
	}

	// Handle field selection
	if len(ast.Fields) > 0 {
		fields := make([]string, len(ast.Fields))
		for i, field := range ast.Fields {
			fields[i] = field.Name
		}
		result.Source = fields
	} else if len(ast.SourceIncludes) > 0 || len(ast.SourceExcludes) > 0 {
		source := make(map[string][]string)
		if len(ast.SourceIncludes) > 0 {
			includes := make([]string, len(ast.SourceIncludes))
			for i, field := range ast.SourceIncludes {
				includes[i] = field.Name
			}
			source["includes"] = includes
		}
		if len(ast.SourceExcludes) > 0 {
			excludes := make([]string, len(ast.SourceExcludes))
			for i, field := range ast.SourceExcludes {
				excludes[i] = field.Name
			}
			source["excludes"] = excludes
		}
		result.Source = source
	}

	// Add highlighting
	if len(ast.Highlight) > 0 {
		highlight := map[string]interface{}{
			"fields": make(map[string]interface{}),
		}
		for _, field := range ast.Highlight {
			fieldsMap, ok := highlight["fields"].(map[string]interface{})
			if !ok {
				continue
			}
			fieldsMap[field.Name] = map[string]interface{}{}
		}
		result.Highlight = highlight
	}

	// Add minimum score filter
	if ast.MinScore != nil {
		result.MinScore = ast.MinScore
	}

	return result, nil
}

func (*Provider) renderInsert(ast *AST) (*QueryResult, error) {
	if len(ast.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires values")
	}

	// OpenSearch supports bulk insert, but for now single document
	if len(ast.Values) > 1 {
		return nil, fmt.Errorf("OpenSearch INSERT currently supports single document only")
	}

	valueSet := ast.Values[0]

	// Extract document ID if provided
	var docID string
	if ast.TableConfig.IDField != "" {
		for field, param := range valueSet {
			if field.Name == ast.TableConfig.IDField {
				docID = fmt.Sprintf(":%s", param.Name)
				break
			}
		}
	}

	// Build document from values
	doc := make(map[string]interface{})
	for field, param := range valueSet {
		if field.Name != ast.TableConfig.IDField {
			doc[field.Name] = fmt.Sprintf(":%s", param.Name)
		}
	}

	result := &QueryResult{
		Index:     ast.Target.Name, // Use validated table name directly
		Operation: "_doc",
		Document:  doc,
		Refresh:   &ast.TableConfig.RefreshPolicy,
	}

	// Add document ID to operation if provided
	if docID != "" {
		result.Operation = fmt.Sprintf("_doc/%s", docID)
	}

	return result, nil
}

func (p *Provider) renderUpdate(ast *AST) (*QueryResult, error) {
	if len(ast.Updates) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one field")
	}

	// OpenSearch supports partial updates
	doc := make(map[string]interface{})
	for field, param := range ast.Updates {
		doc[field.Name] = fmt.Sprintf(":%s", param.Name)
	}

	// For updates, we need either a document ID or a query
	if ast.WhereClause != nil {
		// Update by query
		query, err := p.buildQuery(ast.WhereClause)
		if err != nil {
			return nil, err
		}

		result := &QueryResult{
			Index:     ast.Target.Name, // Use validated table name directly
			Operation: "_update_by_query",
			Query: map[string]interface{}{
				"query": query,
				"script": map[string]interface{}{
					"source": buildUpdateScript(doc),
					"params": doc,
				},
			},
			Refresh: &ast.TableConfig.RefreshPolicy,
		}
		return result, nil
	}

	return nil, fmt.Errorf("UPDATE requires WHERE clause")
}

func (p *Provider) renderDelete(ast *AST) (*QueryResult, error) {
	if ast.WhereClause == nil {
		return nil, fmt.Errorf("DELETE requires WHERE clause for safety")
	}

	// Check if it's a simple ID-based delete
	docID, err := p.extractDocumentID(ast)
	if err == nil && docID != "" {
		// Delete by ID
		result := &QueryResult{
			Index:     ast.Target.Name, // Use validated table name directly
			Operation: fmt.Sprintf("_doc/%s", docID),
			Refresh:   &ast.TableConfig.RefreshPolicy,
		}
		return result, nil
	}

	// Delete by query
	query, err := p.buildQuery(ast.WhereClause)
	if err != nil {
		return nil, err
	}

	result := &QueryResult{
		Index:     ast.Target.Name, // Use validated table name directly
		Operation: "_delete_by_query",
		Query: map[string]interface{}{
			"query": query,
		},
		Refresh: &ast.TableConfig.RefreshPolicy,
	}

	return result, nil
}

func (p *Provider) renderCount(ast *AST) (*QueryResult, error) {
	result := &QueryResult{
		Index:     ast.Target.Name, // Use validated table name directly
		Operation: "_count",
	}

	// Build query from WHERE clause
	if ast.WhereClause != nil {
		query, err := p.buildQuery(ast.WhereClause)
		if err != nil {
			return nil, err
		}
		result.Query = map[string]interface{}{"query": query}
	} else {
		// Count all documents
		result.Query = map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}

	return result, nil
}

// buildQuery converts WHERE clause to OpenSearch query DSL.
func (p *Provider) buildQuery(where astql.ConditionItem) (map[string]interface{}, error) {
	switch cond := where.(type) {
	case astql.Condition:
		return p.buildSimpleQuery(cond)
	case astql.ConditionGroup:
		return p.buildBoolQuery(cond)
	default:
		return nil, fmt.Errorf("unsupported condition type")
	}
}

// buildSimpleQuery converts a simple condition to query DSL.
func (*Provider) buildSimpleQuery(cond astql.Condition) (map[string]interface{}, error) {
	field := cond.Field.Name

	// Only format param if not NULL check
	var param string
	if cond.Operator != astql.IsNull && cond.Operator != astql.IsNotNull {
		param = fmt.Sprintf(":%s", cond.Value.Name)
	}

	switch cond.Operator {
	case astql.EQ:
		// Use term query for exact match
		return map[string]interface{}{
			"term": map[string]interface{}{
				field: param,
			},
		}, nil

	case astql.NE:
		// Use bool must_not
		return map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": map[string]interface{}{
					"term": map[string]interface{}{
						field: param,
					},
				},
			},
		}, nil

	case astql.LIKE:
		// Use wildcard query
		return map[string]interface{}{
			"wildcard": map[string]interface{}{
				field: param,
			},
		}, nil

	case astql.GT, astql.GE, astql.LT, astql.LE:
		// Use range query
		rangeOp := map[astql.Operator]string{
			astql.GT: "gt",
			astql.GE: "gte",
			astql.LT: "lt",
			astql.LE: "lte",
		}
		return map[string]interface{}{
			"range": map[string]interface{}{
				field: map[string]interface{}{
					rangeOp[cond.Operator]: param,
				},
			},
		}, nil

	case astql.IN:
		// Use terms query (plural)
		return map[string]interface{}{
			"terms": map[string]interface{}{
				field: param, // Expects array parameter
			},
		}, nil

	case astql.IsNull:
		// Check for NULL - use exists query
		return map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": map[string]interface{}{
					"exists": map[string]interface{}{
						"field": field,
					},
				},
			},
		}, nil

	case astql.IsNotNull:
		// Check for NOT NULL - use exists query
		return map[string]interface{}{
			"exists": map[string]interface{}{
				"field": field,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported operator for OpenSearch: %s", cond.Operator)
	}
}

// buildBoolQuery converts a condition group to bool query.
func (p *Provider) buildBoolQuery(group astql.ConditionGroup) (map[string]interface{}, error) {
	boolQuery := make(map[string]interface{})

	clauseType := "must" // Default to AND
	if group.Logic == astql.OR {
		clauseType = "should"
	}

	clauses := make([]map[string]interface{}, 0, len(group.Conditions))
	for _, cond := range group.Conditions {
		clause, err := p.buildQuery(cond)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}

	boolQuery[clauseType] = clauses

	// For OR queries, set minimum_should_match
	if group.Logic == astql.OR {
		boolQuery["minimum_should_match"] = 1
	}

	return map[string]interface{}{"bool": boolQuery}, nil
}

// extractDocumentID tries to extract document ID from WHERE clause.
func (*Provider) extractDocumentID(ast *AST) (string, error) {
	if ast.WhereClause == nil {
		return "", fmt.Errorf("no WHERE clause")
	}

	cond, ok := ast.WhereClause.(astql.Condition)
	if !ok {
		return "", fmt.Errorf("complex WHERE clause")
	}

	if cond.Field.Name != ast.TableConfig.IDField {
		return "", fmt.Errorf("WHERE clause must use ID field")
	}

	if cond.Operator != astql.EQ {
		return "", fmt.Errorf("ID lookup requires equality operator")
	}

	return fmt.Sprintf(":%s", cond.Value.Name), nil
}

// buildUpdateScript creates update script from field updates.
func buildUpdateScript(updates map[string]interface{}) string {
	parts := make([]string, 0, 8)
	for field := range updates {
		parts = append(parts, fmt.Sprintf("ctx._source.%s = params.%s", field, field))
	}
	return strings.Join(parts, "; ")
}

// encodeQuery converts QueryResult to string representation.
func encodeQuery(q *QueryResult) string {
	parts := []string{q.Operation, q.Index}

	// Add query details
	if q.Query != nil {
		parts = append(parts, fmt.Sprintf("query=%v", q.Query))
	}
	if q.Document != nil {
		parts = append(parts, fmt.Sprintf("doc=%v", q.Document))
	}
	if q.Sort != nil {
		parts = append(parts, fmt.Sprintf("sort=%v", q.Sort))
	}
	if q.From != nil {
		parts = append(parts, fmt.Sprintf("from=%d", *q.From))
	}
	if q.Size != nil {
		parts = append(parts, fmt.Sprintf("size=%d", *q.Size))
	}

	return strings.Join(parts, " ")
}

// extractRequiredParams extracts parameter names from the AST.
func extractRequiredParams(ast *AST) []string {
	params := make(map[string]bool)

	// From WHERE clause
	extractParamsFromCondition(ast.WhereClause, params)

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

	// Convert to slice
	result := make([]string, 0, len(params))
	for param := range params {
		result = append(result, param)
	}

	return result
}

// extractParamsFromCondition recursively extracts params from conditions.
func extractParamsFromCondition(cond astql.ConditionItem, params map[string]bool) {
	if cond == nil {
		return
	}

	switch c := cond.(type) {
	case astql.Condition:
		// Don't extract params for NULL checks
		if c.Operator != astql.IsNull && c.Operator != astql.IsNotNull {
			params[c.Value.Name] = true
		}
	case astql.ConditionGroup:
		for _, subcond := range c.Conditions {
			extractParamsFromCondition(subcond, params)
		}
	}
}
