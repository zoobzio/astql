package redis

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// Default values used as placeholders.
const (
	defaultTTL = 3600 // 1 hour default TTL
)

// Provider renders Redis commands from ASTQL AST.
type Provider struct {
	// TableConfigs maps table names to their Redis configurations.
	TableConfigs map[string]TableConfig
}

// NewProvider creates a new Redis provider.
func NewProvider() *Provider {
	return &Provider{
		TableConfigs: make(map[string]TableConfig),
	}
}

// RegisterTable configures how a table maps to Redis.
func (p *Provider) RegisterTable(tableName string, config TableConfig) {
	p.TableConfigs[tableName] = config
}

// CommandResult represents a Redis command with its arguments.
//
//nolint:govet // field alignment not critical for command results
type CommandResult struct {
	Args    []interface{}
	Command string
	TTL     *int // Optional TTL for SET operations
}

// Render converts an AST to Redis commands.
func (p *Provider) Render(ast *AST) (*astql.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	// Get table configuration
	config, exists := p.TableConfigs[ast.Target.Name]
	if !exists {
		return nil, fmt.Errorf("table '%s' not registered for Redis", ast.Target.Name)
	}
	ast.TableConfig = &config

	// Render based on operation
	var commands []CommandResult
	var err error

	switch ast.Operation {
	case astql.OpSelect:
		commands, err = p.renderSelect(ast)
	case astql.OpInsert:
		commands, err = p.renderInsert(ast)
	case astql.OpUpdate:
		commands, err = p.renderUpdate(ast)
	case astql.OpDelete:
		commands, err = p.renderDelete(ast)
	case astql.OpCount:
		commands, err = p.renderCount(ast)
	case astql.OpListen:
		commands, err = p.renderListen(ast)
	case astql.OpNotify:
		commands, err = p.renderNotify(ast)
	case astql.OpUnlisten:
		commands, err = p.renderUnlisten(ast)
	default:
		return nil, fmt.Errorf("unsupported operation for Redis: %s", ast.Operation)
	}

	if err != nil {
		return nil, err
	}

	// Convert to QueryResult
	// For Redis, we'll encode commands as a special format in SQL field
	sql := encodeCommands(commands)

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

func (p *Provider) renderSelect(ast *AST) ([]CommandResult, error) {
	// Extract key from WHERE clause or use table pattern directly
	key, err := p.extractKey(ast)
	if err != nil {
		// For some Redis patterns (like single sets/sorted sets),
		// the key might be the table pattern itself
		if ast.TableConfig.KeyPattern != "" && !strings.Contains(ast.TableConfig.KeyPattern, "{") {
			// Pattern doesn't have placeholders, use it directly
			key = ast.TableConfig.KeyPattern
		} else {
			return nil, err
		}
	}

	var cmd CommandResult
	switch ast.TableConfig.DataType {
	case TypeString:
		cmd = CommandResult{Command: "GET", Args: []interface{}{key}}
	case TypeHash:
		switch len(ast.Fields) {
		case 0:
			// Select all fields
			cmd = CommandResult{Command: "HGETALL", Args: []interface{}{key}}
		case 1:
			// Select specific field
			cmd = CommandResult{Command: "HGET", Args: []interface{}{key, ast.Fields[0].Name}}
		default:
			// Select multiple fields
			args := []interface{}{key}
			for _, field := range ast.Fields {
				args = append(args, field.Name)
			}
			cmd = CommandResult{Command: "HMGET", Args: args}
		}
	case TypeSet:
		cmd = CommandResult{Command: "SMEMBERS", Args: []interface{}{key}}
	case TypeZSet:
		// Support LIMIT/OFFSET for sorted sets
		start := 0
		stop := -1
		if ast.Offset != nil {
			start = *ast.Offset
		}
		if ast.Limit != nil {
			stop = start + *ast.Limit - 1
		}
		cmd = CommandResult{Command: "ZRANGE", Args: []interface{}{key, start, stop, "WITHSCORES"}}
	case TypeList:
		// Support LIMIT/OFFSET for lists
		start := 0
		stop := -1
		if ast.Offset != nil {
			start = *ast.Offset
		}
		if ast.Limit != nil {
			stop = start + *ast.Limit - 1
		}
		cmd = CommandResult{Command: "LRANGE", Args: []interface{}{key, start, stop}}
	default:
		return nil, fmt.Errorf("SELECT not supported for Redis type %s", ast.TableConfig.DataType)
	}

	return []CommandResult{cmd}, nil
}

func (p *Provider) renderInsert(ast *AST) ([]CommandResult, error) {
	if len(ast.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires values")
	}

	commands := make([]CommandResult, 0, len(ast.Values))

	for _, valueSet := range ast.Values {
		// Extract key from values
		key, err := p.extractKeyFromValues(ast, valueSet)
		if err != nil {
			return nil, err
		}

		// Check for TTL from AST
		var ttl *int
		if ast.TTLParam != nil {
			// TTL will be resolved at execution time from parameters
			// For now, we'll use a placeholder
			ttlValue := defaultTTL
			ttl = &ttlValue
		}

		var cmd CommandResult
		switch ast.TableConfig.DataType {
		case TypeString:
			// Find the value field (first non-ID field)
			var value interface{}
			for field, param := range valueSet {
				if field.Name != ast.TableConfig.IDField {
					value = fmt.Sprintf(":%s", param.Name)
					break
				}
			}
			if ttl != nil {
				cmd = CommandResult{Command: "SETEX", Args: []interface{}{key, *ttl, value}}
			} else {
				cmd = CommandResult{Command: "SET", Args: []interface{}{key, value}}
			}

		case TypeHash:
			// Build HSET arguments
			args := []interface{}{key}
			for field, param := range valueSet {
				if field.Name != ast.TableConfig.IDField {
					args = append(args, field.Name, fmt.Sprintf(":%s", param.Name))
				}
			}
			cmd = CommandResult{Command: "HSET", Args: args, TTL: ttl}

		case TypeSet:
			// Build SADD arguments
			args := []interface{}{key}
			for field, param := range valueSet {
				if field.Name != ast.TableConfig.IDField {
					args = append(args, fmt.Sprintf(":%s", param.Name))
				}
			}
			cmd = CommandResult{Command: "SADD", Args: args, TTL: ttl}

		case TypeZSet:
			// Extract score from AST
			var score float64
			if ast.ScoreParam != nil {
				// Score will be resolved at execution time from parameters
				// For now, we'll use a placeholder
				score = 1.0
			}

			// Build ZADD arguments
			args := []interface{}{key}
			// For sorted sets, we need at least one member
			// If we only have the ID field, use it as the member value
			memberAdded := false
			for field, param := range valueSet {
				if field.Name != ast.TableConfig.IDField {
					args = append(args, score, fmt.Sprintf(":%s", param.Name))
					memberAdded = true
				}
			}
			// If no member was added (only ID field exists), use ID as member
			if !memberAdded {
				for field, param := range valueSet {
					if field.Name == ast.TableConfig.IDField {
						args = append(args, score, fmt.Sprintf(":%s", param.Name))
						break
					}
				}
			}
			cmd = CommandResult{Command: "ZADD", Args: args, TTL: ttl}

		case TypeList:
			// Build RPUSH arguments (append to list)
			args := []interface{}{key}
			for field, param := range valueSet {
				if field.Name != ast.TableConfig.IDField {
					args = append(args, fmt.Sprintf(":%s", param.Name))
				}
			}
			cmd = CommandResult{Command: "RPUSH", Args: args, TTL: ttl}

		default:
			return nil, fmt.Errorf("INSERT not supported for Redis type %s", ast.TableConfig.DataType)
		}

		commands = append(commands, cmd)
	}

	return commands, nil
}

func (p *Provider) renderUpdate(ast *AST) ([]CommandResult, error) {
	// Redis doesn't have true UPDATE - we use SET operations
	// Convert UPDATE to INSERT-like operations
	if len(ast.Updates) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one field")
	}

	// Extract key from WHERE clause
	_, err := p.extractKey(ast)
	if err != nil {
		return nil, err
	}

	// Build value set from updates
	valueSet := make(map[astql.Field]astql.Param)
	for field, param := range ast.Updates {
		valueSet[field] = param
	}

	// Add the ID field from WHERE clause
	if ast.WhereClause != nil {
		if cond, ok := ast.WhereClause.(astql.Condition); ok && cond.Field.Name == ast.TableConfig.IDField {
			valueSet[cond.Field] = cond.Value
		}
	}

	// Create a temporary AST for INSERT operation
	insertAST := &AST{
		QueryAST: &astql.QueryAST{
			Operation: astql.OpInsert,
			Target:    ast.Target,
			Values:    []map[astql.Field]astql.Param{valueSet},
		},
		TableConfig: ast.TableConfig,
	}

	return p.renderInsert(insertAST)
}

func (p *Provider) renderDelete(ast *AST) ([]CommandResult, error) {
	// Extract key from WHERE clause
	key, err := p.extractKey(ast)
	if err != nil {
		return nil, err
	}

	var cmd CommandResult
	switch ast.TableConfig.DataType {
	case TypeString, TypeHash, TypeSet, TypeZSet, TypeList:
		cmd = CommandResult{Command: "DEL", Args: []interface{}{key}}
	default:
		return nil, fmt.Errorf("DELETE not supported for Redis type %s", ast.TableConfig.DataType)
	}

	return []CommandResult{cmd}, nil
}

func (p *Provider) renderCount(ast *AST) ([]CommandResult, error) {
	// For COUNT, we typically count all keys matching a pattern
	// or the size of a specific data structure

	if ast.WhereClause != nil {
		// If there's a WHERE clause, try to extract specific key
		key, err := p.extractKey(ast)
		if err == nil {
			// Count elements in a specific key
			var cmd CommandResult
			switch ast.TableConfig.DataType {
			case TypeHash:
				cmd = CommandResult{Command: "HLEN", Args: []interface{}{key}}
			case TypeSet:
				cmd = CommandResult{Command: "SCARD", Args: []interface{}{key}}
			case TypeZSet:
				cmd = CommandResult{Command: "ZCARD", Args: []interface{}{key}}
			case TypeList:
				cmd = CommandResult{Command: "LLEN", Args: []interface{}{key}}
			default:
				return nil, fmt.Errorf("COUNT not supported for Redis type %s", ast.TableConfig.DataType)
			}
			return []CommandResult{cmd}, nil
		}
	}

	// Count all keys matching the table pattern
	pattern := strings.ReplaceAll(ast.TableConfig.KeyPattern, "{"+ast.TableConfig.IDField+"}", "*")

	// Note: In production, you'd want to use SCAN instead of KEYS
	return []CommandResult{
		{Command: "KEYS", Args: []interface{}{pattern}},
		// The result would need to be counted client-side
	}, nil
}

func (*Provider) renderListen(ast *AST) ([]CommandResult, error) {
	// Redis SUBSCRIBE
	channel := ast.Target.Name + "_changes"
	return []CommandResult{
		{Command: "SUBSCRIBE", Args: []interface{}{channel}},
	}, nil
}

func (*Provider) renderNotify(ast *AST) ([]CommandResult, error) {
	// Redis PUBLISH
	channel := ast.Target.Name + "_changes"
	if ast.NotifyPayload == nil {
		return nil, fmt.Errorf("NOTIFY requires a payload")
	}

	return []CommandResult{
		{Command: "PUBLISH", Args: []interface{}{channel, fmt.Sprintf(":%s", ast.NotifyPayload.Name)}},
	}, nil
}

func (*Provider) renderUnlisten(ast *AST) ([]CommandResult, error) {
	// Redis UNSUBSCRIBE
	channel := ast.Target.Name + "_changes"
	return []CommandResult{
		{Command: "UNSUBSCRIBE", Args: []interface{}{channel}},
	}, nil
}

// extractKey extracts the Redis key from WHERE clause.
func (*Provider) extractKey(ast *AST) (string, error) {
	if ast.WhereClause == nil {
		return "", fmt.Errorf("Redis operations require WHERE clause with %s field", ast.TableConfig.IDField)
	}

	// Only support simple equality on ID field
	cond, ok := ast.WhereClause.(astql.Condition)
	if !ok {
		return "", fmt.Errorf("Redis only supports simple equality conditions")
	}

	if cond.Field.Name != ast.TableConfig.IDField {
		return "", fmt.Errorf("Redis WHERE clause must use %s field", ast.TableConfig.IDField)
	}

	if cond.Operator != astql.EQ {
		return "", fmt.Errorf("Redis only supports equality (=) operator")
	}

	// Build key from pattern
	key := strings.Replace(ast.TableConfig.KeyPattern, "{"+ast.TableConfig.IDField+"}", fmt.Sprintf(":%s", cond.Value.Name), 1)

	return key, nil
}

// extractKeyFromValues extracts the Redis key from value set.
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
		return "", fmt.Errorf("Redis INSERT requires %s field", ast.TableConfig.IDField)
	}

	// Build key from pattern
	key := strings.Replace(ast.TableConfig.KeyPattern, "{"+ast.TableConfig.IDField+"}", fmt.Sprintf(":%s", idParam.Name), 1)

	return key, nil
}

// encodeCommands converts Redis commands to a string representation.
// In a real implementation, this might return structured data.
func encodeCommands(commands []CommandResult) string {
	parts := make([]string, 0, len(commands))
	for _, cmd := range commands {
		argStrs := make([]string, len(cmd.Args))
		for i, arg := range cmd.Args {
			argStrs[i] = fmt.Sprintf("%v", arg)
		}
		cmdStr := fmt.Sprintf("%s %s", cmd.Command, strings.Join(argStrs, " "))
		if cmd.TTL != nil {
			cmdStr += fmt.Sprintf(" EX %d", *cmd.TTL)
		}
		parts = append(parts, cmdStr)
	}
	return strings.Join(parts, "; ")
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

	// From NOTIFY payload
	if ast.NotifyPayload != nil {
		params[ast.NotifyPayload.Name] = true
	}

	// From TTL parameter
	if ast.TTLParam != nil {
		params[ast.TTLParam.Name] = true
	}

	// From Score parameter
	if ast.ScoreParam != nil {
		params[ast.ScoreParam.Name] = true
	}

	// Convert to slice
	result := make([]string, 0, len(params))
	for param := range params {
		result = append(result, param)
	}

	return result
}
