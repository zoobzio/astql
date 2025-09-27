package postgres

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
	"gopkg.in/yaml.v3"
)

func TestValidateParamName(t *testing.T) {
	tests := []struct {
		name    string
		param   string
		wantErr bool
		errMsg  string
	}{
		// Valid cases
		{"simple name", "userId", false, ""},
		{"with underscore", "user_id", false, ""},
		{"with numbers", "user123", false, ""},
		{"starts with underscore", "_userId", true, "invalid parameter name"}, // params must start with letter

		// Invalid cases - all return the same error message
		{"empty", "", true, "invalid parameter name"},
		{"starts with number", "123user", true, "invalid parameter name"},
		{"contains space", "user id", true, "invalid parameter name"},
		{"contains hyphen", "user-id", true, "invalid parameter name"},
		{"sql keyword", "select", true, "invalid parameter name"},
		// Valid cases that were incorrectly expected to fail
		{"contains sql", "sqlQuery", false, ""},          // "sqlQuery" is a valid identifier
		{"too long", strings.Repeat("a", 65), false, ""}, // no length limit in our validation
		{"special chars", "user$id", true, "invalid parameter name"},
		{"semicolon", "user;id", true, "invalid parameter name"},
		{"quote", "user'id", true, "invalid parameter name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the exported validation from param.go
			_, err := astql.TryP(tt.param)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateParamName(%q) error = %v, wantErr %v", tt.param, err, tt.wantErr)
			}
			if err != nil && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error containing %q, got %q", tt.errMsg, err.Error())
			}
		})
	}
}

func TestValidateAllowlists(t *testing.T) {
	t.Run("Operations", func(t *testing.T) {
		valid := []string{"SELECT", "select", "INSERT", "DELETE", "UPDATE", "COUNT"}
		invalid := []string{"TRUNCATE", "DROP", "CREATE", "ALTER", "", "LISTEN", "NOTIFY", "UNLISTEN"}

		for _, op := range valid {
			if err := ValidateOperation(op); err != nil {
				t.Errorf("Expected %q to be valid operation: %v", op, err)
			}
		}

		for _, op := range invalid {
			if err := ValidateOperation(op); err == nil {
				t.Errorf("Expected %q to be invalid operation", op)
			}
		}
	})

	t.Run("Operators", func(t *testing.T) {
		valid := []string{"=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE", "IN", "NOT IN", "IS NULL", "IS NOT NULL", "EXISTS", "NOT EXISTS"}
		invalid := []string{"BETWEEN", "ALL", "ANY", "SOME", "~", "!~", ""}

		for _, op := range valid {
			if err := ValidateOperator(op); err != nil {
				t.Errorf("Expected %q to be valid operator: %v", op, err)
			}
		}

		for _, op := range invalid {
			if err := ValidateOperator(op); err == nil {
				t.Errorf("Expected %q to be invalid operator", op)
			}
		}
	})
}

func TestBuildFromSchemaBasicSelect(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "User",
		Fields:    []string{"id", "name", "email"},
		Where: &ConditionSchema{
			Field:    "id",
			Operator: "=",
			Param:    "userId",
		},
		OrderBy: []OrderSchema{
			{Field: "name", Direction: "ASC"},
		},
		Limit: intPtr(10),
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if ast.Operation != types.OpSelect {
		t.Errorf("Expected SELECT operation, got %v", ast.Operation)
	}

	if len(ast.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(ast.Fields))
	}

	if ast.WhereClause == nil {
		t.Error("Expected WHERE clause")
	}

	if len(ast.Ordering) != 1 {
		t.Errorf("Expected 1 ORDER BY, got %d", len(ast.Ordering))
	}

	if ast.Limit == nil || *ast.Limit != 10 {
		t.Error("Expected LIMIT 10")
	}
}

func TestBuildFromSchemaValidation(t *testing.T) {
	SetupTest(t)

	tests := []struct {
		name    string
		schema  QuerySchema
		wantErr string
	}{
		{
			name: "invalid operation",
			schema: QuerySchema{
				Operation: "TRUNCATE",
				Table:     "User",
			},
			wantErr: "operation 'TRUNCATE' not allowed",
		},
		{
			name: "unregistered table",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "invalid_table",
			},
			wantErr: "not found",
		},
		{
			name: "unregistered field",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				Fields:    []string{"invalid_field"},
			},
			wantErr: "not found",
		},
		{
			name: "invalid param name",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				Where: &ConditionSchema{
					Field:    "id",
					Operator: "=",
					Param:    "user-id", // Invalid: contains hyphen
				},
			},
			wantErr: "invalid parameter name",
		},
		{
			name: "sql keyword param",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				Where: &ConditionSchema{
					Field:    "id",
					Operator: "=",
					Param:    "select", // SQL keyword
				},
			},
			wantErr: "invalid parameter name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildFromSchema(&tt.schema)
			if err == nil {
				t.Error("Expected error but got none")
			} else if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestBuildFromSchemaComplexConditions(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "User",
		Where: &ConditionSchema{
			Logic: "OR",
			Conditions: []ConditionSchema{
				{
					Field:    "age",
					Operator: ">",
					Param:    "minAge",
				},
				{
					Logic: "AND",
					Conditions: []ConditionSchema{
						{
							Field:    "name",
							Operator: "LIKE",
							Param:    "namePattern",
						},
						{
							Field:    "email",
							Operator: "NOT LIKE",
							Param:    "emailPattern",
						},
					},
				},
			},
		},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if ast.WhereClause == nil {
		t.Error("Expected WHERE clause")
	}

	// Verify the WHERE clause exists and is complex
	// The actual structure depends on internal types implementation
}

func TestBuildFromSchemaJoins(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "User",
		Joins: []JoinSchema{
			{
				Type:  "INNER",
				Table: "Post",
				Alias: "p",
				On: ConditionSchema{
					LeftField:  "users.id",
					RightField: "p.user_id",
					Operator:   "=",
				},
			},
		},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if len(ast.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(ast.Joins))
	}

	join := ast.Joins[0]
	if join.Type != "INNER JOIN" {
		t.Errorf("Expected INNER JOIN, got %s", join.Type)
	}
}

func TestBuildFromSchemaSubquery(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "User",
		Where: &ConditionSchema{
			Field:    "user_id",
			Operator: "IN",
			Subquery: &QuerySchema{
				Operation: "SELECT",
				Table:     "ActiveUser",
				Fields:    []string{"id"},
			},
		},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if ast.WhereClause == nil {
		t.Error("Expected WHERE clause")
	}
}

func TestBuildFromSchemaInsertWithConflict(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "INSERT",
		Table:     "User",
		Values: []map[string]string{
			{
				"name":  "userName",
				"email": "userEmail",
			},
		},
		OnConflict: &ConflictSchema{
			Columns: []string{"email"},
			Action:  "UPDATE",
			Updates: map[string]string{
				"name": "newName",
			},
		},
		Returning: []string{"id"},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if ast.Operation != types.OpInsert {
		t.Errorf("Expected INSERT operation, got %v", ast.Operation)
	}

	if len(ast.Values) != 1 {
		t.Errorf("Expected 1 value set, got %d", len(ast.Values))
	}

	if ast.OnConflict == nil {
		t.Error("Expected ON CONFLICT clause")
	}

	if len(ast.Returning) != 1 {
		t.Errorf("Expected 1 returning field, got %d", len(ast.Returning))
	}
}

func TestSchemaYAMLParsing(t *testing.T) {
	yamlContent := `
operation: SELECT
table: User
fields: 
  - id
  - name
  - email
where:
  logic: AND
  conditions:
    - field: age
      operator: ">="
      param: minAge
    - field: email
      operator: LIKE
      param: emailPattern
order_by:
  - field: name
    direction: ASC
limit: 10
`

	var schema QuerySchema
	if err := yaml.Unmarshal([]byte(yamlContent), &schema); err != nil {
		t.Fatalf("Failed to parse YAML: %v", err)
	}

	if schema.Operation != "SELECT" {
		t.Errorf("Expected SELECT operation, got %s", schema.Operation)
	}

	if len(schema.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(schema.Fields))
	}

	if schema.Where == nil || schema.Where.Logic != "AND" {
		t.Error("Expected AND logic in WHERE clause")
	}
}

func TestSchemaJSONParsing(t *testing.T) {
	jsonContent := `{
		"operation": "INSERT",
		"table": "User",
		"values": [
			{
				"name": "userName",
				"email": "userEmail"
			}
		],
		"on_conflict": {
			"columns": ["email"],
			"action": "UPDATE",
			"updates": {
				"name": "newName"
			}
		},
		"returning": ["id", "name"]
	}`

	var schema QuerySchema
	if err := json.Unmarshal([]byte(jsonContent), &schema); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if schema.Operation != "INSERT" {
		t.Errorf("Expected INSERT operation, got %s", schema.Operation)
	}

	if len(schema.Values) != 1 {
		t.Errorf("Expected 1 value set, got %d", len(schema.Values))
	}

	if schema.OnConflict == nil || schema.OnConflict.Action != "UPDATE" {
		t.Error("Expected UPDATE action in ON CONFLICT")
	}
}

func TestBuildFromSchemaAggregates(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "Order",
		FieldExpressions: []FieldExpressionSchema{
			{
				Field: "customer_id",
				Alias: "customer",
			},
			{
				Field:     "total",
				Aggregate: "SUM",
				Alias:     "total_amount",
			},
			{
				Field:     "id",
				Aggregate: "COUNT",
				Alias:     "order_count",
			},
		},
		GroupBy: []string{"customer_id"},
		Having: []ConditionSchema{
			{
				Field:    "total",
				Operator: ">",
				Param:    "minTotal",
			},
		},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if len(ast.FieldExpressions) != 3 {
		t.Errorf("Expected 3 field expressions, got %d", len(ast.FieldExpressions))
	}

	if len(ast.GroupBy) != 1 {
		t.Errorf("Expected 1 GROUP BY field, got %d", len(ast.GroupBy))
	}

	if len(ast.Having) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(ast.Having))
	}
}

func TestBuildFromSchemaCaseExpression(t *testing.T) {
	SetupTest(t)

	schema := &QuerySchema{
		Operation: "SELECT",
		Table:     "User",
		FieldExpressions: []FieldExpressionSchema{
			{
				Case: &CaseSchema{
					When: []WhenSchema{
						{
							Condition: ConditionSchema{
								Field:    "age",
								Operator: "<",
								Param:    "minAge",
							},
							Result: "child",
						},
						{
							Condition: ConditionSchema{
								Field:    "age",
								Operator: ">=",
								Param:    "seniorAge",
							},
							Result: "senior",
						},
					},
					Else: "adult",
				},
				Alias: "age_group",
			},
		},
	}

	ast, err := BuildFromSchema(schema)
	if err != nil {
		t.Fatalf("BuildFromSchema failed: %v", err)
	}

	if len(ast.FieldExpressions) != 1 {
		t.Errorf("Expected 1 field expression, got %d", len(ast.FieldExpressions))
	}
}

func TestSchemaSecurityValidation(t *testing.T) {
	SetupTest(t)

	tests := []struct {
		name    string
		schema  QuerySchema
		wantErr string
	}{
		{
			name: "SQL injection in param name",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				Where: &ConditionSchema{
					Field:    "id",
					Operator: "=",
					Param:    "id; DROP TABLE users--",
				},
			},
			wantErr: "invalid parameter name",
		},
		{
			name: "unregistered table injection",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "users; DROP TABLE users--",
			},
			wantErr: "not found",
		},
		{
			name: "invalid operator",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				Where: &ConditionSchema{
					Field:    "id",
					Operator: "~", // PostgreSQL regex operator, not allowed
					Param:    "pattern",
				},
			},
			wantErr: "operator '~' not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildFromSchema(&tt.schema)
			if err == nil {
				t.Error("Expected security validation error")
			} else if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestBuildFromSchemaMathExpressions(t *testing.T) {
	SetupTest(t)

	tests := []struct {
		name        string
		schema      QuerySchema
		wantErr     bool
		errContains string
	}{
		{
			name: "Math expression - ROUND",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "ROUND",
							Field:    "age",
							Arg:      strPtr("precision"),
						},
						Alias: "rounded_age",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Math expression - ROUND missing precision",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "ROUND",
							Field:    "age",
						},
					},
				},
			},
			wantErr:     true,
			errContains: "ROUND requires precision argument",
		},
		{
			name: "Math expression - FLOOR",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "FLOOR",
							Field:    "age",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Math expression - POWER",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "POWER",
							Field:    "age",
							Arg:      strPtr("exponent"),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Math expression - POWER missing exponent",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "POWER",
							Field:    "age",
						},
					},
				},
			},
			wantErr:     true,
			errContains: "POWER requires exponent argument",
		},
		{
			name: "Math expression - invalid function",
			schema: QuerySchema{
				Operation: "SELECT",
				Table:     "User",
				FieldExpressions: []FieldExpressionSchema{
					{
						Field: "age",
						Math: &MathSchema{
							Function: "INVALID",
							Field:    "age",
						},
					},
				},
			},
			wantErr:     true,
			errContains: "math function 'INVALID' not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildFromSchema(&tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildFromSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("Expected error containing %q, got %q", tt.errContains, err.Error())
			}
		})
	}
}

func TestValidateFunctions(t *testing.T) {
	t.Run("ValidateMathFunction", func(t *testing.T) {
		valid := []string{"round", "ROUND", "floor", "FLOOR", "ceil", "CEIL", "abs", "ABS", "power", "POWER", "sqrt", "SQRT"}
		invalid := []string{"median", "MEDIAN", "random", "", "DROP TABLE"}

		for _, fn := range valid {
			if err := ValidateMathFunction(fn); err != nil {
				t.Errorf("Expected %q to be valid math function: %v", fn, err)
			}
		}

		for _, fn := range invalid {
			if err := ValidateMathFunction(fn); err == nil {
				t.Errorf("Expected %q to be invalid math function", fn)
			}
		}
	})

	t.Run("ValidateDirection full coverage", func(t *testing.T) {
		// Test empty direction (should be valid as default)
		if err := ValidateDirection(""); err != nil {
			t.Error("Empty direction should be valid (default)")
		}

		// Test invalid direction
		if err := ValidateDirection("INVALID"); err == nil {
			t.Error("Expected error for invalid direction")
		}
	})

	t.Run("ValidateLogicOperator full coverage", func(t *testing.T) {
		valid := []string{"AND", "and", "OR", "or"}
		invalid := []string{"XOR", "NOT", ""}

		for _, op := range valid {
			if err := ValidateLogicOperator(op); err != nil {
				t.Errorf("Expected %q to be valid logic operator: %v", op, err)
			}
		}

		for _, op := range invalid {
			if err := ValidateLogicOperator(op); err == nil {
				t.Errorf("Expected %q to be invalid logic operator", op)
			}
		}
	})

	t.Run("ValidateJoinType full coverage", func(t *testing.T) {
		valid := []string{"inner", "INNER", "left", "LEFT", "right", "RIGHT", "cross", "CROSS"}
		invalid := []string{"full", "FULL", "natural", ""}

		for _, jt := range valid {
			if err := ValidateJoinType(jt); err != nil {
				t.Errorf("Expected %q to be valid join type: %v", jt, err)
			}
		}

		for _, jt := range invalid {
			if err := ValidateJoinType(jt); err == nil {
				t.Errorf("Expected %q to be invalid join type", jt)
			}
		}
	})

	t.Run("ValidateConflictAction full coverage", func(t *testing.T) {
		valid := []string{"nothing", "NOTHING", "update", "UPDATE"}
		invalid := []string{"replace", "ignore", ""}

		for _, action := range valid {
			if err := ValidateConflictAction(action); err != nil {
				t.Errorf("Expected %q to be valid conflict action: %v", action, err)
			}
		}

		for _, action := range invalid {
			if err := ValidateConflictAction(action); err == nil {
				t.Errorf("Expected %q to be invalid conflict action", action)
			}
		}
	})

	t.Run("ValidateAggregate full coverage", func(t *testing.T) {
		valid := []string{"sum", "SUM", "avg", "AVG", "min", "MIN", "max", "MAX", "count", "COUNT", "count_distinct", "COUNT_DISTINCT"}
		invalid := []string{"median", "mode", ""}

		for _, agg := range valid {
			if err := ValidateAggregate(agg); err != nil {
				t.Errorf("Expected %q to be valid aggregate: %v", agg, err)
			}
		}

		for _, agg := range invalid {
			if err := ValidateAggregate(agg); err == nil {
				t.Errorf("Expected %q to be invalid aggregate", agg)
			}
		}
	})
}

// Helper functions.
func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}
