package postgres

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
)

func TestNewAST(t *testing.T) {

	base := &types.QueryAST{
		Operation: types.OpSelect,
		Target:    types.Table{Name: "users"},
	}

	ast := NewAST(base)

	if ast.QueryAST != base {
		t.Error("Expected QueryAST to be set")
	}
	if ast.OnConflict != nil {
		t.Error("Expected OnConflict to be nil")
	}
	if len(ast.Joins) != 0 {
		t.Error("Expected Joins to be empty")
	}
	if ast.Distinct {
		t.Error("Expected Distinct to be false")
	}
}

func TestValidateSelect(t *testing.T) {

	ast := &AST{
		QueryAST: &types.QueryAST{
			Operation: types.OpSelect,
			Target:    types.Table{Name: "users"},
		},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid SELECT, got: %v", err)
	}
}

func TestValidateInsertWithOnConflict(t *testing.T) {

	t.Run("Valid ON CONFLICT", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpInsert,
				Target:    types.Table{Name: "users"},
				Values: []map[types.Field]types.Param{
					{types.Field{Name: "name"}: types.Param{Name: "userName"}},
				},
			},
			OnConflict: &ConflictClause{
				Action:  DoNothing,
				Columns: []types.Field{{Name: "email"}},
			},
		}

		err := ast.Validate()
		if err != nil {
			t.Errorf("Expected no error for valid ON CONFLICT, got: %v", err)
		}
	})

	t.Run("Invalid ON CONFLICT - no columns", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpInsert,
				Target:    types.Table{Name: "users"},
				Values: []map[types.Field]types.Param{
					{types.Field{Name: "name"}: types.Param{Name: "userName"}},
				},
			},
			OnConflict: &ConflictClause{
				Action:  DoNothing,
				Columns: []types.Field{}, // Empty columns
			},
		}

		err := ast.Validate()
		if err == nil {
			t.Error("Expected error for ON CONFLICT without columns")
		}
		if !strings.Contains(err.Error(), "ON CONFLICT requires at least one column") {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})
}

func TestValidateUpdateDelete(t *testing.T) {

	testCases := []struct {
		name      string
		operation types.Operation
	}{
		{"UPDATE", types.OpUpdate},
		{"DELETE", types.OpDelete},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("Valid "+tc.name, func(t *testing.T) {
				ast := &AST{
					QueryAST: &types.QueryAST{
						Operation: tc.operation,
						Target:    types.Table{Name: "users"},
					},
				}
				if tc.operation == types.OpUpdate {
					ast.Updates = map[types.Field]types.Param{
						{Name: "name"}: {Name: "newName"},
					}
				}

				err := ast.Validate()
				if err != nil {
					t.Errorf("Expected no error for valid %s, got: %v", tc.name, err)
				}
			})

			t.Run(tc.name+" with invalid SELECT features", func(t *testing.T) {
				testSubCases := []struct {
					name string
					ast  *AST
				}{
					{
						"DISTINCT",
						&AST{
							QueryAST: &types.QueryAST{
								Operation: tc.operation,
								Target:    types.Table{Name: "users"},
							},
							Distinct: true,
						},
					},
					{
						"JOIN",
						&AST{
							QueryAST: &types.QueryAST{
								Operation: tc.operation,
								Target:    types.Table{Name: "users"},
							},
							Joins: []Join{{Type: InnerJoin, Table: types.Table{Name: "posts"}}},
						},
					},
					{
						"GROUP BY",
						&AST{
							QueryAST: &types.QueryAST{
								Operation: tc.operation,
								Target:    types.Table{Name: "users"},
							},
							GroupBy: []types.Field{types.Field{Name: "status"}},
						},
					},
				}

				for _, subCase := range testSubCases {
					t.Run(subCase.name, func(t *testing.T) {
						if tc.operation == types.OpUpdate {
							subCase.ast.Updates = map[types.Field]types.Param{
								{Name: "name"}: {Name: "newName"},
							}
						}

						err := subCase.ast.Validate()
						if err == nil {
							t.Errorf("Expected error for %s with %s", tc.name, subCase.name)
						}
						expectedMsg := tc.name + " cannot have SELECT features"
						if !strings.Contains(err.Error(), expectedMsg) {
							t.Errorf("Expected error containing '%s', got: %v", expectedMsg, err)
						}
					})
				}
			})
		})
	}
}

func TestValidateComplexStructures(t *testing.T) {

	t.Run("SELECT with all PostgreSQL features", func(t *testing.T) {
		ast := &AST{
			QueryAST: &types.QueryAST{
				Operation: types.OpSelect,
				Target:    types.Table{Name: "users", Alias: "u"},
				Fields: []types.Field{
					types.Field{Name: "id"}.WithTable("u"),
					types.Field{Name: "name"}.WithTable("u"),
				},
				WhereClause: astql.C(types.Field{Name: "active"}, types.EQ, types.Param{Name: "isActive"}),
				Ordering: []types.OrderBy{
					{Field: types.Field{Name: "created_at"}, Direction: types.DESC},
				},
				Limit:  intPtrAST(10),
				Offset: intPtrAST(20),
			},
			Joins: []Join{
				{
					Type:  InnerJoin,
					Table: types.Table{Name: "posts", Alias: "p"},
					On:    astql.C(types.Field{Name: "user_id"}.WithTable("p"), types.EQ, types.Param{Name: "userId"}),
				},
			},
			GroupBy: []types.Field{types.Field{Name: "status"}},
			Having: []types.Condition{
				astql.C(types.Field{Name: "count"}, types.GT, types.Param{Name: "minCount"}),
			},
			FieldExpressions: []FieldExpression{
				{
					Aggregate: AggSum,
					Field:     types.Field{Name: "amount"},
					Alias:     "total_amount",
				},
			},
			Returning: []types.Field{types.Field{Name: "id"}},
			Distinct:  true,
		}

		err := ast.Validate()
		if err != nil {
			t.Errorf("Expected no error for complex SELECT, got: %v", err)
		}
	})
}

func intPtrAST(i int) *int {
	return &i
}
