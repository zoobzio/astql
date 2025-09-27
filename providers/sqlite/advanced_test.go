package sqlite

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/internal/types"
	"github.com/zoobzio/sentinel"
)

// Test structs.
type Product struct {
	ID    int     `db:"id"`
	Name  string  `db:"name"`
	Price float64 `db:"price"`
	Stock int     `db:"stock"`
}

type Sale struct {
	ID        int     `db:"id"`
	ProductID int     `db:"product_id"`
	Quantity  int     `db:"quantity"`
	Total     float64 `db:"total"`
}

func setupAdvancedTest(t *testing.T) {
	t.Helper()

	// Register test structs with sentinel
	sentinel.Inspect[Product]()
	sentinel.Inspect[Sale]()
}

func TestAggregates(t *testing.T) {
	setupAdvancedTest(t)

	t.Run("SUM aggregate", func(t *testing.T) {
		query := Select(astql.T("Sale")).
			FieldExpressions(
				Sum(astql.F("total")).As("total_sales"),
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SUM(") {
			t.Errorf("Expected SUM in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `AS "total_sales"`) {
			t.Errorf("Expected alias in SQL: %s", result.SQL)
		}
	})

	t.Run("Multiple aggregates", func(t *testing.T) {
		query := Select(astql.T("Product")).
			FieldExpressions(
				CountField(astql.F("id")).As("count"),
				Avg(astql.F("price")).As("avg_price"),
				Min(astql.F("price")).As("min_price"),
				Max(astql.F("price")).As("max_price"),
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		expectedFuncs := []string{"COUNT(", "AVG(", "MIN(", "MAX("}
		for _, fn := range expectedFuncs {
			if !strings.Contains(result.SQL, fn) {
				t.Errorf("Expected %s in SQL: %s", fn, result.SQL)
			}
		}
	})

	t.Run("COUNT DISTINCT", func(t *testing.T) {
		query := Select(astql.T("Sale")).
			FieldExpressions(
				CountDistinct(astql.F("product_id")).As("unique_products"),
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(DISTINCT") {
			t.Errorf("Expected COUNT(DISTINCT in SQL: %s", result.SQL)
		}
	})
}

func TestCaseExpression(t *testing.T) {
	setupAdvancedTest(t)

	query := Select(astql.T("Product")).
		FieldExpressions(
			FieldExpression{
				Field: astql.F("name"),
			},
			FieldExpression{
				Case: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: astql.C(astql.F("stock"), types.EQ, astql.P("zero")),
							Result:    astql.P("out_of_stock"),
						},
						{
							Condition: astql.C(astql.F("stock"), types.LT, astql.P("low")),
							Result:    astql.P("low_stock"),
						},
					},
					ElseClause: &[]types.Param{astql.P("in_stock")}[0],
				},
				Alias: "stock_status",
			},
		)

	ast, err := query.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	provider := NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	if !strings.Contains(result.SQL, "CASE WHEN") {
		t.Errorf("Expected CASE WHEN in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "THEN") {
		t.Errorf("Expected THEN in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "ELSE") {
		t.Errorf("Expected ELSE in SQL: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "END") {
		t.Errorf("Expected END in SQL: %s", result.SQL)
	}
}

func TestCoalesceNullIf(t *testing.T) {
	setupAdvancedTest(t)

	t.Run("COALESCE", func(t *testing.T) {
		query := Select(astql.T("Product")).
			FieldExpressions(
				FieldExpression{
					Coalesce: &CoalesceExpression{
						Values: []types.Param{
							astql.P("discount_price"),
							astql.P("regular_price"),
							astql.P("default_price"),
						},
					},
					Alias: "final_price",
				},
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COALESCE(") {
			t.Errorf("Expected COALESCE in SQL: %s", result.SQL)
		}
		// Should have 3 question marks for parameters
		if strings.Count(result.SQL, "?") != 3 {
			t.Errorf("Expected 3 parameters in SQL: %s", result.SQL)
		}
	})

	t.Run("NULLIF", func(t *testing.T) {
		query := Select(astql.T("Product")).
			FieldExpressions(
				FieldExpression{
					NullIf: &NullIfExpression{
						Value1: astql.P("price"),
						Value2: astql.P("zero"),
					},
					Alias: "non_zero_price",
				},
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NULLIF(") {
			t.Errorf("Expected NULLIF in SQL: %s", result.SQL)
		}
	})
}

func TestSubqueries(t *testing.T) {
	setupAdvancedTest(t)

	t.Run("IN subquery", func(t *testing.T) {
		subquery := Sub(
			Select(astql.T("Sale")).
				Fields(astql.F("product_id")).
				Where(astql.C(astql.F("quantity"), types.GT, astql.P("min_qty"))),
		)

		query := Select(astql.T("Product")).
			Where(CSub(astql.F("id"), types.IN, subquery))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, " IN (SELECT") {
			t.Errorf("Expected IN (SELECT in SQL: %s", result.SQL)
		}
	})

	t.Run("EXISTS subquery", func(t *testing.T) {
		subquery := Sub(
			Select(astql.T("Sale")).
				Where(CF(astql.F("product_id"), types.EQ, astql.F("id").WithTable("p"))),
		)

		query := Select(astql.T("Product", "p")).
			Where(CSubExists(types.EXISTS, subquery))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "EXISTS (SELECT") {
			t.Errorf("Expected EXISTS (SELECT in SQL: %s", result.SQL)
		}
	})

	t.Run("NOT IN subquery", func(t *testing.T) {
		subquery := Sub(
			Select(astql.T("Sale")).
				Fields(astql.F("product_id")).
				GroupBy(astql.F("product_id")).
				Having(astql.C(astql.F("product_id"), types.GT, astql.P("threshold"))),
		)

		query := Select(astql.T("Product")).
			Where(CSub(astql.F("id"), types.NotIn, subquery))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NOT IN (SELECT") {
			t.Errorf("Expected NOT IN (SELECT in SQL: %s", result.SQL)
		}
	})
}

func TestReturning(t *testing.T) {
	setupAdvancedTest(t)

	t.Run("INSERT RETURNING", func(t *testing.T) {
		query := Insert(astql.T("Product")).
			Values(map[types.Field]types.Param{
				astql.F("name"):  astql.P("productName"),
				astql.F("price"): astql.P("productPrice"),
			}).
			Returning(astql.F("id"), astql.F("name"))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("Expected RETURNING in SQL: %s", result.SQL)
		}
	})

	t.Run("UPDATE RETURNING", func(t *testing.T) {
		query := Update(astql.T("Product")).
			Set(astql.F("price"), astql.P("newPrice")).
			Where(astql.C(astql.F("id"), types.EQ, astql.P("productId"))).
			Returning(astql.F("id"), astql.F("price"))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("Expected RETURNING in SQL: %s", result.SQL)
		}
	})

	t.Run("DELETE RETURNING", func(t *testing.T) {
		query := Delete(astql.T("Product")).
			Where(astql.C(astql.F("stock"), types.EQ, astql.P("zero"))).
			Returning(astql.F("id"), astql.F("name"))

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("Expected RETURNING in SQL: %s", result.SQL)
		}
	})
}

func TestBuilderHelpers(t *testing.T) {
	setupAdvancedTest(t)

	t.Run("Builder helper functions", func(t *testing.T) {
		// Test the fluent interface with helper functions
		caseExpr := Case().
			When(astql.C(astql.F("price"), types.GT, astql.P("high")), astql.P("expensive")).
			When(astql.C(astql.F("price"), types.GT, astql.P("medium")), astql.P("moderate")).
			Else(astql.P("cheap")).
			End().As("price_category")

		coalesceExpr := Coalesce(astql.P("sale_price"), astql.P("regular_price")).As("final_price")
		nullIfExpr := NullIf(astql.P("quantity"), astql.P("zero")).As("non_zero_qty")

		query := Select(astql.T("Product")).
			FieldExpressions(
				FieldExpression{Case: &caseExpr, Alias: caseExpr.Alias},
				FieldExpression{Coalesce: &coalesceExpr, Alias: coalesceExpr.Alias},
				FieldExpression{NullIf: &nullIfExpr, Alias: nullIfExpr.Alias},
			)

		ast, err := query.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		provider := NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Render failed: %v", err)
		}

		// Check all constructs are present
		if !strings.Contains(result.SQL, "CASE WHEN") {
			t.Errorf("Expected CASE WHEN in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COALESCE(") {
			t.Errorf("Expected COALESCE in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "NULLIF(") {
			t.Errorf("Expected NULLIF in SQL: %s", result.SQL)
		}
		// Check aliases
		if !strings.Contains(result.SQL, `"price_category"`) {
			t.Errorf("Expected price_category alias in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"final_price"`) {
			t.Errorf("Expected final_price alias in SQL: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"non_zero_qty"`) {
			t.Errorf("Expected non_zero_qty alias in SQL: %s", result.SQL)
		}
	})
}
