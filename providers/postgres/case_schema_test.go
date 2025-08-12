package postgres_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/astql/providers/postgres"

	"github.com/zoobzio/astql"

	"gopkg.in/yaml.v3"
)

func TestCaseSchema(t *testing.T) {
	// Setup test models
	astql.SetupTestModels()

	t.Run("CASE in JSON schema", func(t *testing.T) {
		jsonQuery := `{
			"operation": "SELECT",
			"table": "test_users",
			"fields": ["name"],
			"cases": [
				{
					"case": {
						"when": [
							{
								"condition": {
									"field": "age",
									"operator": "<",
									"param": "minorAge"
								},
								"result": "minorLabel"
							},
							{
								"condition": {
									"field": "age",
									"operator": "<",
									"param": "seniorAge"
								},
								"result": "adultLabel"
							}
						],
						"else": "seniorLabel"
					},
					"alias": "age_group"
				}
			]
		}`

		var schema postgres.QuerySchema
		if err := json.Unmarshal([]byte(jsonQuery), &schema); err != nil {
			t.Fatal(err)
		}

		// Note: BuildFromSchema returns QueryAST, but CASE requires PostgresAST
		// So we'll need to enhance the schema builder to support PostgreSQL features
		t.Skip("CASE schema support requires PostgreSQL-specific schema builder")
	})

	t.Run("CASE in YAML schema", func(t *testing.T) {
		yamlQuery := `
operation: SELECT
table: test_users
fields: [id, name]
cases:
  - case:
      when:
        - condition:
            field: active
            operator: "="
            param: trueValue
          result: activeLabel
        - condition:
            field: active
            operator: "="
            param: falseValue
          result: inactiveLabel
      else: unknownLabel
    alias: status
`

		var schema postgres.QuerySchema
		if err := yaml.Unmarshal([]byte(yamlQuery), &schema); err != nil {
			t.Fatal(err)
		}

		// Note: BuildFromSchema returns QueryAST, but CASE requires PostgresAST
		t.Skip("CASE schema support requires PostgreSQL-specific schema builder")
	})
}
