package types

// Operator represents query comparison operators.
type Operator string

const (
	// Basic comparison operators.
	EQ Operator = "="
	NE Operator = "!="
	GT Operator = ">"
	GE Operator = ">="
	LT Operator = "<"
	LE Operator = "<="

	// Extended operators.
	IN        Operator = "IN"
	NotIn     Operator = "NOT IN"
	LIKE      Operator = "LIKE"
	NotLike   Operator = "NOT LIKE"
	IsNull    Operator = "IS NULL"
	IsNotNull Operator = "IS NOT NULL"
	EXISTS    Operator = "EXISTS"
	NotExists Operator = "NOT EXISTS"
)
