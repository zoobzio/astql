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

	// Vector operators (pgvector).
	VectorL2Distance     Operator = "<->" // L2/Euclidean distance
	VectorInnerProduct   Operator = "<#>" // Negative inner product
	VectorCosineDistance Operator = "<=>" // Cosine distance
	VectorL1Distance     Operator = "<+>" // L1/Manhattan distance
)
