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
	ILIKE     Operator = "ILIKE"
	NotILike  Operator = "NOT ILIKE"
	IsNull    Operator = "IS NULL"
	IsNotNull Operator = "IS NOT NULL"
	EXISTS    Operator = "EXISTS"
	NotExists Operator = "NOT EXISTS"

	// Regex operators (PostgreSQL).
	RegexMatch     Operator = "~"
	RegexIMatch    Operator = "~*"
	NotRegexMatch  Operator = "!~"
	NotRegexIMatch Operator = "!~*"

	// Array operators (PostgreSQL).
	ArrayContains    Operator = "@>"
	ArrayContainedBy Operator = "<@"
	ArrayOverlap     Operator = "&&"

	// Vector operators (pgvector).
	VectorL2Distance     Operator = "<->" // L2/Euclidean distance
	VectorInnerProduct   Operator = "<#>" // Negative inner product
	VectorCosineDistance Operator = "<=>" // Cosine distance
	VectorL1Distance     Operator = "<+>" // L1/Manhattan distance
)
