package render

// RowLockingLevel indicates the level of row-level locking support.
type RowLockingLevel int

const (
	RowLockingNone  RowLockingLevel = iota // No row locking
	RowLockingBasic                        // FOR UPDATE, FOR SHARE
	RowLockingFull                         // + FOR NO KEY UPDATE, FOR KEY SHARE
)

// Capabilities describes the SQL features supported by a dialect.
type Capabilities struct {
	DistinctOn          bool            // DISTINCT ON (field, ...)
	Upsert              bool            // ON CONFLICT / ON DUPLICATE KEY
	Returning           bool            // RETURNING clause
	CaseInsensitiveLike bool            // ILIKE operator
	RegexOperators      bool            // ~, ~*, !~, !~*
	ArrayOperators      bool            // @>, <@, &&
	InArray             bool            // IN (:array_param)
	RowLocking          RowLockingLevel // FOR UPDATE/SHARE support
}
