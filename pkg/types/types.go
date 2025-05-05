package types

// DataType represents SQL data types
type DataType int

const (
	TypeNull DataType = iota
	TypeInt
	TypeFloat
	TypeString
	TypeBool
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	StmtCreate StatementType = iota
	StmtDrop
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtSelect
)

// ResultType represents the type of operation result
type ResultType int

const (
	ResultSuccess ResultType = iota
	ResultRows
	ResultError
)

// Constraint represents column constraints
type Constraint int

const (
	ConstraintNone Constraint = iota
	ConstraintNotNull
	ConstraintUnique
	ConstraintPrimaryKey
)
