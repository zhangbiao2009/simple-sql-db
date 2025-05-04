package parser

import (
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Parser is responsible for parsing SQL strings
type Parser interface {
	Parse(sql string) (Statement, error)
}

// Statement represents a parsed SQL statement
type Statement interface {
	Type() types.StatementType
}

// CreateTableStatement represents a CREATE TABLE statement
type CreateTableStatement interface {
	Statement
	TableName() string
	Columns() []ColumnDefinition
}

// DropTableStatement represents a DROP TABLE statement
type DropTableStatement interface {
	Statement
	TableName() string
}

// InsertStatement represents an INSERT statement
type InsertStatement interface {
	Statement
	TableName() string
	Columns() []string
	Values() [][]Value
}

// UpdateStatement represents an UPDATE statement
type UpdateStatement interface {
	Statement
	TableName() string
	SetClauses() map[string]Expression
	WhereClause() Expression
}

// DeleteStatement represents a DELETE statement
type DeleteStatement interface {
	Statement
	TableName() string
	WhereClause() Expression
}

// SelectStatement represents a SELECT statement
type SelectStatement interface {
	Statement
	TableName() string
	Columns() []string
	WhereClause() Expression
}

// ColumnDefinition represents a column definition in CREATE TABLE
type ColumnDefinition interface {
	Name() string
	Type() types.DataType
	Constraints() []types.Constraint
}

// Expression represents an expression in SQL statements
type Expression interface {
	Eval(row map[string]Value) (Value, error)
}

// Value represents a SQL value
type Value interface {
	Type() types.DataType
	AsInt() (int64, error)
	AsFloat() (float64, error)
	AsString() (string, error)
	AsBool() (bool, error)
	AsNull() (bool, error)
}
