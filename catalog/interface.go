package catalog

import (
	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Catalog manages table schemas
type Catalog interface {
	// CreateTable creates a new table schema
	CreateTable(name string, columns []parser.ColumnDefinition) error

	// DropTable removes a table schema
	DropTable(name string) error

	// GetTable retrieves a table schema
	GetTable(name string) (TableSchema, bool)

	// ListTables lists all available tables
	ListTables() []string
}

// TableSchema represents a table's schema
type TableSchema interface {
	// Name returns the table name
	Name() string

	// Columns returns all column definitions
	Columns() []parser.ColumnDefinition

	// GetColumn retrieves a column by name
	GetColumn(name string) (parser.ColumnDefinition, bool)

	// HasColumn checks if a column exists
	HasColumn(name string) bool

	// GetColumnType gets the data type of a column
	GetColumnType(name string) types.DataType
}
