package storage

import (
	"github.com/zhangbiao2009/simple-sql-db/pkg/catalog"
	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
)

// Storage is the interface for the storage engine
type Storage interface {
	// CreateTable creates a new table in storage
	CreateTable(tableName string, schema catalog.TableSchema) error

	// DropTable removes a table from storage
	DropTable(tableName string) error

	// Insert inserts a new row into a table
	Insert(tableName string, values map[string]parser.Value) error

	// Update updates rows in a table that match a condition
	Update(tableName string, values map[string]parser.Value, condition FilterFunc) (int, error)

	// Delete deletes rows from a table that match a condition
	Delete(tableName string, condition FilterFunc) (int, error)

	// Select selects rows from a table that match a condition
	Select(tableName string, columns []string, condition FilterFunc) (RowIterator, error)
}

// Row represents a row in a table
type Row map[string]parser.Value

// FilterFunc is a function that filters rows
type FilterFunc func(row Row) (bool, error)

// RowIterator iterates over rows returned by a query
type RowIterator interface {
	// Next advances to the next row
	Next() bool

	// Row returns the current row
	Row() Row

	// Err returns any error that occurred during iteration
	Err() error

	// Close closes the iterator
	Close()
}
