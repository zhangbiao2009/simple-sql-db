package catalog

import (
	"errors"
	"sync"

	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// MemoryCatalog is an in-memory implementation of the Catalog interface
type MemoryCatalog struct {
	tables map[string]TableSchema
	mu     sync.RWMutex
}

// NewCatalog creates a new memory catalog
func NewCatalog() Catalog {
	return &MemoryCatalog{
		tables: make(map[string]TableSchema),
	}
}

// CreateTable creates a new table schema
func (c *MemoryCatalog) CreateTable(name string, columns []parser.ColumnDefinition) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return errors.New("table already exists")
	}

	c.tables[name] = &memoryTableSchema{
		name:    name,
		columns: columns,
	}

	return nil
}

// DropTable removes a table schema
func (c *MemoryCatalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return errors.New("table does not exist")
	}

	delete(c.tables, name)
	return nil
}

// GetTable retrieves a table schema
func (c *MemoryCatalog) GetTable(name string) (TableSchema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.tables[name]
	return schema, ok
}

// ListTables lists all available tables
func (c *MemoryCatalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tableNames := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tableNames = append(tableNames, name)
	}

	return tableNames
}

// memoryTableSchema is an in-memory implementation of the TableSchema interface
type memoryTableSchema struct {
	name    string
	columns []parser.ColumnDefinition
}

// Name returns the table name
func (s *memoryTableSchema) Name() string {
	return s.name
}

// Columns returns all column definitions
func (s *memoryTableSchema) Columns() []parser.ColumnDefinition {
	return s.columns
}

// GetColumn retrieves a column by name
func (s *memoryTableSchema) GetColumn(name string) (parser.ColumnDefinition, bool) {
	for _, col := range s.columns {
		if col.Name() == name {
			return col, true
		}
	}
	return nil, false
}

// HasColumn checks if a column exists
func (s *memoryTableSchema) HasColumn(name string) bool {
	_, found := s.GetColumn(name)
	return found
}

// GetColumnType gets the data type of a column
func (s *memoryTableSchema) GetColumnType(name string) types.DataType {
	col, found := s.GetColumn(name)
	if !found {
		return types.TypeNull
	}
	return col.Type()
}
