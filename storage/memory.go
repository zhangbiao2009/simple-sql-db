package storage

import (
	"fmt"
	"sync"

	"github.com/zhangbiao2009/simple-sql-db/catalog"
	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// MemoryStorage is an in-memory implementation of Storage
type MemoryStorage struct {
	// map[tableName][]Row
	tables map[string][]Row
	mu     sync.RWMutex
	// Keep track of schemas for validation
	schemas map[string]catalog.TableSchema
}

// NewMemoryStorage creates a new memory storage
func NewMemoryStorage() Storage {
	return &MemoryStorage{
		tables:  make(map[string][]Row),
		schemas: make(map[string]catalog.TableSchema),
	}
}

// CreateTable creates a new table in storage
func (s *MemoryStorage) CreateTable(tableName string, schema catalog.TableSchema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableName]; exists {
		return fmt.Errorf("table '%s' already exists in storage", tableName)
	}

	s.tables[tableName] = []Row{}
	s.schemas[tableName] = schema

	return nil
}

// DropTable removes a table from storage
func (s *MemoryStorage) DropTable(tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableName]; !exists {
		return fmt.Errorf("table '%s' does not exist in storage", tableName)
	}

	delete(s.tables, tableName)
	delete(s.schemas, tableName)

	return nil
}

// Insert inserts a new row into a table
func (s *MemoryStorage) Insert(tableName string, values map[string]parser.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, exists := s.tables[tableName]
	if !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	schema, exists := s.schemas[tableName]
	if !exists {
		return fmt.Errorf("schema for table '%s' not found", tableName)
	}

	// Validate values against schema
	for _, col := range schema.Columns() {
		colName := col.Name()
		val, exists := values[colName]

		// Check for NOT NULL constraint
		for _, constraint := range col.Constraints() {
			if constraint == types.ConstraintNotNull {
				if !exists || val == nil {
					return fmt.Errorf("column '%s' cannot be NULL", colName)
				}

				isNull, _ := val.AsNull()
				if isNull {
					return fmt.Errorf("column '%s' cannot be NULL", colName)
				}
			}
		}

		// Check data type if value exists
		if exists && val != nil {
			isNull, _ := val.AsNull()
			if !isNull && val.Type() != col.Type() {
				return fmt.Errorf("type mismatch for column '%s'", colName)
			}
		}
	}

	// Add the row
	s.tables[tableName] = append(rows, values)
	return nil
}

// Update updates rows in a table that match a condition
func (s *MemoryStorage) Update(tableName string, values map[string]parser.Value, condition FilterFunc) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, exists := s.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	schema, exists := s.schemas[tableName]
	if !exists {
		return 0, fmt.Errorf("schema for table '%s' not found", tableName)
	}

	// Validate values against schema
	for colName, val := range values {
		col, exists := schema.GetColumn(colName)
		if !exists {
			return 0, fmt.Errorf("column '%s' does not exist", colName)
		}

		isNull, _ := val.AsNull()
		if !isNull && val.Type() != col.Type() {
			return 0, fmt.Errorf("type mismatch for column '%s'", colName)
		}

		// Check NOT NULL constraint
		for _, constraint := range col.Constraints() {
			if constraint == types.ConstraintNotNull && isNull {
				return 0, fmt.Errorf("column '%s' cannot be NULL", colName)
			}
		}
	}

	count := 0
	for i := range rows {
		match, err := condition(rows[i])
		if err != nil {
			return count, err
		}

		if match {
			// Update values
			for colName, val := range values {
				rows[i][colName] = val
			}
			count++
		}
	}

	return count, nil
}

// Delete deletes rows from a table that match a condition
func (s *MemoryStorage) Delete(tableName string, condition FilterFunc) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, exists := s.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	newRows := make([]Row, 0, len(rows))
	deleted := 0

	for _, row := range rows {
		match, err := condition(row)
		if err != nil {
			return deleted, err
		}

		if match {
			deleted++
		} else {
			newRows = append(newRows, row)
		}
	}

	s.tables[tableName] = newRows
	return deleted, nil
}

// Select selects rows from a table that match a condition
func (s *MemoryStorage) Select(tableName string, columns []string, condition FilterFunc) (RowIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, exists := s.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	schema, exists := s.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("schema for table '%s' not found", tableName)
	}

	// Validate columns
	if len(columns) > 0 && columns[0] != "*" {
		for _, colName := range columns {
			if !schema.HasColumn(colName) {
				return nil, fmt.Errorf("column '%s' does not exist in table '%s'", colName, tableName)
			}
		}
	} else if len(columns) == 1 && columns[0] == "*" {
		// Select all columns
		allCols := schema.Columns()
		columns = make([]string, len(allCols))
		for i, col := range allCols {
			columns[i] = col.Name()
		}
	}

	// Filter rows
	filteredRows := make([]Row, 0, len(rows))
	for _, row := range rows {
		match, err := condition(row)
		if err != nil {
			return nil, err
		}

		if match {
			// Only include selected columns
			if len(columns) > 0 {
				selectRow := make(Row)
				for _, colName := range columns {
					selectRow[colName] = row[colName]
				}
				filteredRows = append(filteredRows, selectRow)
			} else {
				filteredRows = append(filteredRows, row)
			}
		}
	}

	return &memoryRowIterator{
		rows: filteredRows,
	}, nil
}

// memoryRowIterator is an implementation of RowIterator
type memoryRowIterator struct {
	rows  []Row
	index int
	err   error
}

// Next advances to the next row
func (i *memoryRowIterator) Next() bool {
	if i.index >= len(i.rows) {
		return false
	}
	i.index++
	return i.index <= len(i.rows)
}

// Row returns the current row
func (i *memoryRowIterator) Row() Row {
	if i.index <= 0 || i.index > len(i.rows) {
		return nil
	}
	return i.rows[i.index-1]
}

// Err returns any error that occurred during iteration
func (i *memoryRowIterator) Err() error {
	return i.err
}

// Close closes the iterator
func (i *memoryRowIterator) Close() {
	// Nothing to close for in-memory iterator
}
