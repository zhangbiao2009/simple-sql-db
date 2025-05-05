// Package diskbased implements a disk-based storage engine using B+ trees.
package diskbased

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/zhangbiao2009/simple-sql-db/pkg/catalog"
	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
	"github.com/zhangbiao2009/simple-sql-db/pkg/storage"
	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

const (
	// DatabaseDir is the directory where database files are stored
	DatabaseDir = "./data"

	// CatalogFileName is the name of the catalog file
	CatalogFileName = "catalog.json"

	// MaxTableNameLength is the maximum length of a table name
	MaxTableNameLength = 64
)

// DiskStorage implements the storage.Storage interface using a disk-based B+ tree
type DiskStorage struct {
	dbDir       string
	pageManager *PageManager
	tables      map[string]*TableInfo
	mu          sync.RWMutex
}

// TableInfo stores information about a table
type TableInfo struct {
	Schema     catalog.TableSchema
	IndexTree  *BPlusTree
	RootPageID PageID
}

// NewDiskStorage creates a new disk-based storage engine
func NewDiskStorage(dbDir string) (*DiskStorage, error) {
	// Create database directory if it doesn't exist
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		err := os.MkdirAll(dbDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create database directory: %w", err)
		}
	}

	// Create page manager for catalog
	catalogPath := filepath.Join(dbDir, CatalogFileName)
	pageManager, err := NewPageManager(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}

	storage := &DiskStorage{
		dbDir:       dbDir,
		pageManager: pageManager,
		tables:      make(map[string]*TableInfo),
	}

	// Load existing tables from catalog
	err = storage.loadCatalog()
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	return storage, nil
}

// loadCatalog loads the catalog from disk
func (ds *DiskStorage) loadCatalog() error {
	// Check if catalog exists and has at least one page
	if ds.pageManager.numPages <= 1 {
		// No catalog yet, initialize a new one
		return nil
	}

	// Get catalog root page
	page, err := ds.pageManager.GetPage(1)
	if err != nil {
		return err
	}

	// Read catalog data
	data := page.Data()
	numTables := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	for i := uint32(0); i < numTables; i++ {
		// Read table name
		tableNameLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		tableName := string(data[offset : offset+int(tableNameLen)])
		offset += int(tableNameLen)

		// Read root page ID
		rootPageID := PageID(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		// Read schema length
		schemaLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Read schema JSON
		schemaJSON := data[offset : offset+int(schemaLen)]
		offset += int(schemaLen)

		// Unmarshal schema
		var schema catalog.TableSchema
		err = json.Unmarshal(schemaJSON, &schema)
		if err != nil {
			return err
		}

		// Open table file
		tableFile := filepath.Join(ds.dbDir, tableName+".db")
		tablePageManager, err := NewPageManager(tableFile)
		if err != nil {
			return err
		}

		// Create B+ tree for the table
		tree, err := NewBPlusTree(tablePageManager, rootPageID)
		if err != nil {
			return err
		}

		// Add table to tables map
		ds.tables[tableName] = &TableInfo{
			Schema:     schema,
			IndexTree:  tree,
			RootPageID: rootPageID,
		}
	}

	return nil
}

// saveCatalog saves the catalog to disk
func (ds *DiskStorage) saveCatalog() error {
	// Get catalog page
	page, err := ds.pageManager.GetPage(1)
	if err != nil {
		// Create page if it doesn't exist
		page, err = ds.pageManager.AllocatePage()
		if err != nil {
			return err
		}
	}

	// Write catalog data
	data := page.Data()
	numTables := uint32(len(ds.tables))
	binary.LittleEndian.PutUint32(data[0:4], numTables)
	offset := 4

	for tableName, table := range ds.tables {
		// Write table name
		tableNameLen := uint32(len(tableName))
		binary.LittleEndian.PutUint32(data[offset:offset+4], tableNameLen)
		offset += 4
		copy(data[offset:offset+int(tableNameLen)], tableName)
		offset += int(tableNameLen)

		// Write root page ID
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(table.RootPageID))
		offset += 4

		// Marshal schema
		schemaJSON, err := json.Marshal(table.Schema)
		if err != nil {
			return err
		}

		// Write schema length
		schemaLen := uint32(len(schemaJSON))
		binary.LittleEndian.PutUint32(data[offset:offset+4], schemaLen)
		offset += 4

		// Write schema JSON
		copy(data[offset:offset+int(schemaLen)], schemaJSON)
		offset += int(schemaLen)
	}

	// Mark page as dirty
	page.MarkDirty()

	// Flush all dirty pages
	return ds.pageManager.FlushAllPages()
}

// serializeRow serializes a row into a byte slice
func serializeRow(values map[string]parser.Value, schema catalog.TableSchema) ([]byte, error) {
	// Serialize as JSON for simplicity
	return json.Marshal(values)
}

// Close closes the storage and releases resources
func (ds *DiskStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Save catalog before closing
	err := ds.saveCatalog()
	if err != nil {
		return err
	}

	// Close page manager
	if ds.pageManager != nil {
		err := ds.pageManager.Close()
		if err != nil {
			return fmt.Errorf("failed to close catalog page manager: %w", err)
		}
	}

	// Close all table page managers
	for tableName, table := range ds.tables {
		if table.IndexTree != nil && table.IndexTree.pageManager != nil {
			err := table.IndexTree.pageManager.Close()
			if err != nil {
				return fmt.Errorf("failed to close table %s page manager: %w", tableName, err)
			}
		}
	}

	return nil
}

// deserializeRow deserializes a byte slice into a row
func deserializeRow(data []byte, schema catalog.TableSchema) (map[string]parser.Value, error) {
	var values map[string]parser.Value
	err := json.Unmarshal(data, &values)
	return values, err
}

// createRowID creates a row ID from values
func createRowID(values map[string]parser.Value, schema catalog.TableSchema) ([]byte, error) {
	// Find primary key columns
	primaryKeyColumns := getPrimaryKeyColumns(schema)

	// Use primary key if available
	if len(primaryKeyColumns) > 0 {
		return serializeCompositePrimaryKey(values, primaryKeyColumns)
	}

	// Otherwise, use all values
	return serializeRow(values, schema)
}

// Helper function to get primary key columns from schema
func getPrimaryKeyColumns(schema catalog.TableSchema) []string {
	var primaryKeyColumns []string

	// Go through all columns and check for primary key constraint
	for _, col := range schema.Columns() {
		for _, constraint := range col.Constraints() {
			if constraint == types.ConstraintPrimaryKey {
				primaryKeyColumns = append(primaryKeyColumns, col.Name())
				break
			}
		}
	}

	return primaryKeyColumns
}

// Helper function to serialize primary key values
func serializeCompositePrimaryKey(values map[string]parser.Value, primaryKey []string) ([]byte, error) {
	pkValues := make([]parser.Value, 0, len(primaryKey))
	pkTypes := make([]types.DataType, 0, len(primaryKey))

	for _, colName := range primaryKey {
		val, ok := values[colName]
		if !ok {
			return nil, fmt.Errorf("missing primary key column: %s", colName)
		}

		pkValues = append(pkValues, val)
		pkTypes = append(pkTypes, val.Type())
	}

	// Create composite row ID with values and types
	key := NewCompositeRowID(pkValues, pkTypes)
	return key.Bytes(), nil
}

// CreateTable creates a new table in storage
func (ds *DiskStorage) CreateTable(tableName string, schema catalog.TableSchema) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check if table already exists
	if _, exists := ds.tables[tableName]; exists {
		return fmt.Errorf("table %s already exists", tableName)
	}

	// Create table file
	tableFile := filepath.Join(ds.dbDir, tableName+".db")
	tablePageManager, err := NewPageManager(tableFile)
	if err != nil {
		return err
	}

	// Create B+ tree for the table
	tree, err := CreateNewTree(tablePageManager)
	if err != nil {
		return err
	}

	// Add table to tables map
	ds.tables[tableName] = &TableInfo{
		Schema:     schema,
		IndexTree:  tree,
		RootPageID: tree.rootPageID,
	}

	// Save catalog
	return ds.saveCatalog()
}

// DropTable removes a table from storage
func (ds *DiskStorage) DropTable(tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check if table exists
	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Close B+ tree
	if tableInfo.IndexTree != nil && tableInfo.IndexTree.pageManager != nil {
		tableInfo.IndexTree.pageManager.Close()
	}

	// Remove table file
	tableFile := filepath.Join(ds.dbDir, tableName+".db")
	err := os.Remove(tableFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove table from tables map
	delete(ds.tables, tableName)

	// Save catalog
	return ds.saveCatalog()
}

// Insert inserts a new row into a table
func (ds *DiskStorage) Insert(tableName string, values map[string]parser.Value) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check if table exists
	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Create row ID
	rowID, err := createRowID(values, tableInfo.Schema)
	if err != nil {
		return err
	}

	// Serialize row
	rowData, err := serializeRow(values, tableInfo.Schema)
	if err != nil {
		return err
	}

	// Insert into B+ tree
	return tableInfo.IndexTree.Insert(rowID, rowData)
}

// Update updates rows in a table that match a condition
func (ds *DiskStorage) Update(tableName string, values map[string]parser.Value, condition storage.FilterFunc) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check if table exists
	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get all rows from the table
	iter, err := ds.Select(tableName, nil, nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	// Update matching rows
	count := 0
	for iter.Next() {
		row := iter.Row()

		// Apply condition if provided
		if condition != nil {
			match, err := condition(row)
			if err != nil {
				return count, err
			}
			if !match {
				continue
			}
		}

		// Update values
		for key, value := range values {
			row[key] = value
		}

		// Delete old row
		oldRowID, err := createRowID(iter.(*DiskRowIterator).originalRow, tableInfo.Schema)
		if err != nil {
			return count, err
		}
		err = tableInfo.IndexTree.Delete(oldRowID)
		if err != nil {
			return count, err
		}

		// Insert updated row
		newRowID, err := createRowID(row, tableInfo.Schema)
		if err != nil {
			return count, err
		}
		rowData, err := serializeRow(row, tableInfo.Schema)
		if err != nil {
			return count, err
		}
		err = tableInfo.IndexTree.Insert(newRowID, rowData)
		if err != nil {
			return count, err
		}

		count++
	}

	if iter.Err() != nil {
		return count, iter.Err()
	}

	return count, nil
}

// Delete deletes rows from a table that match a condition
func (ds *DiskStorage) Delete(tableName string, condition storage.FilterFunc) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check if table exists
	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get all rows from the table
	iter, err := ds.Select(tableName, nil, nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	// Delete matching rows
	count := 0
	for iter.Next() {
		row := iter.Row()

		// Apply condition if provided
		if condition != nil {
			match, err := condition(row)
			if err != nil {
				return count, err
			}
			if !match {
				continue
			}
		}

		// Delete row
		rowID, err := createRowID(row, tableInfo.Schema)
		if err != nil {
			return count, err
		}
		err = tableInfo.IndexTree.Delete(rowID)
		if err != nil {
			return count, err
		}

		count++
	}

	if iter.Err() != nil {
		return count, iter.Err()
	}

	return count, nil
}

// Select selects rows from a table that match a condition
func (ds *DiskStorage) Select(tableName string, columns []string, condition storage.FilterFunc) (storage.RowIterator, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	// Check if table exists
	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Create iterator
	iter := &DiskRowIterator{
		tableInfo:    tableInfo,
		columns:      columns,
		condition:    condition,
		rows:         make([]storage.Row, 0),
		originalRows: make([]storage.Row, 0),
		currentIdx:   -1,
	}

	// Load all rows that match the condition
	// This is inefficient, but simplifies the implementation
	// In a real database, we would use the B+ tree to efficiently filter rows
	err := iter.loadRows()
	if err != nil {
		return nil, err
	}

	return iter, nil
}

// DiskRowIterator implements the storage.RowIterator interface for disk-based storage
type DiskRowIterator struct {
	tableInfo    *TableInfo
	columns      []string
	condition    storage.FilterFunc
	rows         []storage.Row
	originalRows []storage.Row
	originalRow  storage.Row
	currentIdx   int
	err          error
}

// loadRows loads all rows that match the condition
func (iter *DiskRowIterator) loadRows() error {
	// Find the leaf node containing the smallest key
	leafNodeID, err := iter.tableInfo.IndexTree.findLeafNode(iter.tableInfo.IndexTree.rootPageID, []byte{0})
	if err != nil {
		return err
	}

	// Iterate through all leaf nodes
	for leafNodeID != 0 {
		// Get the leaf node
		node, err := iter.tableInfo.IndexTree.pageManager.GetPage(leafNodeID)
		if err != nil {
			return err
		}

		// Read leaf node header
		data := node.Data()
		nodeType := data[0] // Node type is the first byte
		if nodeType != NodeTypeLeaf {
			return fmt.Errorf("expected leaf node, got %d", nodeType)
		}

		numKeys := binary.LittleEndian.Uint32(data[1:5])
		nextLeafID := PageID(binary.LittleEndian.Uint32(data[5:9]))

		// Read all keys and values
		keyValueOffset := int(NodeHeaderSize)
		for i := uint32(0); i < numKeys; i++ {
			// Read key length
			keyLen := int(binary.LittleEndian.Uint32(data[keyValueOffset : keyValueOffset+4]))
			keyValueOffset += 4

			// Read key data (keeping but not using the key)
			_ = data[keyValueOffset : keyValueOffset+keyLen]
			keyValueOffset += keyLen

			// Read value length
			valueLen := int(binary.LittleEndian.Uint32(data[keyValueOffset : keyValueOffset+4]))
			keyValueOffset += 4

			// Read value data
			value := data[keyValueOffset : keyValueOffset+valueLen]
			keyValueOffset += valueLen

			// Deserialize row
			row, err := deserializeRow(value, iter.tableInfo.Schema)
			if err != nil {
				return err
			}

			// Apply condition if provided
			if iter.condition != nil {
				match, err := iter.condition(row)
				if err != nil {
					return err
				}
				if !match {
					continue
				}
			}

			// Project columns if specified
			if iter.columns != nil && len(iter.columns) > 0 {
				projectedRow := make(storage.Row)
				for _, col := range iter.columns {
					if val, ok := row[col]; ok {
						projectedRow[col] = val
					}
				}
				iter.rows = append(iter.rows, projectedRow)
			} else {
				iter.rows = append(iter.rows, row)
			}

			// Keep original row for updates/deletes
			iter.originalRows = append(iter.originalRows, row)
		}

		// Move to next leaf node
		leafNodeID = nextLeafID
	}

	return nil
}

// Next advances to the next row
func (iter *DiskRowIterator) Next() bool {
	iter.currentIdx++
	return iter.currentIdx < len(iter.rows)
}

// Row returns the current row
func (iter *DiskRowIterator) Row() storage.Row {
	if iter.currentIdx < 0 || iter.currentIdx >= len(iter.rows) {
		return nil
	}
	row := iter.rows[iter.currentIdx]
	iter.originalRow = iter.originalRows[iter.currentIdx]
	return row
}

// Err returns any error that occurred during iteration
func (iter *DiskRowIterator) Err() error {
	return iter.err
}

// Close closes the iterator
func (iter *DiskRowIterator) Close() {
	// Nothing to do for now
}
