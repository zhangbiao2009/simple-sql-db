package storage

import (
	"testing"

	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

// Mock implementation of parser.Value for testing
type mockValue struct {
	dataType  types.DataType
	intVal    int64
	floatVal  float64
	stringVal string
	boolVal   bool
}

func (v *mockValue) Type() types.DataType {
	return v.dataType
}

func (v *mockValue) AsInt() (int64, error) {
	return v.intVal, nil
}

func (v *mockValue) AsFloat() (float64, error) {
	return v.floatVal, nil
}

func (v *mockValue) AsString() (string, error) {
	return v.stringVal, nil
}

func (v *mockValue) AsBool() (bool, error) {
	return v.boolVal, nil
}

func (v *mockValue) AsNull() (bool, error) {
	return v.dataType == types.TypeNull, nil
}

// Mock implementation of catalog.TableSchema for testing
type mockTableSchema struct {
	name    string
	columns []parser.ColumnDefinition
}

func (s *mockTableSchema) Name() string {
	return s.name
}

func (s *mockTableSchema) Columns() []parser.ColumnDefinition {
	return s.columns
}

func (s *mockTableSchema) GetColumn(name string) (parser.ColumnDefinition, bool) {
	for _, col := range s.columns {
		if col.Name() == name {
			return col, true
		}
	}
	return nil, false
}

func (s *mockTableSchema) HasColumn(name string) bool {
	_, found := s.GetColumn(name)
	return found
}

func (s *mockTableSchema) GetColumnType(name string) types.DataType {
	col, found := s.GetColumn(name)
	if !found {
		return types.TypeNull
	}
	return col.Type()
}

// Mock implementation of parser.ColumnDefinition for testing
type mockColumnDefinition struct {
	name        string
	dataType    types.DataType
	constraints []types.Constraint
}

func (c *mockColumnDefinition) Name() string {
	return c.name
}

func (c *mockColumnDefinition) Type() types.DataType {
	return c.dataType
}

func (c *mockColumnDefinition) Constraints() []types.Constraint {
	return c.constraints
}

func TestMemoryStorage(t *testing.T) {
	// Create a new storage instance
	storage := NewMemoryStorage()

	// Create mock columns for testing
	columns := []parser.ColumnDefinition{
		&mockColumnDefinition{
			name:        "id",
			dataType:    types.TypeInt,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
		&mockColumnDefinition{
			name:        "name",
			dataType:    types.TypeString,
			constraints: []types.Constraint{types.ConstraintNotNull},
		},
		&mockColumnDefinition{
			name:     "active",
			dataType: types.TypeBool,
		},
	}

	// Create mock schema
	schema := &mockTableSchema{
		name:    "users",
		columns: columns,
	}

	// Test CreateTable
	err := storage.CreateTable("users", schema)
	if err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// Test creating a duplicate table (should fail)
	err = storage.CreateTable("users", schema)
	if err == nil {
		t.Errorf("CreateTable() with duplicate name should error")
	}

	// Test Insert
	row := map[string]parser.Value{
		"id": &mockValue{
			dataType: types.TypeInt,
			intVal:   1,
		},
		"name": &mockValue{
			dataType:  types.TypeString,
			stringVal: "Alice",
		},
		"active": &mockValue{
			dataType: types.TypeBool,
			boolVal:  true,
		},
	}

	err = storage.Insert("users", row)
	if err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// Insert a second row for testing
	row2 := map[string]parser.Value{
		"id": &mockValue{
			dataType: types.TypeInt,
			intVal:   2,
		},
		"name": &mockValue{
			dataType:  types.TypeString,
			stringVal: "Bob",
		},
		"active": &mockValue{
			dataType: types.TypeBool,
			boolVal:  false,
		},
	}

	err = storage.Insert("users", row2)
	if err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// Test Select all rows
	rows, err := storage.Select("users", []string{"id", "name", "active"}, func(row Row) (bool, error) {
		return true, nil // Match all rows
	})
	if err != nil {
		t.Errorf("Select() error = %v", err)
	}

	// Count rows
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()

	if rowCount != 2 {
		t.Errorf("Select() returned %v rows, want 2", rowCount)
	}

	// Test Select with filter
	rows, err = storage.Select("users", []string{"name"}, func(row Row) (bool, error) {
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		return name == "Alice", nil
	})
	if err != nil {
		t.Errorf("Select() with filter error = %v", err)
	}

	// Verify the filtered row
	if !rows.Next() {
		t.Errorf("Expected one row in filtered Select()")
	}
	row = rows.Row()
	nameVal := row["name"]
	name, _ := nameVal.AsString()
	if name != "Alice" {
		t.Errorf("Filtered Select() returned name = %v, want Alice", name)
	}
	rows.Close()

	// Test Update
	updateVals := map[string]parser.Value{
		"active": &mockValue{
			dataType: types.TypeBool,
			boolVal:  false,
		},
	}

	count, err := storage.Update("users", updateVals, func(row Row) (bool, error) {
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		return name == "Alice", nil
	})
	if err != nil {
		t.Errorf("Update() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Update() affected %v rows, want 1", count)
	}

	// Verify the update
	rows, _ = storage.Select("users", []string{"name", "active"}, func(row Row) (bool, error) {
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		return name == "Alice", nil
	})
	rows.Next()
	row = rows.Row()
	activeVal := row["active"]
	active, _ := activeVal.AsBool()
	if active != false {
		t.Errorf("After Update(), active = %v, want false", active)
	}
	rows.Close()

	// Test Delete
	count, err = storage.Delete("users", func(row Row) (bool, error) {
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		return name == "Bob", nil
	})
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Delete() affected %v rows, want 1", count)
	}

	// Verify the delete
	rows, _ = storage.Select("users", []string{"name"}, func(row Row) (bool, error) {
		return true, nil
	})
	rowCount = 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if rowCount != 1 {
		t.Errorf("After Delete(), row count = %v, want 1", rowCount)
	}

	// Test DropTable
	err = storage.DropTable("users")
	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// Test dropping a non-existent table
	err = storage.DropTable("nonexistent")
	if err == nil {
		t.Errorf("DropTable() with non-existent table should error")
	}
}
