package catalog

import (
	"testing"

	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Mock implementation of parser.ColumnDefinition for testing
type mockColumnDefinition struct {
	name        string
	dataType    types.DataType
	constraints []types.Constraint
}

func (m *mockColumnDefinition) Name() string {
	return m.name
}

func (m *mockColumnDefinition) Type() types.DataType {
	return m.dataType
}

func (m *mockColumnDefinition) Constraints() []types.Constraint {
	return m.constraints
}

func TestMemoryCatalog(t *testing.T) {
	cat := NewCatalog()

	// Create mock columns for testing
	cols := []parser.ColumnDefinition{
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
			name:     "age",
			dataType: types.TypeInt,
		},
	}

	// Test creating a table
	err := cat.CreateTable("users", cols)
	if err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// Test listing tables
	tables := cat.ListTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("ListTables() = %v, want [users]", tables)
	}

	// Test getting a table
	schema, found := cat.GetTable("users")
	if !found {
		t.Errorf("GetTable() found = false, want true")
	}
	if schema.Name() != "users" {
		t.Errorf("schema.Name() = %v, want users", schema.Name())
	}

	// Test getting columns
	schemaCols := schema.Columns()
	if len(schemaCols) != 3 {
		t.Errorf("len(schema.Columns()) = %v, want 3", len(schemaCols))
	}

	// Test getting a specific column
	col, found := schema.GetColumn("name")
	if !found {
		t.Errorf("GetColumn() found = false, want true")
	}
	if col.Name() != "name" {
		t.Errorf("col.Name() = %v, want name", col.Name())
	}
	if col.Type() != types.TypeString {
		t.Errorf("col.Type() = %v, want %v", col.Type(), types.TypeString)
	}

	// Test HasColumn
	if !schema.HasColumn("id") {
		t.Errorf("HasColumn() for 'id' = false, want true")
	}
	if schema.HasColumn("nonexistent") {
		t.Errorf("HasColumn() for 'nonexistent' = true, want false")
	}

	// Test GetColumnType
	if schema.GetColumnType("age") != types.TypeInt {
		t.Errorf("GetColumnType() for 'age' = %v, want %v", schema.GetColumnType("age"), types.TypeInt)
	}
	if schema.GetColumnType("nonexistent") != types.TypeNull {
		t.Errorf("GetColumnType() for 'nonexistent' = %v, want %v", schema.GetColumnType("nonexistent"), types.TypeNull)
	}

	// Test creating a duplicate table (should fail)
	err = cat.CreateTable("users", cols)
	if err == nil {
		t.Errorf("CreateTable() with duplicate name should error")
	}

	// Test dropping a table
	err = cat.DropTable("users")
	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// Verify table was dropped
	tables = cat.ListTables()
	if len(tables) != 0 {
		t.Errorf("ListTables() after drop = %v, want []", tables)
	}

	// Test dropping a non-existent table
	err = cat.DropTable("nonexistent")
	if err == nil {
		t.Errorf("DropTable() with non-existent table should error")
	}
}
