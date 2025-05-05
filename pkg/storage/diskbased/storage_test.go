package diskbased

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
	"github.com/zhangbiao2009/simple-sql-db/pkg/storage"
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

func setupTestDB(t *testing.T) (string, *DiskStorage) {
	// Create a temporary directory for the test database
	tempDir, err := os.MkdirTemp("", "diskbasedtest-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a new disk storage instance
	storage, err := NewDiskStorage(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create disk storage: %v", err)
	}

	return tempDir, storage
}

func cleanupTestDB(tempDir string) {
	os.RemoveAll(tempDir)
}

func TestDiskStorage_BasicOperations(t *testing.T) {
	tempDir, diskStorage := setupTestDB(t)
	defer cleanupTestDB(tempDir)

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
	err := diskStorage.CreateTable("users", schema)
	if err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// Verify table file was created
	if _, err := os.Stat(filepath.Join(tempDir, "users.db")); os.IsNotExist(err) {
		t.Errorf("Table file was not created")
	}

	// Test creating a duplicate table (should fail)
	err = diskStorage.CreateTable("users", schema)
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

	err = diskStorage.Insert("users", row)
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

	err = diskStorage.Insert("users", row2)
	if err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// Note: Primary key constraints would normally be tested here
	// The B+ tree doesn't enforce primary key constraints at the storage level
	// This would need to be handled at a higher level

	// Test Select all rows
	rows, err := diskStorage.Select("users", []string{"id", "name", "active"}, nil)
	if err != nil {
		t.Errorf("Select() error = %v", err)
	}

	// Count rows
	rowCount := 0
	for rows.Next() {
		rowCount++
		row := rows.Row()

		// Verify row data is correct
		if row["id"] == nil || row["name"] == nil || row["active"] == nil {
			t.Errorf("Select() returned incomplete row")
		}
	}
	if err := rows.Err(); err != nil {
		t.Errorf("Error iterating rows: %v", err)
	}
	rows.Close()

	if rowCount != 2 {
		t.Errorf("Select() returned %v rows, want 2", rowCount)
	}

	// Test Select with filter
	rows, err = diskStorage.Select("users", []string{"name"}, func(row storage.Row) (bool, error) {
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
	} else {
		row = rows.Row()
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		if name != "Alice" {
			t.Errorf("Filtered Select() returned name = %v, want Alice", name)
		}
	}
	rows.Close()

	// Test Update
	updateVals := map[string]parser.Value{
		"active": &mockValue{
			dataType: types.TypeBool,
			boolVal:  false,
		},
	}

	count, err := diskStorage.Update("users", updateVals, func(row storage.Row) (bool, error) {
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
	rows, _ = diskStorage.Select("users", []string{"name", "active"}, func(row storage.Row) (bool, error) {
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		return name == "Alice", nil
	})
	if rows.Next() {
		row = rows.Row()
		activeVal := row["active"]
		active, _ := activeVal.AsBool()
		if active != false {
			t.Errorf("After Update(), active = %v, want false", active)
		}
	} else {
		t.Errorf("Failed to find updated row")
	}
	rows.Close()

	// Test Delete
	count, err = diskStorage.Delete("users", func(row storage.Row) (bool, error) {
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
	rows, _ = diskStorage.Select("users", []string{"name"}, nil)
	rowCount = 0
	for rows.Next() {
		rowCount++
		row := rows.Row()
		nameVal := row["name"]
		name, _ := nameVal.AsString()
		if name == "Bob" {
			t.Errorf("Found deleted row")
		}
	}
	rows.Close()
	if rowCount != 1 {
		t.Errorf("After Delete(), row count = %v, want 1", rowCount)
	}

	// Test DropTable
	err = diskStorage.DropTable("users")
	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// Verify table file was deleted
	if _, err := os.Stat(filepath.Join(tempDir, "users.db")); !os.IsNotExist(err) {
		t.Errorf("Table file was not deleted")
	}

	// Test dropping a non-existent table
	err = diskStorage.DropTable("nonexistent")
	if err == nil {
		t.Errorf("DropTable() with non-existent table should error")
	}
}

func TestDiskStorage_Persistence(t *testing.T) {
	tempDir, diskStorage := setupTestDB(t)
	defer cleanupTestDB(tempDir)

	// Create schema
	columns := []parser.ColumnDefinition{
		&mockColumnDefinition{
			name:        "id",
			dataType:    types.TypeInt,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
		&mockColumnDefinition{
			name:     "data",
			dataType: types.TypeString,
		},
	}
	schema := &mockTableSchema{name: "test_persist", columns: columns}

	// Create table and insert data
	err := diskStorage.CreateTable("test_persist", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some rows
	for i := int64(1); i <= 5; i++ {
		err = diskStorage.Insert("test_persist", map[string]parser.Value{
			"id":   &mockValue{dataType: types.TypeInt, intVal: i},
			"data": &mockValue{dataType: types.TypeString, stringVal: "test_data"},
		})
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Close the storage
	err = diskStorage.Close()
	if err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Reopen the storage
	reopenedStorage, err := NewDiskStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer reopenedStorage.Close()

	// Verify data persisted
	rows, err := reopenedStorage.Select("test_persist", []string{"id", "data"}, nil)
	if err != nil {
		t.Errorf("Select() after reopening error = %v", err)
	}

	rowCount := 0
	for rows.Next() {
		rowCount++
		row := rows.Row()

		idVal, err := row["id"].AsInt()
		if err != nil {
			t.Errorf("Failed to get id as int: %v", err)
		}

		if idVal < 1 || idVal > 5 {
			t.Errorf("Got unexpected ID value: %d", idVal)
		}

		dataVal, err := row["data"].AsString()
		if err != nil {
			t.Errorf("Failed to get data as string: %v", err)
		}

		if dataVal != "test_data" {
			t.Errorf("Got data = %s, want test_data", dataVal)
		}
	}
	rows.Close()

	if rowCount != 5 {
		t.Errorf("After reopening, row count = %v, want 5", rowCount)
	}
}

func TestDiskStorage_CompositeKeys(t *testing.T) {
	tempDir, diskStorage := setupTestDB(t)
	defer cleanupTestDB(tempDir)

	// Create schema with composite primary key
	columns := []parser.ColumnDefinition{
		&mockColumnDefinition{
			name:        "first_name",
			dataType:    types.TypeString,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
		&mockColumnDefinition{
			name:        "last_name",
			dataType:    types.TypeString,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
		&mockColumnDefinition{
			name:     "age",
			dataType: types.TypeInt,
		},
	}
	schema := &mockTableSchema{name: "composite_test", columns: columns}

	// Create table
	err := diskStorage.CreateTable("composite_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows with different combinations of the composite key
	testCases := []struct {
		firstName string
		lastName  string
		age       int64
	}{
		{"John", "Doe", 30},
		{"John", "Smith", 25},
		{"Jane", "Doe", 28},
	}

	for _, tc := range testCases {
		err = diskStorage.Insert("composite_test", map[string]parser.Value{
			"first_name": &mockValue{dataType: types.TypeString, stringVal: tc.firstName},
			"last_name":  &mockValue{dataType: types.TypeString, stringVal: tc.lastName},
			"age":        &mockValue{dataType: types.TypeInt, intVal: tc.age},
		})
		if err != nil {
			t.Errorf("Failed to insert (%s, %s): %v", tc.firstName, tc.lastName, err)
		}
	}

	// Test selecting with a condition on the composite key
	rows, err := diskStorage.Select("composite_test", []string{"first_name", "last_name", "age"}, func(row storage.Row) (bool, error) {
		firstName, _ := row["first_name"].AsString()
		lastName, _ := row["last_name"].AsString()
		return firstName == "John" && lastName == "Doe", nil
	})
	if err != nil {
		t.Errorf("Select() error = %v", err)
	}

	// Should find exactly one row
	count := 0
	for rows.Next() {
		count++
		row := rows.Row()
		firstName, _ := row["first_name"].AsString()
		lastName, _ := row["last_name"].AsString()
		age, _ := row["age"].AsInt()

		if firstName != "John" || lastName != "Doe" || age != 30 {
			t.Errorf("Got row (%s, %s, %d), want (John, Doe, 30)", firstName, lastName, age)
		}
	}
	rows.Close()

	if count != 1 {
		t.Errorf("Got %d rows, want 1", count)
	}
}

func TestDiskStorage_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	tempDir, diskStorage := setupTestDB(t)
	defer cleanupTestDB(tempDir)

	// Create schema
	columns := []parser.ColumnDefinition{
		&mockColumnDefinition{
			name:        "id",
			dataType:    types.TypeInt,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
		&mockColumnDefinition{
			name:     "data",
			dataType: types.TypeString,
		},
	}
	schema := &mockTableSchema{name: "large_test", columns: columns}

	// Create table
	err := diskStorage.CreateTable("large_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert many rows to test B+ tree splits and balancing
	rowCount := 1000
	for i := 1; i <= rowCount; i++ {
		data := make([]byte, 100) // 100 byte payload per row
		for j := range data {
			data[j] = byte(i % 256)
		}

		err = diskStorage.Insert("large_test", map[string]parser.Value{
			"id":   &mockValue{dataType: types.TypeInt, intVal: int64(i)},
			"data": &mockValue{dataType: types.TypeString, stringVal: string(data)},
		})
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Test retrieving all rows
	rows, err := diskStorage.Select("large_test", []string{"id"}, nil)
	if err != nil {
		t.Errorf("Select() error = %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != rowCount {
		t.Errorf("Got %d rows, want %d", count, rowCount)
	}

	// Test random access by primary key
	for _, id := range []int{1, 500, rowCount} {
		rows, err := diskStorage.Select("large_test", []string{"id", "data"}, func(row storage.Row) (bool, error) {
			idVal, _ := row["id"].AsInt()
			return idVal == int64(id), nil
		})
		if err != nil {
			t.Errorf("Select() for id=%d error = %v", id, err)
			continue
		}

		found := false
		for rows.Next() {
			found = true
			row := rows.Row()
			idVal, _ := row["id"].AsInt()
			if idVal != int64(id) {
				t.Errorf("Got id=%d, want %d", idVal, id)
			}
		}
		rows.Close()

		if !found {
			t.Errorf("Couldn't find row with id=%d", id)
		}
	}
}
