package executor

import (
	"testing"

	"github.com/zhangbiao2009/simple-sql-db/catalog"
	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/storage"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Mock implementations for testing
type mockStatement struct {
	stmtType types.StatementType
}

func (s *mockStatement) Type() types.StatementType {
	return s.stmtType
}

type mockCreateTableStmt struct {
	mockStatement
	tableName string
	columns   []parser.ColumnDefinition
}

func (s *mockCreateTableStmt) TableName() string {
	return s.tableName
}

func (s *mockCreateTableStmt) Columns() []parser.ColumnDefinition {
	return s.columns
}

type mockDropTableStmt struct {
	mockStatement
	tableName string
}

func (s *mockDropTableStmt) TableName() string {
	return s.tableName
}

type mockInsertStmt struct {
	mockStatement
	tableName string
	columns   []string
	values    [][]parser.Value
}

func (s *mockInsertStmt) TableName() string {
	return s.tableName
}

func (s *mockInsertStmt) Columns() []string {
	return s.columns
}

func (s *mockInsertStmt) Values() [][]parser.Value {
	return s.values
}

type mockSelectStmt struct {
	mockStatement
	tableName string
	columns   []string
	whereExpr parser.Expression
}

func (s *mockSelectStmt) TableName() string {
	return s.tableName
}

func (s *mockSelectStmt) Columns() []string {
	return s.columns
}

func (s *mockSelectStmt) WhereClause() parser.Expression {
	return s.whereExpr
}

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

type mockExpression struct {
	result parser.Value
}

func (e *mockExpression) Eval(row map[string]parser.Value) (parser.Value, error) {
	return e.result, nil
}

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

func TestExecuteCreateTable(t *testing.T) {
	cat := catalog.NewCatalog()
	store := storage.NewMemoryStorage()
	exec := NewExecutor(cat, store)

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
	}

	// Create a mock CREATE TABLE statement
	stmt := &mockCreateTableStmt{
		mockStatement: mockStatement{stmtType: types.StmtCreate},
		tableName:     "users",
		columns:       cols,
	}

	// Execute the statement
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	if result.Type() != types.ResultSuccess {
		t.Errorf("Result type = %v, want %v", result.Type(), types.ResultSuccess)
	}

	// Verify the table was created
	_, found := cat.GetTable("users")
	if !found {
		t.Errorf("Table 'users' not found in catalog after creation")
	}
}

func TestExecuteInsertAndSelect(t *testing.T) {
	cat := catalog.NewCatalog()
	store := storage.NewMemoryStorage()
	exec := NewExecutor(cat, store)

	// First create a table
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
	}

	createStmt := &mockCreateTableStmt{
		mockStatement: mockStatement{stmtType: types.StmtCreate},
		tableName:     "users",
		columns:       cols,
	}

	exec.Execute(createStmt)

	// Create a mock INSERT statement
	values := []parser.Value{
		&mockValue{
			dataType: types.TypeInt,
			intVal:   1,
		},
		&mockValue{
			dataType:  types.TypeString,
			stringVal: "Alice",
		},
	}

	insertStmt := &mockInsertStmt{
		mockStatement: mockStatement{stmtType: types.StmtInsert},
		tableName:     "users",
		columns:       []string{"id", "name"},
		values:        [][]parser.Value{values},
	}

	// Execute the INSERT
	result, err := exec.Execute(insertStmt)
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	if result.Type() != types.ResultSuccess {
		t.Errorf("Result type = %v, want %v", result.Type(), types.ResultSuccess)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("RowsAffected() = %v, want 1", result.RowsAffected())
	}

	// Create a mock SELECT statement to verify the insert
	selectStmt := &mockSelectStmt{
		mockStatement: mockStatement{stmtType: types.StmtSelect},
		tableName:     "users",
		columns:       []string{"id", "name"},
		whereExpr:     &mockExpression{result: &mockValue{dataType: types.TypeBool, boolVal: true}},
	}

	// Execute the SELECT
	result, err = exec.Execute(selectStmt)
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	if result.Type() != types.ResultRows {
		t.Errorf("Result type = %v, want %v", result.Type(), types.ResultRows)
	}

	// Check the selected row
	rows := result.Rows()
	if !rows.Next() {
		t.Errorf("No rows returned from SELECT")
	}

	row := rows.Row()
	idVal := row["id"]
	id, _ := idVal.AsInt()
	if id != 1 {
		t.Errorf("Selected id = %v, want 1", id)
	}

	nameVal := row["name"]
	name, _ := nameVal.AsString()
	if name != "Alice" {
		t.Errorf("Selected name = %v, want Alice", name)
	}

	if rows.Next() {
		t.Errorf("More than one row returned from SELECT")
	}
}

func TestExecuteDropTable(t *testing.T) {
	cat := catalog.NewCatalog()
	store := storage.NewMemoryStorage()
	exec := NewExecutor(cat, store)

	// First create a table
	cols := []parser.ColumnDefinition{
		&mockColumnDefinition{
			name:        "id",
			dataType:    types.TypeInt,
			constraints: []types.Constraint{types.ConstraintPrimaryKey},
		},
	}

	createStmt := &mockCreateTableStmt{
		mockStatement: mockStatement{stmtType: types.StmtCreate},
		tableName:     "users",
		columns:       cols,
	}

	exec.Execute(createStmt)

	// Create a mock DROP TABLE statement
	dropStmt := &mockDropTableStmt{
		mockStatement: mockStatement{stmtType: types.StmtDrop},
		tableName:     "users",
	}

	// Execute the drop
	result, err := exec.Execute(dropStmt)
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	if result.Type() != types.ResultSuccess {
		t.Errorf("Result type = %v, want %v", result.Type(), types.ResultSuccess)
	}

	// Verify the table was dropped
	_, found := cat.GetTable("users")
	if found {
		t.Errorf("Table 'users' still found in catalog after drop")
	}
}
