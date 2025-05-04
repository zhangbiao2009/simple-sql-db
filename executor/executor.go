package executor

import (
	"fmt"

	"github.com/zhangbiao2009/simple-sql-db/catalog"
	"github.com/zhangbiao2009/simple-sql-db/parser"
	"github.com/zhangbiao2009/simple-sql-db/storage"
	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Result represents the result of executing a statement
type Result interface {
	// Type returns the type of the result
	Type() types.ResultType

	// RowsAffected returns the number of rows affected by the statement
	RowsAffected() int

	// Rows returns the rows returned by the statement
	Rows() storage.RowIterator

	// Error returns any error that occurred during execution
	Error() error
}

// Executor executes SQL statements
type Executor struct {
	catalog catalog.Catalog
	storage storage.Storage
}

// NewExecutor creates a new executor with the given catalog and storage
func NewExecutor(catalog catalog.Catalog, storage storage.Storage) *Executor {
	return &Executor{
		catalog: catalog,
		storage: storage,
	}
}

// Execute executes a SQL statement
func (e *Executor) Execute(stmt parser.Statement) (Result, error) {
	switch stmt.Type() {
	case types.StmtCreate:
		return e.executeCreateTable(stmt.(parser.CreateTableStatement))
	case types.StmtDrop:
		return e.executeDropTable(stmt.(parser.DropTableStatement))
	case types.StmtInsert:
		return e.executeInsert(stmt.(parser.InsertStatement))
	case types.StmtUpdate:
		return e.executeUpdate(stmt.(parser.UpdateStatement))
	case types.StmtDelete:
		return e.executeDelete(stmt.(parser.DeleteStatement))
	case types.StmtSelect:
		return e.executeSelect(stmt.(parser.SelectStatement))
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", stmt.Type())
	}
}

// executeCreateTable executes a CREATE TABLE statement
func (e *Executor) executeCreateTable(stmt parser.CreateTableStatement) (Result, error) {
	// Create the table schema in the catalog
	err := e.catalog.CreateTable(stmt.TableName(), stmt.Columns())
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	// Get the table schema that was just created
	schema, found := e.catalog.GetTable(stmt.TableName())
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found after creation", stmt.TableName()),
		}, nil
	}

	// Create the table in storage
	err = e.storage.CreateTable(stmt.TableName(), schema)
	if err != nil {
		// Roll back the catalog change
		e.catalog.DropTable(stmt.TableName())
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	return &executionResult{
		resultType:   types.ResultSuccess,
		rowsAffected: 0,
	}, nil
}

// executeDropTable executes a DROP TABLE statement
func (e *Executor) executeDropTable(stmt parser.DropTableStatement) (Result, error) {
	// Check if the table exists
	_, found := e.catalog.GetTable(stmt.TableName())
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found", stmt.TableName()),
		}, nil
	}

	// Drop the table from storage
	err := e.storage.DropTable(stmt.TableName())
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	// Drop the table from catalog
	err = e.catalog.DropTable(stmt.TableName())
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	return &executionResult{
		resultType:   types.ResultSuccess,
		rowsAffected: 0,
	}, nil
}

// executeInsert executes an INSERT statement
func (e *Executor) executeInsert(stmt parser.InsertStatement) (Result, error) {
	tableName := stmt.TableName()

	// Check if the table exists
	schema, found := e.catalog.GetTable(tableName)
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found", tableName),
		}, nil
	}

	// Validate column names
	columns := stmt.Columns()
	for _, colName := range columns {
		if !schema.HasColumn(colName) {
			return &executionResult{
				resultType: types.ResultError,
				err:        fmt.Errorf("column '%s' not found in table '%s'", colName, tableName),
			}, nil
		}
	}

	rowsAffected := 0
	// Process each row of values
	for _, values := range stmt.Values() {
		if len(values) != len(columns) {
			return &executionResult{
				resultType: types.ResultError,
				err:        fmt.Errorf("column count doesn't match value count"),
			}, nil
		}

		// Prepare the row
		row := make(map[string]parser.Value)
		for i, colName := range columns {
			row[colName] = values[i]
		}

		// Insert the row
		err := e.storage.Insert(tableName, row)
		if err != nil {
			return &executionResult{
				resultType:   types.ResultError,
				err:          err,
				rowsAffected: rowsAffected,
			}, nil
		}

		rowsAffected++
	}

	return &executionResult{
		resultType:   types.ResultSuccess,
		rowsAffected: rowsAffected,
	}, nil
}

// executeUpdate executes an UPDATE statement
func (e *Executor) executeUpdate(stmt parser.UpdateStatement) (Result, error) {
	tableName := stmt.TableName()

	// Check if the table exists
	_, found := e.catalog.GetTable(tableName)
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found", tableName),
		}, nil
	}

	// Prepare update values
	updateValues := make(map[string]parser.Value)
	for colName, expr := range stmt.SetClauses() {
		// Evaluate the expression
		val, err := evaluateExpression(expr, nil) // No row context for simple expressions
		if err != nil {
			return &executionResult{
				resultType: types.ResultError,
				err:        fmt.Errorf("failed to evaluate expression for column '%s': %v", colName, err),
			}, nil
		}
		updateValues[colName] = val
	}

	// Create filter function from WHERE clause
	filter := createFilterFunc(stmt.WhereClause())

	// Execute the update
	rowsAffected, err := e.storage.Update(tableName, updateValues, filter)
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	return &executionResult{
		resultType:   types.ResultSuccess,
		rowsAffected: rowsAffected,
	}, nil
}

// executeDelete executes a DELETE statement
func (e *Executor) executeDelete(stmt parser.DeleteStatement) (Result, error) {
	tableName := stmt.TableName()

	// Check if the table exists
	_, found := e.catalog.GetTable(tableName)
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found", tableName),
		}, nil
	}

	// Create filter function from WHERE clause
	filter := createFilterFunc(stmt.WhereClause())

	// Execute the delete
	rowsAffected, err := e.storage.Delete(tableName, filter)
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	return &executionResult{
		resultType:   types.ResultSuccess,
		rowsAffected: rowsAffected,
	}, nil
}

// executeSelect executes a SELECT statement
func (e *Executor) executeSelect(stmt parser.SelectStatement) (Result, error) {
	tableName := stmt.TableName()

	// Check if the table exists
	_, found := e.catalog.GetTable(tableName)
	if !found {
		return &executionResult{
			resultType: types.ResultError,
			err:        fmt.Errorf("table '%s' not found", tableName),
		}, nil
	}

	// Create filter function from WHERE clause
	filter := createFilterFunc(stmt.WhereClause())

	// Execute the select
	rows, err := e.storage.Select(tableName, stmt.Columns(), filter)
	if err != nil {
		return &executionResult{
			resultType: types.ResultError,
			err:        err,
		}, nil
	}

	return &executionResult{
		resultType: types.ResultRows,
		rows:       rows,
	}, nil
}

// Helper functions

// createFilterFunc creates a filter function from an expression
func createFilterFunc(expr parser.Expression) storage.FilterFunc {
	return func(row storage.Row) (bool, error) {
		// Evaluate the expression with the current row
		result, err := expr.Eval(row)
		if err != nil {
			return false, err
		}

		// Check if the result is a boolean true
		boolVal, err := result.AsBool()
		if err != nil {
			return false, err
		}

		return boolVal, nil
	}
}

// evaluateExpression evaluates an expression using the given row context
// For literals, the row context is not needed
func evaluateExpression(expr parser.Expression, row storage.Row) (parser.Value, error) {
	return expr.Eval(row)
}

// executionResult is the result of executing a statement
type executionResult struct {
	resultType   types.ResultType
	rowsAffected int
	rows         storage.RowIterator
	err          error
}

func (r *executionResult) Type() types.ResultType {
	return r.resultType
}

func (r *executionResult) RowsAffected() int {
	return r.rowsAffected
}

func (r *executionResult) Rows() storage.RowIterator {
	return r.rows
}

func (r *executionResult) Error() error {
	return r.err
}
