package db

import (
	"fmt"
	"strings"

	"github.com/zhangbiao2009/simple-sql-db/pkg/catalog"
	"github.com/zhangbiao2009/simple-sql-db/pkg/executor"
	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
	"github.com/zhangbiao2009/simple-sql-db/pkg/storage"
	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

// DB represents an in-memory database instance
type DB struct {
	parser   parser.Parser
	catalog  catalog.Catalog
	storage  storage.Storage
	executor *executor.Executor
}

// Result represents a database query result
type Result struct {
	// For all queries
	Success bool
	Error   error

	// For DML (rows affected)
	RowsAffected int

	// For queries returning rows
	Columns []string
	Rows    []map[string]string
}

// New creates a new in-memory database
func New() *DB {
	cat := catalog.NewCatalog()
	store := storage.NewMemoryStorage()
	return &DB{
		parser:   parser.NewParser(),
		catalog:  cat,
		storage:  store,
		executor: executor.NewExecutor(cat, store),
	}
}

// Execute executes a SQL statement and returns the result
func (db *DB) Execute(sql string) Result {
	stmt, err := db.parser.Parse(sql)
	if err != nil {
		return Result{
			Success: false,
			Error:   err,
		}
	}

	execResult, err := db.executor.Execute(stmt)
	if err != nil {
		return Result{
			Success: false,
			Error:   err,
		}
	}

	if execResult.Error() != nil {
		return Result{
			Success: false,
			Error:   execResult.Error(),
		}
	}

	result := Result{
		Success: true,
	}

	switch execResult.Type() {
	case types.ResultSuccess:
		result.RowsAffected = execResult.RowsAffected()
	case types.ResultRows:
		rows := execResult.Rows()
		if rows == nil {
			result.Success = false
			result.Error = fmt.Errorf("query returned nil rows")
			return result
		}

		result.Rows = []map[string]string{}
		columnSet := make(map[string]bool)

		// Extract rows and columns
		for rows.Next() {
			row := rows.Row()
			resultRow := make(map[string]string)

			for colName, val := range row {
				columnSet[colName] = true

				// Convert value to string for display
				var valStr string
				switch val.Type() {
				case types.TypeInt:
					intVal, _ := val.AsInt()
					valStr = fmt.Sprintf("%d", intVal)
				case types.TypeFloat:
					floatVal, _ := val.AsFloat()
					valStr = fmt.Sprintf("%g", floatVal)
				case types.TypeString:
					valStr, _ = val.AsString()
				case types.TypeBool:
					boolVal, _ := val.AsBool()
					valStr = fmt.Sprintf("%t", boolVal)
				case types.TypeNull:
					valStr = "NULL"
				}
				resultRow[colName] = valStr
			}

			result.Rows = append(result.Rows, resultRow)
		}

		// Extract column names in a deterministic order
		for col := range columnSet {
			result.Columns = append(result.Columns, col)
		}

		// Close the row iterator
		rows.Close()
	}

	return result
}

// FormatResult formats a database result as a string
func FormatResult(result Result) string {
	var sb strings.Builder

	if !result.Success {
		return fmt.Sprintf("Error: %v", result.Error)
	}

	if result.Rows != nil && len(result.Rows) > 0 {
		// Format columns
		columnWidths := make(map[string]int)
		for _, col := range result.Columns {
			columnWidths[col] = len(col)
		}

		// Determine the width needed for each column
		for _, row := range result.Rows {
			for _, col := range result.Columns {
				valLen := len(row[col])
				if valLen > columnWidths[col] {
					columnWidths[col] = valLen
				}
			}
		}

		// Print header
		for i, col := range result.Columns {
			if i > 0 {
				sb.WriteString(" | ")
			}
			format := fmt.Sprintf("%%-%ds", columnWidths[col])
			sb.WriteString(fmt.Sprintf(format, col))
		}
		sb.WriteString("\n")

		// Print separator
		for i, col := range result.Columns {
			if i > 0 {
				sb.WriteString("-+-")
			}
			sb.WriteString(strings.Repeat("-", columnWidths[col]))
		}
		sb.WriteString("\n")

		// Print rows
		for _, row := range result.Rows {
			for i, col := range result.Columns {
				if i > 0 {
					sb.WriteString(" | ")
				}
				format := fmt.Sprintf("%%-%ds", columnWidths[col])
				sb.WriteString(fmt.Sprintf(format, row[col]))
			}
			sb.WriteString("\n")
		}

		sb.WriteString(fmt.Sprintf("\n%d rows returned\n", len(result.Rows)))
	} else if result.RowsAffected >= 0 {
		sb.WriteString(fmt.Sprintf("%d rows affected\n", result.RowsAffected))
	} else {
		sb.WriteString("Success\n")
	}

	return sb.String()
}
