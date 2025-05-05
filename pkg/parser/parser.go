package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

// SimpleParser implements the Parser interface
type SimpleParser struct{}

// NewParser creates a new SimpleParser
func NewParser() Parser {
	return &SimpleParser{}
}

// Parse parses a SQL statement and returns a Statement interface
func (p *SimpleParser) Parse(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")

	if strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE") {
		return p.parseCreateTable(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "DROP TABLE") {
		return p.parseDropTable(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "INSERT INTO") {
		return p.parseInsert(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "UPDATE") {
		return p.parseUpdate(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "DELETE FROM") {
		return p.parseDelete(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return p.parseSelect(sql)
	}

	return nil, errors.New("unsupported SQL statement")
}

// parseCreateTable parses a CREATE TABLE statement
func (p *SimpleParser) parseCreateTable(sql string) (CreateTableStatement, error) {
	// Basic regex for CREATE TABLE name (col1 type, col2 type, ...)
	r := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)\s*\((.*)\)`)
	matches := r.FindStringSubmatch(sql)

	if len(matches) != 3 {
		return nil, errors.New("invalid CREATE TABLE syntax")
	}

	tableName := matches[1]
	columnDefs := matches[2]

	// Split column definitions
	colParts := splitIgnoringParentheses(columnDefs, ',')
	columns := make([]ColumnDefinition, 0, len(colParts))

	for _, colStr := range colParts {
		colStr = strings.TrimSpace(colStr)
		parts := strings.Fields(colStr)

		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %s", colStr)
		}

		name := parts[0]
		dataType := parseDataType(parts[1])
		constraints := parseConstraints(parts[2:])

		columns = append(columns, &columnDefinition{
			name:        name,
			dataType:    dataType,
			constraints: constraints,
		})
	}

	return &createTableStatement{
		tableName: tableName,
		columns:   columns,
	}, nil
}

// parseDropTable parses a DROP TABLE statement
func (p *SimpleParser) parseDropTable(sql string) (DropTableStatement, error) {
	r := regexp.MustCompile(`(?i)DROP\s+TABLE\s+(\w+)`)
	matches := r.FindStringSubmatch(sql)

	if len(matches) != 2 {
		return nil, errors.New("invalid DROP TABLE syntax")
	}

	return &dropTableStatement{
		tableName: matches[1],
	}, nil
}

// parseInsert parses an INSERT statement
func (p *SimpleParser) parseInsert(sql string) (InsertStatement, error) {
	// First try the format with explicit columns: INSERT INTO table (col1, col2) VALUES (val1, val2)
	r1 := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)`)
	matches := r1.FindStringSubmatch(sql)

	if len(matches) == 4 {
		tableName := matches[1]

		// Parse column names
		colStr := matches[2]
		columns := splitAndTrim(colStr, ',')

		// Parse values
		valuesStr := matches[3]
		valueGroups := parseValueLists(valuesStr)

		return &insertStatement{
			tableName: tableName,
			columns:   columns,
			values:    valueGroups,
		}, nil
	}

	// Try the format without explicit columns: INSERT INTO table VALUES (val1, val2)
	r2 := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)\s+VALUES\s+(.+)`)
	matches = r2.FindStringSubmatch(sql)

	if len(matches) == 3 {
		tableName := matches[1]

		// Parse values
		valuesStr := matches[2]
		valueGroups := parseValueLists(valuesStr)

		return &insertStatement{
			tableName: tableName,
			columns:   []string{}, // Empty columns means "use all columns in order"
			values:    valueGroups,
		}, nil
	}

	return nil, errors.New("invalid INSERT syntax")
}

// parseUpdate parses an UPDATE statement
func (p *SimpleParser) parseUpdate(sql string) (UpdateStatement, error) {
	// Basic implementation for UPDATE table SET col=val WHERE condition
	r := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$`)
	matches := r.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, errors.New("invalid UPDATE syntax")
	}

	tableName := matches[1]
	setClauses := make(map[string]Expression)

	// Parse SET clauses
	setStr := matches[2]
	setParts := splitIgnoringParentheses(setStr, ',')

	for _, part := range setParts {
		part = strings.TrimSpace(part)
		eqParts := strings.SplitN(part, "=", 2)
		if len(eqParts) != 2 {
			return nil, fmt.Errorf("invalid SET clause: %s", part)
		}

		colName := strings.TrimSpace(eqParts[0])
		valExpr, err := parseExpression(strings.TrimSpace(eqParts[1]))
		if err != nil {
			return nil, err
		}

		setClauses[colName] = valExpr
	}

	var whereExpr Expression
	// Parse WHERE clause if present
	if len(matches) > 3 && len(matches[3]) > 0 {
		var err error
		whereExpr, err = parseExpression(matches[3])
		if err != nil {
			return nil, err
		}
	} else {
		// No WHERE clause, means all rows
		whereExpr = &literalExpression{val: &literalValue{dataType: types.TypeBool, boolVal: true}}
	}

	return &updateStatement{
		tableName:  tableName,
		setClauses: setClauses,
		whereExpr:  whereExpr,
	}, nil
}

// parseDelete parses a DELETE statement
func (p *SimpleParser) parseDelete(sql string) (DeleteStatement, error) {
	// DELETE FROM table WHERE condition
	r := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)
	matches := r.FindStringSubmatch(sql)

	if len(matches) < 2 {
		return nil, errors.New("invalid DELETE syntax")
	}

	tableName := matches[1]

	var whereExpr Expression
	// Parse WHERE clause if present
	if len(matches) > 2 && len(matches[2]) > 0 {
		var err error
		whereExpr, err = parseExpression(matches[2])
		if err != nil {
			return nil, err
		}
	} else {
		// No WHERE clause, means all rows
		whereExpr = &literalExpression{val: &literalValue{dataType: types.TypeBool, boolVal: true}}
	}

	return &deleteStatement{
		tableName: tableName,
		whereExpr: whereExpr,
	}, nil
}

// parseSelect parses a SELECT statement
func (p *SimpleParser) parseSelect(sql string) (SelectStatement, error) {
	// SELECT col1, col2 FROM table WHERE condition
	r := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)
	matches := r.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, errors.New("invalid SELECT syntax")
	}

	colsStr := matches[1]
	tableName := matches[2]

	columns := splitAndTrim(colsStr, ',')

	var whereExpr Expression
	// Parse WHERE clause if present
	if len(matches) > 3 && len(matches[3]) > 0 {
		var err error
		whereExpr, err = parseExpression(matches[3])
		if err != nil {
			return nil, err
		}
	} else {
		// No WHERE clause, means all rows
		whereExpr = &literalExpression{val: &literalValue{dataType: types.TypeBool, boolVal: true}}
	}

	return &selectStatement{
		tableName: tableName,
		columns:   columns,
		whereExpr: whereExpr,
	}, nil
}

// Helper functions for parsing

// parseDataType converts string type to DataType
func parseDataType(typeStr string) types.DataType {
	switch strings.ToUpper(typeStr) {
	case "INT", "INTEGER":
		return types.TypeInt
	case "FLOAT", "REAL", "DOUBLE":
		return types.TypeFloat
	case "TEXT", "VARCHAR", "CHAR", "STRING":
		return types.TypeString
	case "BOOL", "BOOLEAN":
		return types.TypeBool
	default:
		return types.TypeString // default to string
	}
}

// parseConstraints extracts constraints from string tokens
func parseConstraints(constraintStrs []string) []types.Constraint {
	result := []types.Constraint{}

	for _, str := range constraintStrs {
		str = strings.ToUpper(strings.TrimSpace(str))
		switch str {
		case "NOT NULL":
			result = append(result, types.ConstraintNotNull)
		case "UNIQUE":
			result = append(result, types.ConstraintUnique)
		case "PRIMARY KEY":
			result = append(result, types.ConstraintPrimaryKey)
		}
	}

	return result
}

// splitAndTrim splits a string by a separator and trims whitespace
func splitAndTrim(s string, sep rune) []string {
	parts := splitIgnoringParentheses(s, sep)
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}

// splitIgnoringParentheses splits a string by a separator, ignoring separators in parentheses
func splitIgnoringParentheses(s string, sep rune) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, r := range s {
		if r == '(' {
			depth++
		} else if r == ')' {
			depth--
		}

		if r == sep && depth == 0 {
			result = append(result, current.String())
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// parseValueLists parses the VALUES part of an INSERT
func parseValueLists(valuesStr string) [][]Value {
	valueGroups := [][]Value{}

	// Extract each value group (val1, val2), (val3, val4)
	groupRegex := regexp.MustCompile(`\(([^)]+)\)`)
	matches := groupRegex.FindAllStringSubmatch(valuesStr, -1)

	for _, match := range matches {
		valueList := splitIgnoringParentheses(match[1], ',')
		values := make([]Value, 0, len(valueList))

		for _, val := range valueList {
			val = strings.TrimSpace(val)
			// Parse each value
			expr, err := parseExpression(val)
			if err != nil {
				// Just create a NULL value if there's an error
				values = append(values, &literalValue{dataType: types.TypeNull})
				continue
			}

			if litExpr, ok := expr.(*literalExpression); ok {
				values = append(values, litExpr.val)
			} else {
				// For simplicity, only support literals in INSERT
				values = append(values, &literalValue{dataType: types.TypeNull})
			}
		}

		valueGroups = append(valueGroups, values)
	}

	return valueGroups
}

// parseExpression parses an expression string
func parseExpression(expr string) (Expression, error) {
	expr = strings.TrimSpace(expr)

	// Handle basic operators
	if strings.Contains(expr, "=") {
		parts := strings.SplitN(expr, "=", 2)
		if len(parts) == 2 {
			left, err := parseExpression(parts[0])
			if err != nil {
				return nil, err
			}
			right, err := parseExpression(parts[1])
			if err != nil {
				return nil, err
			}
			return &binaryExpression{
				left:     left,
				right:    right,
				operator: "=",
			}, nil
		}
	}

	// Handle literals
	if isStringLiteral(expr) {
		// Remove quotes from string literals
		return &literalExpression{
			val: &literalValue{
				dataType:  types.TypeString,
				stringVal: expr[1 : len(expr)-1],
			},
		}, nil
	} else if isNumericLiteral(expr) {
		// Check if it's a float or int
		if strings.Contains(expr, ".") {
			return &literalExpression{
				val: &literalValue{
					dataType: types.TypeFloat,
					floatVal: parseFloat(expr),
				},
			}, nil
		} else {
			return &literalExpression{
				val: &literalValue{
					dataType: types.TypeInt,
					intVal:   parseInt(expr),
				},
			}, nil
		}
	} else if strings.EqualFold(expr, "NULL") {
		return &literalExpression{
			val: &literalValue{
				dataType: types.TypeNull,
			},
		}, nil
	} else if strings.EqualFold(expr, "TRUE") {
		return &literalExpression{
			val: &literalValue{
				dataType: types.TypeBool,
				boolVal:  true,
			},
		}, nil
	} else if strings.EqualFold(expr, "FALSE") {
		return &literalExpression{
			val: &literalValue{
				dataType: types.TypeBool,
				boolVal:  false,
			},
		}, nil
	}

	// If it's not a literal, assume it's a column reference
	return &columnExpression{
		columnName: expr,
	}, nil
}

// isStringLiteral checks if expr is a string literal
func isStringLiteral(expr string) bool {
	return (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) ||
		(strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\""))
}

// isNumericLiteral checks if expr is a numeric literal
func isNumericLiteral(expr string) bool {
	_, err := fmt.Sscanf(expr, "%f", new(float64))
	return err == nil
}

// parseInt converts string to int64
func parseInt(s string) int64 {
	var result int64
	fmt.Sscanf(s, "%d", &result)
	return result
}

// parseFloat converts string to float64
func parseFloat(s string) float64 {
	var result float64
	fmt.Sscanf(s, "%f", &result)
	return result
}
