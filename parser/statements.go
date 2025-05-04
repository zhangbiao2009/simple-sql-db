package parser

import (
	"fmt"
	"strings"

	"github.com/zhangbiao2009/simple-sql-db/types"
)

// Statement implementations

// createTableStatement implements CreateTableStatement
type createTableStatement struct {
	tableName string
	columns   []ColumnDefinition
}

func (s *createTableStatement) Type() types.StatementType {
	return types.StmtCreate
}

func (s *createTableStatement) TableName() string {
	return s.tableName
}

func (s *createTableStatement) Columns() []ColumnDefinition {
	return s.columns
}

// dropTableStatement implements DropTableStatement
type dropTableStatement struct {
	tableName string
}

func (s *dropTableStatement) Type() types.StatementType {
	return types.StmtDrop
}

func (s *dropTableStatement) TableName() string {
	return s.tableName
}

// insertStatement implements InsertStatement
type insertStatement struct {
	tableName string
	columns   []string
	values    [][]Value
}

func (s *insertStatement) Type() types.StatementType {
	return types.StmtInsert
}

func (s *insertStatement) TableName() string {
	return s.tableName
}

func (s *insertStatement) Columns() []string {
	return s.columns
}

func (s *insertStatement) Values() [][]Value {
	return s.values
}

// updateStatement implements UpdateStatement
type updateStatement struct {
	tableName  string
	setClauses map[string]Expression
	whereExpr  Expression
}

func (s *updateStatement) Type() types.StatementType {
	return types.StmtUpdate
}

func (s *updateStatement) TableName() string {
	return s.tableName
}

func (s *updateStatement) SetClauses() map[string]Expression {
	return s.setClauses
}

func (s *updateStatement) WhereClause() Expression {
	return s.whereExpr
}

// deleteStatement implements DeleteStatement
type deleteStatement struct {
	tableName string
	whereExpr Expression
}

func (s *deleteStatement) Type() types.StatementType {
	return types.StmtDelete
}

func (s *deleteStatement) TableName() string {
	return s.tableName
}

func (s *deleteStatement) WhereClause() Expression {
	return s.whereExpr
}

// selectStatement implements SelectStatement
type selectStatement struct {
	tableName string
	columns   []string
	whereExpr Expression
}

func (s *selectStatement) Type() types.StatementType {
	return types.StmtSelect
}

func (s *selectStatement) TableName() string {
	return s.tableName
}

func (s *selectStatement) Columns() []string {
	return s.columns
}

func (s *selectStatement) WhereClause() Expression {
	return s.whereExpr
}

// Column definition implementation
type columnDefinition struct {
	name        string
	dataType    types.DataType
	constraints []types.Constraint
}

func (c *columnDefinition) Name() string {
	return c.name
}

func (c *columnDefinition) Type() types.DataType {
	return c.dataType
}

func (c *columnDefinition) Constraints() []types.Constraint {
	return c.constraints
}

// Expression implementations

// literalValue implements Value
type literalValue struct {
	dataType  types.DataType
	intVal    int64
	floatVal  float64
	stringVal string
	boolVal   bool
}

func (v *literalValue) Type() types.DataType {
	return v.dataType
}

func (v *literalValue) AsInt() (int64, error) {
	return v.intVal, nil
}

func (v *literalValue) AsFloat() (float64, error) {
	return v.floatVal, nil
}

func (v *literalValue) AsString() (string, error) {
	return v.stringVal, nil
}

func (v *literalValue) AsBool() (bool, error) {
	return v.boolVal, nil
}

func (v *literalValue) AsNull() (bool, error) {
	return v.dataType == types.TypeNull, nil
}

// literalExpression represents a literal value in an expression
type literalExpression struct {
	val Value
}

func (e *literalExpression) Eval(row map[string]Value) (Value, error) {
	return e.val, nil
}

// columnExpression represents a column reference in an expression
type columnExpression struct {
	columnName string
}

func (e *columnExpression) Eval(row map[string]Value) (Value, error) {
	val, ok := row[e.columnName]
	if !ok {
		return &literalValue{dataType: types.TypeNull}, nil
	}
	return val, nil
}

// binaryExpression represents a binary operation in an expression
type binaryExpression struct {
	left     Expression
	right    Expression
	operator string
}

func (e *binaryExpression) Eval(row map[string]Value) (Value, error) {
	leftVal, err := e.left.Eval(row)
	if err != nil {
		return nil, err
	}

	rightVal, err := e.right.Eval(row)
	if err != nil {
		return nil, err
	}

	// For simplicity, only implement equality comparison
	if e.operator == "=" {
		// Handle different type combinations
		switch leftVal.Type() {
		case types.TypeInt:
			leftInt, _ := leftVal.AsInt()

			switch rightVal.Type() {
			case types.TypeInt:
				rightInt, _ := rightVal.AsInt()
				return &literalValue{
					dataType: types.TypeBool,
					boolVal:  leftInt == rightInt,
				}, nil
			case types.TypeString:
				// Try to convert string to int for comparison
				rightStr, _ := rightVal.AsString()
				var rightInt int64
				if _, err := fmt.Sscanf(rightStr, "%d", &rightInt); err == nil {
					return &literalValue{
						dataType: types.TypeBool,
						boolVal:  leftInt == rightInt,
					}, nil
				}
			}

		case types.TypeString:
			leftStr, _ := leftVal.AsString()

			switch rightVal.Type() {
			case types.TypeString:
				rightStr, _ := rightVal.AsString()
				return &literalValue{
					dataType: types.TypeBool,
					boolVal:  leftStr == rightStr,
				}, nil
			case types.TypeInt:
				// Try to convert string to int for comparison
				rightInt, _ := rightVal.AsInt()
				leftIntStr := fmt.Sprintf("%d", rightInt)
				return &literalValue{
					dataType: types.TypeBool,
					boolVal:  leftStr == leftIntStr,
				}, nil
			}

		case types.TypeBool:
			leftBool, _ := leftVal.AsBool()

			switch rightVal.Type() {
			case types.TypeBool:
				rightBool, _ := rightVal.AsBool()
				return &literalValue{
					dataType: types.TypeBool,
					boolVal:  leftBool == rightBool,
				}, nil
			case types.TypeString:
				// Try to convert string to bool for comparison
				rightStr, _ := rightVal.AsString()
				rightBool := strings.EqualFold(rightStr, "true")
				return &literalValue{
					dataType: types.TypeBool,
					boolVal:  leftBool == rightBool,
				}, nil
			}
		}
	}

	// Default to false for unsupported operations or type combinations
	return &literalValue{dataType: types.TypeBool, boolVal: false}, nil
}
