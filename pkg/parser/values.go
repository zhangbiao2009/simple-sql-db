// Package parser provides SQL parsing functionality
package parser

import (
	"fmt"
	"strconv"

	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

// IntValue represents an integer value
type IntValue struct {
	value int64
}

// NewIntValue creates a new integer value
func NewIntValue(value int64) Value {
	return &IntValue{value: value}
}

func (v *IntValue) Type() types.DataType {
	return types.TypeInt
}

func (v *IntValue) AsInt() (int64, error) {
	return v.value, nil
}

func (v *IntValue) AsString() (string, error) {
	return fmt.Sprintf("%d", v.value), nil
}

func (v *IntValue) AsBool() (bool, error) {
	return v.value != 0, nil
}

func (v *IntValue) AsFloat() (float64, error) {
	return float64(v.value), nil
}

func (v *IntValue) AsNull() (bool, error) {
	return false, nil
}

func (v *IntValue) String() string {
	return fmt.Sprintf("%d", v.value)
}

// StringValue represents a string value
type StringValue struct {
	value string
}

// NewStringValue creates a new string value
func NewStringValue(value string) Value {
	return &StringValue{value: value}
}

func (v *StringValue) Type() types.DataType {
	return types.TypeString
}

func (v *StringValue) AsInt() (int64, error) {
	i, err := strconv.ParseInt(v.value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot convert string '%s' to int: %v", v.value, err)
	}
	return i, nil
}

func (v *StringValue) AsString() (string, error) {
	return v.value, nil
}

func (v *StringValue) AsBool() (bool, error) {
	b, err := strconv.ParseBool(v.value)
	if err != nil {
		// Check for "0"/"1" strings
		if v.value == "0" {
			return false, nil
		} else if v.value == "1" {
			return true, nil
		}
		return false, fmt.Errorf("cannot convert string '%s' to bool: %v", v.value, err)
	}
	return b, nil
}

func (v *StringValue) AsFloat() (float64, error) {
	f, err := strconv.ParseFloat(v.value, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot convert string '%s' to float: %v", v.value, err)
	}
	return f, nil
}

func (v *StringValue) AsNull() (bool, error) {
	return false, nil
}

func (v *StringValue) String() string {
	return fmt.Sprintf("'%s'", v.value)
}

// BoolValue represents a boolean value
type BoolValue struct {
	value bool
}

// NewBoolValue creates a new boolean value
func NewBoolValue(value bool) Value {
	return &BoolValue{value: value}
}

func (v *BoolValue) Type() types.DataType {
	return types.TypeBool
}

func (v *BoolValue) AsInt() (int64, error) {
	if v.value {
		return 1, nil
	}
	return 0, nil
}

func (v *BoolValue) AsString() (string, error) {
	return strconv.FormatBool(v.value), nil
}

func (v *BoolValue) AsBool() (bool, error) {
	return v.value, nil
}

func (v *BoolValue) AsFloat() (float64, error) {
	if v.value {
		return 1.0, nil
	}
	return 0.0, nil
}

func (v *BoolValue) AsNull() (bool, error) {
	return false, nil
}

func (v *BoolValue) String() string {
	return strconv.FormatBool(v.value)
}

// FloatValue represents a floating-point value
type FloatValue struct {
	value float64
}

// NewFloatValue creates a new floating-point value
func NewFloatValue(value float64) Value {
	return &FloatValue{value: value}
}

func (v *FloatValue) Type() types.DataType {
	return types.TypeFloat
}

func (v *FloatValue) AsInt() (int64, error) {
	return int64(v.value), nil
}

func (v *FloatValue) AsString() (string, error) {
	return fmt.Sprintf("%g", v.value), nil
}

func (v *FloatValue) AsBool() (bool, error) {
	return v.value != 0, nil
}

func (v *FloatValue) AsFloat() (float64, error) {
	return v.value, nil
}

func (v *FloatValue) AsNull() (bool, error) {
	return false, nil
}

func (v *FloatValue) String() string {
	return fmt.Sprintf("%g", v.value)
}

// NullValue represents a NULL value
type NullValue struct{}

// NewNullValue creates a new NULL value
func NewNullValue() Value {
	return &NullValue{}
}

func (v *NullValue) Type() types.DataType {
	return types.TypeNull
}

func (v *NullValue) AsInt() (int64, error) {
	return 0, fmt.Errorf("cannot convert NULL to int")
}

func (v *NullValue) AsString() (string, error) {
	return "NULL", nil
}

func (v *NullValue) AsBool() (bool, error) {
	return false, fmt.Errorf("cannot convert NULL to bool")
}

func (v *NullValue) AsFloat() (float64, error) {
	return 0, fmt.Errorf("cannot convert NULL to float")
}

func (v *NullValue) AsNull() (bool, error) {
	return true, nil
}

func (v *NullValue) String() string {
	return "NULL"
}
