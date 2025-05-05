package diskbased

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/zhangbiao2009/simple-sql-db/pkg/catalog"
	"github.com/zhangbiao2009/simple-sql-db/pkg/parser"
	"github.com/zhangbiao2009/simple-sql-db/pkg/types"
)

// RowIDGenerator generates and manages row IDs
type RowIDGenerator interface {
	// Generate creates a new RowID for a row
	Generate(row map[string]parser.Value) (RowID, error)

	// Extract extracts the primary key or auto-generated ID from a row
	Extract(row map[string]parser.Value) (RowID, error)

	// NextAutoID returns the next auto-increment ID
	NextAutoID() int64
}

// RowID represents a unique identifier for a row in a table
// It can be either a primary key or an auto-generated key
type RowID interface {
	// Bytes returns the byte representation of the row ID for B+ tree storage
	Bytes() []byte

	// String returns a string representation for debugging
	String() string

	// Compare compares this RowID with another
	Compare(other RowID) int
}

// AutoRowID is a simple auto-incremented row ID
type AutoRowID struct {
	ID int64
}

// PrimaryKeyRowID is a row ID based on a primary key
type PrimaryKeyRowID struct {
	Values []parser.Value
	Types  []types.DataType
}

// CompositeRowID represents a rowID composed of one or more primary key values
type CompositeRowID struct {
	values []parser.Value
	types  []types.DataType
}

// NewAutoRowID creates a new auto-generated row ID
func NewAutoRowID(id int64) *AutoRowID {
	return &AutoRowID{ID: id}
}

// Bytes returns the byte representation of an auto-generated row ID
func (r *AutoRowID) Bytes() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(r.ID))
	return buf
}

// String returns a string representation of an auto-generated row ID
func (r *AutoRowID) String() string {
	return fmt.Sprintf("AutoID:%d", r.ID)
}

// Compare compares this AutoRowID with another RowID
func (r *AutoRowID) Compare(other RowID) int {
	if auto, ok := other.(*AutoRowID); ok {
		if r.ID < auto.ID {
			return -1
		} else if r.ID > auto.ID {
			return 1
		}
		return 0
	}
	// Different types are incomparable, return arbitrary order
	return -1
}

// NewPrimaryKeyRowID creates a new row ID based on a primary key
func NewPrimaryKeyRowID(value parser.Value, dataType types.DataType) *PrimaryKeyRowID {
	return &PrimaryKeyRowID{
		Values: []parser.Value{value},
		Types:  []types.DataType{dataType},
	}
}

// Bytes returns the byte representation of a primary key row ID
func (r *PrimaryKeyRowID) Bytes() []byte {
	var buf bytes.Buffer

	// We only handle single primary key for this implementation
	val := r.Values[0]
	switch r.Types[0] {
	case types.TypeInt:
		intVal, _ := val.AsInt()
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(intVal))
		buf.Write(b)
	case types.TypeString:
		strVal, _ := val.AsString()
		buf.WriteString(strVal)
	// Handle other types as needed
	default:
		// Fallback to string representation
		buf.WriteString(fmt.Sprintf("%v", val))
	}

	return buf.Bytes()
}

// String returns a string representation of a primary key row ID
func (r *PrimaryKeyRowID) String() string {
	val := r.Values[0]
	return fmt.Sprintf("PK:%v", val)
}

// Compare compares this PrimaryKeyRowID with another RowID
func (r *PrimaryKeyRowID) Compare(other RowID) int {
	if pk, ok := other.(*PrimaryKeyRowID); ok {
		// Compare the primary key values
		if len(r.Values) != len(pk.Values) {
			return len(r.Values) - len(pk.Values)
		}

		for i := 0; i < len(r.Values); i++ {
			switch r.Types[i] {
			case types.TypeInt:
				v1, _ := r.Values[i].AsInt()
				v2, _ := pk.Values[i].AsInt()
				if v1 < v2 {
					return -1
				} else if v1 > v2 {
					return 1
				}
			case types.TypeString:
				v1, _ := r.Values[i].AsString()
				v2, _ := pk.Values[i].AsString()
				cmp := bytes.Compare([]byte(v1), []byte(v2))
				if cmp != 0 {
					return cmp
				}
			// Handle other types as needed
			default:
				// Fallback to string comparison
				v1 := fmt.Sprintf("%v", r.Values[i])
				v2 := fmt.Sprintf("%v", pk.Values[i])
				cmp := bytes.Compare([]byte(v1), []byte(v2))
				if cmp != 0 {
					return cmp
				}
			}
		}
		return 0
	}
	// Different types are incomparable, return arbitrary order
	return 1
}

// NewCompositeRowID creates a new composite row ID
func NewCompositeRowID(values []parser.Value, types []types.DataType) *CompositeRowID {
	return &CompositeRowID{
		values: values,
		types:  types,
	}
}

// Values returns the underlying values
func (r *CompositeRowID) Values() []parser.Value {
	return r.values
}

// Types returns the underlying data types
func (r *CompositeRowID) Types() []types.DataType {
	return r.types
}

// String returns a string representation of a composite row ID
func (c *CompositeRowID) String() string {
	result := "CompositeID:"
	for i, val := range c.values {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%v", val)
	}
	return result
}

// Compare compares this CompositeRowID with another RowID
func (c *CompositeRowID) Compare(other RowID) int {
	if comp, ok := other.(*CompositeRowID); ok {
		// Compare the length of values first
		if len(c.values) != len(comp.values) {
			return len(c.values) - len(comp.values)
		}

		// Compare each value
		for i := 0; i < len(c.values); i++ {
			switch c.types[i] {
			case types.TypeInt:
				v1, _ := c.values[i].AsInt()
				v2, _ := comp.values[i].AsInt()
				if v1 < v2 {
					return -1
				} else if v1 > v2 {
					return 1
				}
			case types.TypeString:
				v1, _ := c.values[i].AsString()
				v2, _ := comp.values[i].AsString()
				cmp := bytes.Compare([]byte(v1), []byte(v2))
				if cmp != 0 {
					return cmp
				}
			case types.TypeBool:
				v1, _ := c.values[i].AsBool()
				v2, _ := comp.values[i].AsBool()
				if !v1 && v2 {
					return -1
				} else if v1 && !v2 {
					return 1
				}
			default:
				// Fallback to string comparison
				v1 := fmt.Sprintf("%v", c.values[i])
				v2 := fmt.Sprintf("%v", comp.values[i])
				cmp := bytes.Compare([]byte(v1), []byte(v2))
				if cmp != 0 {
					return cmp
				}
			}
		}
		return 0
	}
	// Different types are incomparable, return arbitrary order
	return 1
}

// Bytes serializes the composite row ID to bytes
func (c *CompositeRowID) Bytes() []byte {
	// First byte is the number of components
	result := make([]byte, 1)
	result[0] = byte(len(c.values))

	// For each component, add type and value
	for i, value := range c.values {
		result = append(result, byte(c.types[i]))

		switch c.types[i] {
		case types.TypeInt:
			intVal, _ := value.AsInt()
			intBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(intBytes, uint64(intVal))
			result = append(result, intBytes...)
		case types.TypeString:
			strVal, _ := value.AsString()
			strBytes := []byte(strVal)
			// Add length of string first (4 bytes)
			lenBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBytes, uint32(len(strBytes)))
			result = append(result, lenBytes...)
			// Then add string bytes
			result = append(result, strBytes...)
		case types.TypeBool:
			boolVal, _ := value.AsBool()
			if boolVal {
				result = append(result, 1)
			} else {
				result = append(result, 0)
			}
		}
	}

	return result
}

// FromBytes deserializes a CompositeRowID from bytes
func FromBytes(data []byte) (*CompositeRowID, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("data too short to be a composite row ID")
	}

	// First byte is the number of components
	count := int(data[0])
	offset := 1

	values := make([]parser.Value, count)
	typesList := make([]types.DataType, count)

	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("data too short to read component %d", i)
		}

		// Read the data type
		dataType := types.DataType(data[offset])
		typesList[i] = dataType
		offset++

		switch dataType {
		case types.TypeInt:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("data too short to read int at component %d", i)
			}
			intVal := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			values[i] = parser.NewIntValue(intVal)
			offset += 8

		case types.TypeString:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("data too short to read string length at component %d", i)
			}
			strLen := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			if offset+int(strLen) > len(data) {
				return nil, fmt.Errorf("data too short to read string at component %d", i)
			}
			strVal := string(data[offset : offset+int(strLen)])
			values[i] = parser.NewStringValue(strVal)
			offset += int(strLen)

		case types.TypeBool:
			if offset >= len(data) {
				return nil, fmt.Errorf("data too short to read bool at component %d", i)
			}
			boolVal := data[offset] != 0
			values[i] = parser.NewBoolValue(boolVal)
			offset++

		default:
			return nil, fmt.Errorf("unknown data type %d at component %d", dataType, i)
		}
	}

	return NewCompositeRowID(values, typesList), nil
}

// TableRowIDGenerator manages generation of row IDs for a specific table
type TableRowIDGenerator struct {
	Schema           catalog.TableSchema
	AutoID           int64
	PrimaryKeyFields []string
	HasPrimaryKey    bool
}

// NewTableRowIDGenerator creates a generator for a specific table
func NewTableRowIDGenerator(schema catalog.TableSchema) *TableRowIDGenerator {
	primaryKeys := []string{}
	hasPrimaryKey := false

	// Identify primary key columns
	for _, col := range schema.Columns() {
		// Check if this column has a primary key constraint
		for _, constraint := range col.Constraints() {
			if constraint == types.ConstraintPrimaryKey {
				primaryKeys = append(primaryKeys, col.Name())
				hasPrimaryKey = true
				break
			}
		}
	}

	return &TableRowIDGenerator{
		Schema:           schema,
		AutoID:           1, // Start auto-increment from 1
		PrimaryKeyFields: primaryKeys,
		HasPrimaryKey:    hasPrimaryKey,
	}
}

// Generate creates a RowID from a row
func (g *TableRowIDGenerator) Generate(row map[string]parser.Value) (RowID, error) {
	if g.HasPrimaryKey {
		// Extract primary key values
		if len(g.PrimaryKeyFields) == 1 {
			// Single primary key
			pkField := g.PrimaryKeyFields[0]
			pkValue, exists := row[pkField]
			if !exists {
				return nil, fmt.Errorf("primary key field %s not found in row", pkField)
			}

			// Get the data type for the primary key
			pkCol, _ := g.Schema.GetColumn(pkField)
			return NewPrimaryKeyRowID(pkValue, pkCol.Type()), nil
		} else {
			// Composite primary key
			values := make([]parser.Value, len(g.PrimaryKeyFields))
			types := make([]types.DataType, len(g.PrimaryKeyFields))

			for i, field := range g.PrimaryKeyFields {
				val, exists := row[field]
				if !exists {
					return nil, fmt.Errorf("primary key field %s not found in row", field)
				}
				values[i] = val

				// Get the data type for this component of the primary key
				col, _ := g.Schema.GetColumn(field)
				types[i] = col.Type()
			}

			return NewCompositeRowID(values, types), nil
		}
	}

	// No primary key defined, use auto-increment ID
	autoID := g.AutoID
	g.AutoID++
	return NewAutoRowID(autoID), nil
}

// Extract extracts the RowID from an existing row
func (g *TableRowIDGenerator) Extract(row map[string]parser.Value) (RowID, error) {
	// Check for hidden rowid column first
	if autoIDVal, exists := row["_rowid"]; exists {
		idStr, err := autoIDVal.AsString()
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return nil, err
		}
		return NewAutoRowID(id), nil
	}

	// Then check for primary keys
	if g.HasPrimaryKey {
		if len(g.PrimaryKeyFields) == 1 {
			// Single primary key
			pkField := g.PrimaryKeyFields[0]
			pkValue, exists := row[pkField]
			if !exists {
				return nil, fmt.Errorf("primary key field %s not found in row", pkField)
			}

			pkCol, _ := g.Schema.GetColumn(pkField)
			return NewPrimaryKeyRowID(pkValue, pkCol.Type()), nil
		} else {
			// Composite primary key
			values := make([]parser.Value, len(g.PrimaryKeyFields))
			types := make([]types.DataType, len(g.PrimaryKeyFields))

			for i, field := range g.PrimaryKeyFields {
				val, exists := row[field]
				if !exists {
					return nil, fmt.Errorf("primary key field %s not found in row", field)
				}
				values[i] = val

				col, _ := g.Schema.GetColumn(field)
				types[i] = col.Type()
			}

			return NewCompositeRowID(values, types), nil
		}
	}

	// If no RowID can be extracted, return an error
	return nil, fmt.Errorf("no primary key or _rowid column found in row")
}

// NextAutoID returns the next auto-increment ID value
func (g *TableRowIDGenerator) NextAutoID() int64 {
	return g.AutoID
}

// UpdateAutoID updates the auto-increment counter to ensure new IDs are greater than existingID
func (g *TableRowIDGenerator) UpdateAutoID(existingID int64) {
	if existingID >= g.AutoID {
		g.AutoID = existingID + 1
	}
}

// SerializeRow converts a row to bytes for storage
func SerializeRow(row map[string]parser.Value) ([]byte, error) {
	return json.Marshal(row)
}

// DeserializeRow converts bytes back to a row
func DeserializeRow(data []byte) (map[string]parser.Value, error) {
	var row map[string]parser.Value
	err := json.Unmarshal(data, &row)
	return row, err
}
