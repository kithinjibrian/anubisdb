package catalog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

type RowValue struct {
	Type  ColumnType  `json:"type"`
	Value interface{} `json:"value"`
}

type Row struct {
	Values map[string]RowValue `json:"values"`
}

func SerializeRow(row *Row) ([]byte, error) {
	return json.Marshal(row.Values)
}

func DeserializeRow(data []byte) (*Row, error) {
	row := &Row{
		Values: make(map[string]RowValue),
	}
	if err := json.Unmarshal(data, &row.Values); err != nil {
		return nil, err
	}
	return row, nil
}

func ExtractColumnValue(row *Row, columnName string) (interface{}, ColumnType, error) {
	rowValue, exists := row.Values[columnName]
	if !exists {
		return nil, "", fmt.Errorf("column '%s' not found in row", columnName)
	}
	return rowValue.Value, rowValue.Type, nil
}

func CreateRow(schema *Schema, values []interface{}) (*Row, error) {
	if len(values) != len(schema.Columns) {
		return nil, fmt.Errorf("value count mismatch: got %d, expected %d",
			len(values), len(schema.Columns))
	}

	row := &Row{
		Values: make(map[string]RowValue),
	}

	for i, col := range schema.Columns {
		if col.NotNull && values[i] == nil {
			return nil, fmt.Errorf("column '%s' cannot be NULL", col.Name)
		}

		row.Values[col.Name] = RowValue{
			Type:  col.Type,
			Value: values[i],
		}
	}

	return row, nil
}

func GetPrimaryKeyValue(row *Row, schema *Schema) (storage.Key, error) {
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			value, colType, err := ExtractColumnValue(row, col.Name)
			if err != nil {
				return nil, err
			}
			return ValueToKey(value, colType)
		}
	}
	return nil, errors.New("no primary key column found")
}

func ValidateRow(row *Row, schema *Schema) error {
	for _, col := range schema.Columns {
		rowValue, exists := row.Values[col.Name]
		if !exists {
			if col.NotNull {
				return fmt.Errorf("column '%s' is required (NOT NULL)", col.Name)
			}
			continue
		}

		if rowValue.Type != col.Type {
			return fmt.Errorf("column '%s' type mismatch: expected %s, got %s",
				col.Name, col.Type, rowValue.Type)
		}

		if col.NotNull && rowValue.Value == nil {
			return fmt.Errorf("column '%s' cannot be NULL", col.Name)
		}
	}

	return nil
}

func ValueToKey(value interface{}, columnType ColumnType) (storage.Key, error) {
	switch columnType {
	case TypeInt:
		switch v := value.(type) {
		case int64:
			return storage.NewIntKey(v), nil
		case float64:
			return storage.NewIntKey(int64(v)), nil
		case int:
			return storage.NewIntKey(int64(v)), nil
		default:
			return nil, fmt.Errorf("invalid int value type: %T", value)
		}
	case TypeText:
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("invalid text value type: %T", value)
		}
		return storage.NewTextKey(str), nil
	case TypeFloat:
		switch v := value.(type) {
		case float64:
			return storage.NewFloatKey(v), nil
		case int64:
			return storage.NewFloatKey(float64(v)), nil
		case int:
			return storage.NewFloatKey(float64(v)), nil
		default:
			return nil, fmt.Errorf("invalid float value type: %T", value)
		}
	case TypeBoolean:
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("invalid boolean value type: %T", value)
		}
		return storage.NewBooleanKey(b), nil
	default:
		return nil, fmt.Errorf("unsupported column type: %s", columnType)
	}
}

func ValuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch v1 := a.(type) {
	case int, int8, int16, int32, int64:
		switch v2 := b.(type) {
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
		}
	case uint, uint8, uint16, uint32, uint64:
		switch v2 := b.(type) {
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
		}
	case float32, float64:
		switch v2 := b.(type) {
		case float32, float64:
			return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
		}
	case string:
		v2, ok := b.(string)
		return ok && v1 == v2
	case bool:
		v2, ok := b.(bool)
		return ok && v1 == v2
	case []byte:
		v2, ok := b.([]byte)
		if !ok {
			return false
		}
		if len(v1) != len(v2) {
			return false
		}
		for i := range v1 {
			if v1[i] != v2[i] {
				return false
			}
		}
		return true
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
