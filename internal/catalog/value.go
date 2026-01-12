package catalog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

type RowValue struct {
	Type  ColumnType
	Value interface{}
}

type Row struct {
	Values map[string]RowValue
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
