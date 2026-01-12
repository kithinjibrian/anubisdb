package catalog

import (
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

type Table struct {
	catalog *Catalog
	schema  *Schema
	btree   *storage.BTree
}

func (c *Catalog) GetTable(tableName string) (*Table, error) {
	schema, err := c.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	btree, err := storage.LoadBTree(c.pager, schema.RootPage)
	if err != nil {
		return nil, fmt.Errorf("failed to load table B-tree: %w", err)
	}

	return &Table{
		catalog: c,
		schema:  schema,
		btree:   btree,
	}, nil
}

func (t *Table) Insert(values []interface{}) error {

	row, err := CreateRow(t.schema, values)
	if err != nil {
		return fmt.Errorf("invalid row: %w", err)
	}

	if err := ValidateRow(row, t.schema); err != nil {
		return fmt.Errorf("row validation failed: %w", err)
	}

	primaryKey, err := GetPrimaryKeyValue(row, t.schema)
	if err != nil {
		return fmt.Errorf("failed to get primary key: %w", err)
	}

	_, err = t.btree.Search(primaryKey)
	if err == nil {
		return fmt.Errorf("primary key already exists: %v", primaryKey)
	}

	rowData, err := SerializeRow(row)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	if err := t.btree.Insert(primaryKey, rowData); err != nil {
		return fmt.Errorf("failed to insert into table: %w", err)
	}

	return nil
}

func (t *Table) Get(key storage.Key) (*Row, error) {
	rowData, err := t.btree.Search(key)
	if err != nil {
		return nil, fmt.Errorf("row not found: %w", err)
	}

	row, err := DeserializeRow(rowData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize row: %w", err)
	}

	return row, nil
}

func (t *Table) Delete(key storage.Key) error {
	if err := t.btree.Delete(key); err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}

	return nil
}

func (t *Table) Update(key storage.Key, newValues []interface{}) error {

	newRow, err := CreateRow(t.schema, newValues)
	if err != nil {
		return fmt.Errorf("invalid row: %w", err)
	}

	if err := ValidateRow(newRow, t.schema); err != nil {
		return fmt.Errorf("row validation failed: %w", err)
	}

	rowData, err := SerializeRow(newRow)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	if err := t.btree.Update(key, rowData); err != nil {
		return fmt.Errorf("failed to update row: %w", err)
	}

	return nil
}

func (t *Table) Scan() ([]*Row, error) {
	entries, err := t.btree.Scan()
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	rows := make([]*Row, 0, len(entries))
	for _, entry := range entries {
		row, err := DeserializeRow(entry.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %w", err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (t *Table) Count() (int, error) {
	return t.btree.Count()
}

func (t *Table) GetSchema() *Schema {
	return t.schema
}
