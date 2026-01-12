package catalog

import (
	"errors"
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

type Table struct {
	Catalog *Catalog
	schema  *Schema
	btree   *storage.BTree
}

func NewTable(catalog *Catalog, schema *Schema, btree *storage.BTree) *Table {
	return &Table{
		Catalog: catalog,
		schema:  schema,
		btree:   btree,
	}
}

func (t *Table) getIndexTree(idxMeta *IndexMetadata) (*storage.BTree, error) {

	idxTree, err := storage.LoadBTree(t.Catalog.pager, idxMeta.RootPage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to load index %s: %w", idxMeta.Name, err)
	}

	return idxTree, nil
}

func (t *Table) getPrimaryKeyColumnName() string {
	for _, col := range t.schema.Columns {
		if col.PrimaryKey {
			return col.Name
		}
	}
	return ""
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

	rowData, err := SerializeRow(row)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	if err := t.btree.Insert(primaryKey, rowData); err != nil {
		return fmt.Errorf("failed to insert into table %s: %w", t.schema.Name, err)
	}

	var insertedIndexes []string

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	for _, idxMeta := range indexes {

		if idxMeta.ColumnName == t.getPrimaryKeyColumnName() {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {
			t.rollbackInsert(primaryKey, insertedIndexes, row)
			return err
		}

		val := row.Values[idxMeta.ColumnName]
		col := t.schema.GetColumn(idxMeta.ColumnName)
		if col == nil {
			t.rollbackInsert(primaryKey, insertedIndexes, row)
			return fmt.Errorf("column %s not found in schema", idxMeta.ColumnName)
		}

		idxKey, err := ValueToKey(val.Value, col.Type)
		if err != nil {
			t.rollbackInsert(primaryKey, insertedIndexes, row)
			return fmt.Errorf("failed to create index key for %s: %w", idxMeta.Name, err)
		}

		if err := idxTree.Insert(idxKey, primaryKey.Encode()); err != nil {
			t.rollbackInsert(primaryKey, insertedIndexes, row)
			if idxMeta.Unique {
				return fmt.Errorf("unique constraint violation on index %s: value '%v' already exists",
					idxMeta.Name, val.Value)
			}
			return fmt.Errorf("failed to insert into index %s: %w", idxMeta.Name, err)
		}

		insertedIndexes = append(insertedIndexes, idxMeta.Name)
	}

	return nil
}

func (t *Table) rollbackInsert(primaryKey storage.Key, insertedIndexes []string, row *Row) {

	if err := t.btree.Delete(primaryKey); err != nil {
		fmt.Printf("Warning: failed to rollback main table insert: %v\n", err)
	}

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	for _, idxMeta := range indexes {

		found := false
		for _, name := range insertedIndexes {
			if name == idxMeta.Name {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {
			fmt.Printf("Warning: failed to load index %s during rollback: %v\n", idxMeta.Name, err)
			continue
		}

		val := row.Values[idxMeta.ColumnName]
		col := t.schema.GetColumn(idxMeta.ColumnName)
		if col == nil {
			continue
		}

		idxKey, err := ValueToKey(val.Value, col.Type)
		if err != nil {
			fmt.Printf("Warning: failed to create index key during rollback: %v\n", err)
			continue
		}

		if err := idxTree.Delete(idxKey); err != nil {
			fmt.Printf("Warning: failed to delete from index %s during rollback: %v\n", idxMeta.Name, err)
		}
	}
}

func (t *Table) Get(key storage.Key) (*Row, error) {
	rowData, err := t.btree.Search(key)
	if err != nil {
		return nil, fmt.Errorf("row not found in table %s: %w", t.schema.Name, err)
	}

	row, err := DeserializeRow(rowData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize row: %w", err)
	}

	return row, nil
}

func (t *Table) Delete(key storage.Key) error {

	row, err := t.Get(key)
	if err != nil {
		return fmt.Errorf("row not found: %w", err)
	}

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	var deletedIndexes []string

	for _, idxMeta := range indexes {

		if idxMeta.ColumnName == t.getPrimaryKeyColumnName() {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {

			fmt.Printf("Warning: failed to load index %s during delete: %v\n", idxMeta.Name, err)
			continue
		}

		val := row.Values[idxMeta.ColumnName]
		col := t.schema.GetColumn(idxMeta.ColumnName)
		if col == nil {
			fmt.Printf("Warning: column %s not found during delete\n", idxMeta.ColumnName)
			continue
		}

		idxKey, err := ValueToKey(val.Value, col.Type)
		if err != nil {
			fmt.Printf("Warning: failed to create index key during delete: %v\n", err)
			continue
		}

		if err := idxTree.Delete(idxKey); err != nil {
			fmt.Printf("Warning: failed to delete from index %s: %v\n", idxMeta.Name, err)
		} else {
			deletedIndexes = append(deletedIndexes, idxMeta.Name)
		}
	}

	if err := t.btree.Delete(key); err != nil {

		t.rollbackDelete(key, row, deletedIndexes)
		return fmt.Errorf("failed to delete row from table %s: %w", t.schema.Name, err)
	}

	return nil
}

func (t *Table) rollbackDelete(primaryKey storage.Key, row *Row, deletedIndexes []string) {
	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	for _, idxMeta := range indexes {

		found := false
		for _, name := range deletedIndexes {
			if name == idxMeta.Name {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {
			continue
		}

		val := row.Values[idxMeta.ColumnName]
		col := t.schema.GetColumn(idxMeta.ColumnName)
		if col == nil {
			continue
		}

		idxKey, err := ValueToKey(val.Value, col.Type)
		if err != nil {
			continue
		}

		if err := idxTree.Insert(idxKey, primaryKey.Encode()); err != nil {
			fmt.Printf("Warning: failed to rollback index %s deletion: %v\n", idxMeta.Name, err)
		}
	}
}

func (t *Table) Update(key storage.Key, newValues []interface{}) error {

	oldRow, err := t.Get(key)
	if err != nil {
		return fmt.Errorf("row not found: %w", err)
	}

	newRow, err := CreateRow(t.schema, newValues)
	if err != nil {
		return fmt.Errorf("invalid row: %w", err)
	}

	if err := ValidateRow(newRow, t.schema); err != nil {
		return fmt.Errorf("row validation failed: %w", err)
	}

	newPK, err := GetPrimaryKeyValue(newRow, t.schema)
	if err != nil {
		return fmt.Errorf("failed to get primary key: %w", err)
	}

	if key.Compare(newPK) != 0 {
		return errors.New("cannot update primary key value - use delete and insert instead")
	}

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	var updatedIndexes []indexUpdate

	for _, idxMeta := range indexes {
		if idxMeta.ColumnName == t.getPrimaryKeyColumnName() {
			continue
		}

		oldVal := oldRow.Values[idxMeta.ColumnName]
		newVal := newRow.Values[idxMeta.ColumnName]

		if ValuesEqual(oldVal.Value, newVal.Value) {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {
			t.rollbackUpdate(updatedIndexes)
			return err
		}

		col := t.schema.GetColumn(idxMeta.ColumnName)
		if col == nil {
			t.rollbackUpdate(updatedIndexes)
			return fmt.Errorf("column %s not found", idxMeta.ColumnName)
		}

		oldKey, err := ValueToKey(oldVal.Value, col.Type)
		if err != nil {
			t.rollbackUpdate(updatedIndexes)
			return fmt.Errorf("failed to create old index key: %w", err)
		}

		if err := idxTree.Delete(oldKey); err != nil {

			fmt.Printf("Warning: failed to delete old index entry from %s: %v\n", idxMeta.Name, err)
		}

		newKey, err := ValueToKey(newVal.Value, col.Type)
		if err != nil {
			t.rollbackUpdate(updatedIndexes)
			return fmt.Errorf("failed to create new index key: %w", err)
		}

		if err := idxTree.Insert(newKey, key.Encode()); err != nil {
			t.rollbackUpdate(updatedIndexes)
			if idxMeta.Unique {
				return fmt.Errorf("unique constraint violation on index %s: value '%v' already exists",
					idxMeta.Name, newVal.Value)
			}
			return fmt.Errorf("failed to insert into index %s: %w", idxMeta.Name, err)
		}

		updatedIndexes = append(updatedIndexes, indexUpdate{
			name:   idxMeta.Name,
			oldKey: oldKey,
			newKey: newKey,
		})
	}

	rowData, err := SerializeRow(newRow)
	if err != nil {
		t.rollbackUpdate(updatedIndexes)
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	if err := t.btree.Update(key, rowData); err != nil {
		t.rollbackUpdate(updatedIndexes)
		return fmt.Errorf("failed to update row in table %s: %w", t.schema.Name, err)
	}

	return nil
}

type indexUpdate struct {
	name   string
	oldKey storage.Key
	newKey storage.Key
}

func (t *Table) rollbackUpdate(updates []indexUpdate) {
	for _, update := range updates {
		indexes := t.Catalog.GetTableIndexes(t.schema.Name)
		var idxMeta *IndexMetadata
		for _, idx := range indexes {
			if idx.Name == update.name {
				idxMeta = idx
				break
			}
		}
		if idxMeta == nil {
			continue
		}

		idxTree, err := t.getIndexTree(idxMeta)
		if err != nil {
			continue
		}

		idxTree.Delete(update.newKey)

	}
}

func (t *Table) GetByIndex(indexName string, value interface{}) (*Row, error) {

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	var idxMeta *IndexMetadata
	for _, idx := range indexes {
		if idx.Name == indexName {
			idxMeta = idx
			break
		}
	}

	if idxMeta == nil {
		return nil, fmt.Errorf("index %s not found on table %s", indexName, t.schema.Name)
	}

	col := t.schema.GetColumn(idxMeta.ColumnName)
	if col == nil {
		return nil, fmt.Errorf("column %s not found", idxMeta.ColumnName)
	}

	idxKey, err := ValueToKey(value, col.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to create index key: %w", err)
	}

	idxTree, err := t.getIndexTree(idxMeta)
	if err != nil {
		return nil, err
	}

	pkBytes, err := idxTree.Search(idxKey)
	if err != nil {
		return nil, fmt.Errorf("value not found in index: %w", err)
	}

	pk, err := storage.DecodeKey(pkBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode primary key from index: %w", err)
	}

	return t.Get(pk)
}

func (t *Table) Scan() ([]*Row, error) {

	entries, err := t.btree.Scan()
	if err != nil {
		return nil, fmt.Errorf("failed to scan table %s: %w", t.schema.Name, err)
	}

	rows := make([]*Row, 0, len(entries))
	for _, entry := range entries {
		row, err := DeserializeRow(entry.Value)
		if err != nil {

			fmt.Printf("Warning: failed to deserialize row in table %s: %v\n", t.schema.Name, err)
			continue
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (t *Table) ScanLimit(offset, limit int) ([]*Row, error) {

	entries, err := t.btree.Scan()
	if err != nil {
		return nil, fmt.Errorf("failed to scan table %s: %w", t.schema.Name, err)
	}

	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(entries) {
		return []*Row{}, nil
	}

	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}

	rows := make([]*Row, 0, end-start)
	for i := start; i < end; i++ {
		row, err := DeserializeRow(entries[i].Value)
		if err != nil {
			fmt.Printf("Warning: failed to deserialize row in table %s: %v\n", t.schema.Name, err)
			continue
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

func (t *Table) Exists(key storage.Key) (bool, error) {

	_, err := t.btree.Search(key)
	if err != nil {
		if err.Error() == "key not found" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *Table) BatchInsert(rows [][]interface{}) error {

	insertedCount := 0

	for i, values := range rows {
		if err := t.Insert(values); err != nil {
			return fmt.Errorf("batch insert failed at row %d (inserted %d rows): %w",
				i, insertedCount, err)
		}
		insertedCount++
	}

	return nil
}

func (t *Table) RangeByIndex(indexName string, startValue, endValue interface{}) ([]*Row, error) {

	indexes := t.Catalog.GetTableIndexes(t.schema.Name)
	var idxMeta *IndexMetadata
	for _, idx := range indexes {
		if idx.Name == indexName {
			idxMeta = idx
			break
		}
	}

	if idxMeta == nil {
		return nil, fmt.Errorf("index %s not found on table %s", indexName, t.schema.Name)
	}

	col := t.schema.GetColumn(idxMeta.ColumnName)
	if col == nil {
		return nil, fmt.Errorf("column %s not found", idxMeta.ColumnName)
	}

	startKey, err := ValueToKey(startValue, col.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to create start key: %w", err)
	}

	endKey, err := ValueToKey(endValue, col.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to create end key: %w", err)
	}

	idxTree, err := t.getIndexTree(idxMeta)
	if err != nil {
		return nil, err
	}

	entries, err := idxTree.RangeSearch(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %w", err)
	}

	rows := make([]*Row, 0, len(entries))
	for _, entry := range entries {
		pk, err := storage.DecodeKey(entry.Value)
		if err != nil {
			fmt.Printf("Warning: failed to decode PK from index: %v\n", err)
			continue
		}

		row, err := t.Get(pk)
		if err != nil {
			fmt.Printf("Warning: failed to get row by PK from index: %v\n", err)
			continue
		}

		rows = append(rows, row)
	}

	return rows, nil
}
