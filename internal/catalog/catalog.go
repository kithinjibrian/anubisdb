package catalog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

const SystemTableName = "anubis_schema"

type ColumnType string

const (
	TypeInt     ColumnType = "INT"
	TypeText    ColumnType = "TEXT"
	TypeFloat   ColumnType = "FLOAT"
	TypeBoolean ColumnType = "BOOLEAN"
)

type Column struct {
	Name       string
	Type       ColumnType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
}

type Schema struct {
	Name     string
	Columns  []Column
	RootPage uint32
}

func (s *Schema) ColumnCount() int {
	return len(s.Columns)
}

func (s *Schema) GetColumn(name string) (*Column, error) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column '%s' not found", name)
}

type IndexMetadata struct {
	Name       string
	TableName  string
	ColumnName string
	IsUnique   bool
	RootPage   uint32
}

type catalogEntry struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	TblName     string `json:"tbl_name"`
	RootPage    uint32 `json:"rootpage"`
	Columns     string `json:"columns,omitempty"`
	IndexColumn string `json:"index_column,omitempty"`
	IsUnique    bool   `json:"is_unique,omitempty"`
}

type Catalog struct {
	pager       *storage.Pager
	schemas     map[string]*Schema
	indexes     map[string]*IndexMetadata
	systemBTree *storage.BTree
	nextEntryID uint64
}

func NewCatalog(pager *storage.Pager) (*Catalog, error) {
	c := &Catalog{
		pager:       pager,
		schemas:     make(map[string]*Schema),
		indexes:     make(map[string]*IndexMetadata),
		nextEntryID: 1,
	}

	if err := c.loadSystemCatalog(); err != nil {

		if err := c.initializeSystemCatalog(); err != nil {
			return nil, fmt.Errorf("failed to initialize system catalog: %w", err)
		}
	}

	return c, nil
}

func (c *Catalog) initializeSystemCatalog() error {

	btree, err := storage.NewBTree(c.pager)
	if err != nil {
		return fmt.Errorf("failed to create system B-tree: %w", err)
	}
	c.systemBTree = btree

	systemSchema := &Schema{
		Name: SystemTableName,
		Columns: []Column{
			{Name: "type", Type: TypeText, NotNull: true},
			{Name: "name", Type: TypeText, NotNull: true},
			{Name: "tbl_name", Type: TypeText, NotNull: true},
			{Name: "rootpage", Type: TypeInt, NotNull: true},
			{Name: "columns", Type: TypeText},
			{Name: "index_column", Type: TypeText},
			{Name: "is_unique", Type: TypeBoolean},
		},
		RootPage: btree.GetRootPage(),
	}
	c.schemas[SystemTableName] = systemSchema

	columnsJSON, err := json.Marshal(systemSchema.Columns)
	if err != nil {
		return fmt.Errorf("failed to marshal system columns: %w", err)
	}

	entry := catalogEntry{
		Type:     "table",
		Name:     SystemTableName,
		TblName:  SystemTableName,
		RootPage: systemSchema.RootPage,
		Columns:  string(columnsJSON),
	}

	if err := c.insertEntry(entry); err != nil {
		return fmt.Errorf("failed to insert system catalog entry: %w", err)
	}

	return nil
}

func (c *Catalog) loadSystemCatalog() error {

	btree, err := storage.LoadBTree(c.pager, 0)
	if err != nil {
		return err
	}
	c.systemBTree = btree

	entries, err := c.systemBTree.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan system catalog: %w", err)
	}

	maxID := uint64(0)

	for _, btreeEntry := range entries {
		entry, err := c.deserializeEntry(btreeEntry.Value)
		if err != nil {
			fmt.Printf("Warning: skipping invalid catalog entry (key %d): %v\n",
				btreeEntry.Key, err)
			continue
		}

		switch entry.Type {
		case "table":
			if err := c.loadTableEntry(entry); err != nil {
				fmt.Printf("Warning: failed to load table '%s': %v\n", entry.Name, err)
			}
		case "index":
			if err := c.loadIndexEntry(entry); err != nil {
				fmt.Printf("Warning: failed to load index '%s': %v\n", entry.Name, err)
			}
		default:
			fmt.Printf("Warning: unknown entry type '%s' for '%s'\n", entry.Type, entry.Name)
		}

		if btreeEntry.Key > maxID {
			maxID = btreeEntry.Key
		}
	}

	c.nextEntryID = maxID + 1
	return nil
}

func (c *Catalog) loadTableEntry(entry *catalogEntry) error {
	var columns []Column
	if err := json.Unmarshal([]byte(entry.Columns), &columns); err != nil {
		return fmt.Errorf("failed to unmarshal columns: %w", err)
	}

	schema := &Schema{
		Name:     entry.Name,
		Columns:  columns,
		RootPage: entry.RootPage,
	}

	c.schemas[entry.Name] = schema
	return nil
}

func (c *Catalog) loadIndexEntry(entry *catalogEntry) error {
	index := &IndexMetadata{
		Name:       entry.Name,
		TableName:  entry.TblName,
		ColumnName: entry.IndexColumn,
		IsUnique:   entry.IsUnique,
		RootPage:   entry.RootPage,
	}

	c.indexes[entry.Name] = index
	return nil
}

func (c *Catalog) insertEntry(entry catalogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	key := c.nextEntryID
	if err := c.systemBTree.Insert(key, data); err != nil {
		return err
	}

	c.nextEntryID++
	return nil
}

func (c *Catalog) deserializeEntry(data []byte) (*catalogEntry, error) {
	var entry catalogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}

	if entry.Type == "" || entry.Name == "" || entry.TblName == "" {
		return nil, errors.New("missing required fields in catalog entry")
	}

	return &entry, nil
}

func (c *Catalog) CreateTable(tableName string, columns []Column) (*Schema, error) {

	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}
	if c.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' already exists", tableName)
	}
	if len(columns) == 0 {
		return nil, errors.New("table must have at least one column")
	}

	primaryKeyCount := 0
	for _, col := range columns {
		if col.PrimaryKey {
			primaryKeyCount++
		}
	}
	if primaryKeyCount > 1 {
		return nil, errors.New("table cannot have more than one primary key column")
	}

	btree, err := storage.NewBTree(c.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate B-tree: %w", err)
	}

	schema := &Schema{
		Name:     tableName,
		Columns:  columns,
		RootPage: btree.GetRootPage(),
	}

	columnsJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal columns: %w", err)
	}

	entry := catalogEntry{
		Type:     "table",
		Name:     tableName,
		TblName:  tableName,
		RootPage: schema.RootPage,
		Columns:  string(columnsJSON),
	}

	if err := c.insertEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	c.schemas[tableName] = schema

	var createdIndexes []*IndexMetadata
	for _, col := range columns {
		var indexName string
		var isUnique bool
		var shouldCreateIndex bool

		if col.PrimaryKey {
			indexName = fmt.Sprintf("pk_%s_%s", tableName, col.Name)
			isUnique = true
			shouldCreateIndex = true
		} else if col.Unique {
			indexName = fmt.Sprintf("unique_%s_%s", tableName, col.Name)
			isUnique = true
			shouldCreateIndex = true
		} else {
			continue
		}

		if shouldCreateIndex {

			index, err := c.createIndexInternal(indexName, tableName, col.Name, isUnique)
			if err != nil {

				c.rollbackTableCreation(tableName, createdIndexes)
				return nil, fmt.Errorf("failed to create auto-index '%s': %w", indexName, err)
			}
			createdIndexes = append(createdIndexes, index)
		}
	}

	return schema, nil
}

func isUniqueColumn(col Column) bool {
	return col.Unique
}

func (c *Catalog) createIndexInternal(indexName, tableName, columnName string, unique bool) (*IndexMetadata, error) {

	schema, err := c.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	btree, err := storage.NewBTree(c.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate index B-tree: %w", err)
	}

	index := &IndexMetadata{
		Name:       indexName,
		TableName:  tableName,
		ColumnName: columnName,
		IsUnique:   unique,
		RootPage:   btree.GetRootPage(),
	}

	if err := c.populateIndex(index, schema, btree); err != nil {
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	entry := catalogEntry{
		Type:        "index",
		Name:        indexName,
		TblName:     tableName,
		RootPage:    index.RootPage,
		IndexColumn: columnName,
		IsUnique:    unique,
	}

	if err := c.insertEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to persist index: %w", err)
	}

	c.indexes[indexName] = index
	return index, nil
}

func (c *Catalog) rollbackTableCreation(tableName string, indexes []*IndexMetadata) {

	for _, idx := range indexes {
		delete(c.indexes, idx.Name)
		c.deleteEntryByName("index", idx.Name)
	}

	delete(c.schemas, tableName)
	c.deleteEntryByName("table", tableName)
}

func (c *Catalog) GetSchema(tableName string) (*Schema, error) {
	schema, exists := c.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}
	return schema, nil
}

func (c *Catalog) TableExists(tableName string) bool {
	_, exists := c.schemas[tableName]
	return exists
}

func (c *Catalog) ListTables() []string {
	tables := make([]string, 0, len(c.schemas))
	for name := range c.schemas {
		if name != SystemTableName {
			tables = append(tables, name)
		}
	}
	return tables
}

func (c *Catalog) DropTable(tableName string) error {
	if tableName == SystemTableName {
		return errors.New("cannot drop system catalog")
	}
	if !c.TableExists(tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	for _, index := range c.GetIndexes(tableName) {
		if err := c.DropIndex(index.Name); err != nil {
			return fmt.Errorf("failed to drop index '%s': %w", index.Name, err)
		}
	}

	if err := c.deleteEntryByName("table", tableName); err != nil {
		return err
	}

	delete(c.schemas, tableName)
	return nil
}

func (c *Catalog) CreateIndex(indexName, tableName, columnName string, unique bool) (*IndexMetadata, error) {

	if indexName == "" {
		return nil, errors.New("index name cannot be empty")
	}
	if c.IndexExists(indexName) {
		return nil, fmt.Errorf("index '%s' already exists", indexName)
	}

	schema, err := c.GetSchema(tableName)
	if err != nil {
		return nil, err
	}

	column, err := schema.GetColumn(columnName)
	if err != nil {
		return nil, fmt.Errorf("column '%s' not found in table '%s'", columnName, tableName)
	}

	btree, err := storage.NewBTree(c.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate index B-tree: %w", err)
	}

	index := &IndexMetadata{
		Name:       indexName,
		TableName:  tableName,
		ColumnName: columnName,
		IsUnique:   unique,
		RootPage:   btree.GetRootPage(),
	}

	if err := c.populateIndex(index, schema, btree); err != nil {
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	entry := catalogEntry{
		Type:        "index",
		Name:        indexName,
		TblName:     tableName,
		RootPage:    index.RootPage,
		IndexColumn: columnName,
		IsUnique:    unique,
	}

	if err := c.insertEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to persist index: %w", err)
	}

	c.indexes[indexName] = index
	return index, nil
}

func (c *Catalog) populateIndex(index *IndexMetadata, schema *Schema, indexBTree *storage.BTree) error {

	tableBTree, err := storage.LoadBTree(c.pager, schema.RootPage)
	if err != nil {
		return fmt.Errorf("failed to load table B-tree: %w", err)
	}

	column, err := schema.GetColumn(index.ColumnName)
	if err != nil {
		return err
	}

	seenValues := make(map[string]bool)

	entries, err := tableBTree.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}

	for _, entry := range entries {

		row, err := DeserializeRow(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to deserialize row: %w", err)
		}

		columnValue, columnType, err := ExtractColumnValue(row, index.ColumnName)
		if err != nil {
			return fmt.Errorf("failed to extract column value: %w", err)
		}

		indexKey, err := ValueToKey(columnValue, columnType)
		if err != nil {
			return fmt.Errorf("failed to convert value to key: %w", err)
		}

		if index.IsUnique {
			keyStr := indexKey.String()
			if seenValues[keyStr] {
				return fmt.Errorf("duplicate value '%v' found for unique index on column '%s'",
					columnValue, index.ColumnName)
			}
			seenValues[keyStr] = true
		}

		indexValue := entry.Key.Encode()

		if err := indexBTree.Insert(indexKey, indexValue); err != nil {
			return fmt.Errorf("failed to insert into index: %w", err)
		}
	}

	return nil
}

func (c *Catalog) GetIndex(indexName string) (*IndexMetadata, error) {
	index, exists := c.indexes[indexName]
	if !exists {
		return nil, fmt.Errorf("index '%s' does not exist", indexName)
	}
	return index, nil
}

func (c *Catalog) GetIndexes(tableName string) []*IndexMetadata {
	var indexes []*IndexMetadata
	for _, idx := range c.indexes {
		if idx.TableName == tableName {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

func (c *Catalog) IndexExists(indexName string) bool {
	_, exists := c.indexes[indexName]
	return exists
}

func (c *Catalog) ListIndexes() []string {
	indexes := make([]string, 0, len(c.indexes))
	for name := range c.indexes {
		indexes = append(indexes, name)
	}
	return indexes
}

func (c *Catalog) DropIndex(indexName string) error {
	if !c.IndexExists(indexName) {
		return fmt.Errorf("index '%s' does not exist", indexName)
	}

	if err := c.deleteEntryByName("index", indexName); err != nil {
		return err
	}

	delete(c.indexes, indexName)
	return nil
}

func (c *Catalog) deleteEntryByName(entryType, name string) error {
	entries, err := c.systemBTree.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan catalog: %w", err)
	}

	for _, btreeEntry := range entries {
		entry, err := c.deserializeEntry(btreeEntry.Value)
		if err != nil {
			continue
		}

		if entry.Type == entryType && entry.Name == name {
			if err := c.systemBTree.Delete(btreeEntry.Key); err != nil {
				return fmt.Errorf("failed to delete catalog entry: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("%s '%s' not found in catalog", entryType, name)
}

func (c *Catalog) ValidateInsert(tableName string, valueCount int) error {
	schema, err := c.GetSchema(tableName)
	if err != nil {
		return err
	}

	if valueCount != schema.ColumnCount() {
		return fmt.Errorf("column count mismatch: got %d, expected %d",
			valueCount, schema.ColumnCount())
	}

	return nil
}

func (c *Catalog) PrintCatalog() error {
	fmt.Println("\n=== System Catalog ===")
	fmt.Printf("Next Entry ID: %d\n\n", c.nextEntryID)

	fmt.Println("Tables:")
	for name, schema := range c.schemas {
		fmt.Printf("  %s (root page: %d)\n", name, schema.RootPage)
		for _, col := range schema.Columns {
			flags := ""
			if col.PrimaryKey {
				flags += " PRIMARY KEY"
			}
			if col.Unique {
				flags += " UNIQUE"
			}
			if col.NotNull {
				flags += " NOT NULL"
			}
			fmt.Printf("    - %s %s%s\n", col.Name, col.Type, flags)
		}
	}

	if len(c.indexes) > 0 {
		fmt.Println("\nIndexes:")
		for name, idx := range c.indexes {
			uniqueStr := ""
			if idx.IsUnique {
				uniqueStr = " UNIQUE"
			}
			fmt.Printf("  %s%s ON %s(%s) (root page: %d)\n",
				name, uniqueStr, idx.TableName, idx.ColumnName, idx.RootPage)
		}
	}

	fmt.Println()
	return nil
}
