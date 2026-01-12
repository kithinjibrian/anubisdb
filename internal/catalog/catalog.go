package catalog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/storage"
)

const SystemCatalogTable = "anubis_catalog"

const (
	MaxCachedTables  = 100
	MaxCachedIndexes = 500
)

type ColumnType string

const (
	TypeInt     ColumnType = "INT"
	TypeText    ColumnType = "TEXT"
	TypeFloat   ColumnType = "FLOAT"
	TypeBoolean ColumnType = "BOOLEAN"
)

type Column struct {
	Name       string     `json:"name"`
	Type       ColumnType `json:"type"`
	PrimaryKey bool       `json:"primary_key"`
	NotNull    bool       `json:"not_null"`
	Unique     bool       `json:"unique"`
}

type Schema struct {
	Name     string   `json:"name"`
	Columns  []Column `json:"columns"`
	RootPage uint32   `json:"root_page"`
	Version  int      `json:"version"`
}

type IndexMetadata struct {
	Name       string `json:"name"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	Unique     bool   `json:"unique"`
	RootPage   uint32 `json:"root_page"`
}

type Catalog struct {
	pager *storage.Pager
	tree  *storage.BTree

	tableCache *lruCache
	indexCache *lruCache
}

type metadataEntry struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func NewCatalog(pager *storage.Pager) (*Catalog, error) {
	cat := &Catalog{
		pager:      pager,
		tableCache: newLRUCache(MaxCachedTables),
		indexCache: newLRUCache(MaxCachedIndexes),
	}

	if pager.GetNumPages() == 0 {
		return cat.initialize()
	}

	tree, err := storage.LoadBTree(pager, 1, false)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	cat.tree = tree

	if err := cat.verifyCatalog(); err != nil {
		return nil, fmt.Errorf("catalog verification failed: %w", err)
	}

	return cat, nil
}

func (c *Catalog) initialize() (*Catalog, error) {
	tree, err := storage.NewBTree(c.pager, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog tree: %w", err)
	}
	c.tree = tree

	catalogSchema := &Schema{
		Name: SystemCatalogTable,
		Columns: []Column{
			{Name: "entry_type", Type: TypeText, NotNull: true},
			{Name: "name", Type: TypeText, NotNull: true, PrimaryKey: true},
			{Name: "metadata", Type: TypeText, NotNull: true},
		},
		RootPage: tree.GetRootPage(),
		Version:  1,
	}

	c.tableCache.Put(SystemCatalogTable, catalogSchema)

	if err := c.saveTable(catalogSchema); err != nil {
		return nil, fmt.Errorf("failed to save catalog schema: %w", err)
	}

	return c, nil
}

func (c *Catalog) verifyCatalog() error {
	count, err := c.tree.Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errors.New("catalog is empty")
	}

	return nil
}

func (c *Catalog) loadTableFromDisk(name string) (*Schema, error) {
	key := stringToKey(name)

	value, err := c.tree.Search(key)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found in catalog", name)
	}

	var meta metadataEntry
	if err := json.Unmarshal(value, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	if meta.Type != "table" {
		return nil, fmt.Errorf("entry '%s' is not a table", name)
	}

	var table Schema
	if err := json.Unmarshal(meta.Data, &table); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table: %w", err)
	}

	if table.RootPage == 0 {
		return nil, fmt.Errorf("invalid root page (0) for table %s", table.Name)
	}
	if table.RootPage > c.pager.GetNumPages() {
		return nil, fmt.Errorf("root page %d out of range for table %s", table.RootPage, table.Name)
	}

	return &table, nil
}

func (c *Catalog) loadIndexFromDisk(name string) (*IndexMetadata, error) {
	key := stringToKey(name)

	value, err := c.tree.Search(key)
	if err != nil {
		return nil, fmt.Errorf("index '%s' not found in catalog", name)
	}

	var meta metadataEntry
	if err := json.Unmarshal(value, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	if meta.Type != "index" {
		return nil, fmt.Errorf("entry '%s' is not an index", name)
	}

	var index IndexMetadata
	if err := json.Unmarshal(meta.Data, &index); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index: %w", err)
	}

	if index.RootPage == 0 {
		return nil, fmt.Errorf("invalid root page (0) for index %s", index.Name)
	}
	if index.RootPage > c.pager.GetNumPages() {
		return nil, fmt.Errorf("root page %d out of range for index %s", index.RootPage, index.Name)
	}

	return &index, nil
}

func (c *Catalog) CreateTable(name string, columns []Column) (*Schema, error) {
	if name == "" {
		return nil, errors.New("table name cannot be empty")
	}
	if c.tableExistsUnsafe(name) {
		return nil, fmt.Errorf("table '%s' already exists", name)
	}
	if len(columns) == 0 {
		return nil, errors.New("table must have at least one column")
	}

	if err := validateColumns(columns); err != nil {
		return nil, err
	}

	tree, err := storage.NewBTree(c.pager, false)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate tree: %w", err)
	}

	schema := &Schema{
		Name:     name,
		Columns:  columns,
		RootPage: tree.GetRootPage(),
		Version:  1,
	}

	if err := c.saveTable(schema); err != nil {
		// TODO: Add pages to freelist when implemented
		return nil, err
	}

	c.tableCache.Put(name, schema)

	if err := c.createAutoIndexes(schema); err != nil {
		if deleteErr := c.deleteTableUnsafe(name); deleteErr != nil {
			fmt.Printf("Error: failed to rollback table creation: %v\n", deleteErr)
		}
		return nil, fmt.Errorf("failed to create auto indexes: %w", err)
	}

	return schema, nil
}

func validateColumns(columns []Column) error {
	pkCount := 0
	names := make(map[string]bool)

	for _, col := range columns {
		if col.Name == "" {
			return errors.New("column name cannot be empty")
		}

		if names[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		names[col.Name] = true

		if col.PrimaryKey {
			pkCount++
		}
	}

	if pkCount > 1 {
		return errors.New("table can have at most one primary key")
	}

	return nil
}

func (c *Catalog) saveTable(schema *Schema) error {
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal table: %w", err)
	}

	meta := metadataEntry{
		Type: "table",
		Data: data,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	key := stringToKey(schema.Name)

	if err := c.tree.Insert(key, metaBytes); err != nil {
		return fmt.Errorf("failed to insert table into catalog: %w", err)
	}

	return nil
}

func (c *Catalog) createAutoIndexes(schema *Schema) error {
	for _, col := range schema.Columns {
		var indexName string
		var unique bool

		if col.PrimaryKey {
			indexName = fmt.Sprintf("pk_%s_%s", schema.Name, col.Name)
			unique = true
		} else if col.Unique {
			indexName = fmt.Sprintf("uq_%s_%s", schema.Name, col.Name)
			unique = true
		} else {
			continue
		}

		if _, err := c.createIndexUnsafe(indexName, schema.Name, col.Name, unique); err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) CreateIndex(name, tableName, columnName string, unique bool) (*IndexMetadata, error) {
	return c.createIndexUnsafe(name, tableName, columnName, unique)
}

func (c *Catalog) createIndexUnsafe(name, tableName, columnName string, unique bool) (*IndexMetadata, error) {
	if name == "" {
		return nil, errors.New("index name cannot be empty")
	}
	if c.indexExistsUnsafe(name) {
		return nil, fmt.Errorf("index '%s' already exists", name)
	}

	table, err := c.getTableUnsafe(tableName)
	if err != nil {
		return nil, err
	}

	column := table.GetColumn(columnName)
	if column == nil {
		return nil, fmt.Errorf("column '%s' not found in table '%s'", columnName, tableName)
	}

	tree, err := storage.NewBTree(c.pager, true)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate index tree: %w", err)
	}

	index := &IndexMetadata{
		Name:       name,
		TableName:  tableName,
		ColumnName: columnName,
		Unique:     unique,
		RootPage:   tree.GetRootPage(),
	}

	if err := c.populateIndex(index, table, tree); err != nil {
		// TODO: Add pages to freelist when implemented
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	if err := c.saveIndex(index); err != nil {
		// TODO: Add pages to freelist when implemented
		return nil, err
	}

	c.indexCache.Put(name, index)
	return index, nil
}

func (c *Catalog) saveIndex(index *IndexMetadata) error {
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	meta := metadataEntry{
		Type: "index",
		Data: data,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	key := stringToKey(index.Name)
	if err := c.tree.Insert(key, metaBytes); err != nil {
		return fmt.Errorf("failed to insert index into catalog: %w", err)
	}

	return nil
}

func (c *Catalog) populateIndex(index *IndexMetadata, table *Schema, indexTree *storage.BTree) error {
	dataTree, err := storage.LoadBTree(c.pager, table.RootPage, false)
	if err != nil {
		return fmt.Errorf("failed to load table tree: %w", err)
	}

	entries, err := dataTree.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}

	for _, entry := range entries {
		row, err := DeserializeRow(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to deserialize row: %w", err)
		}

		colValue, colType, err := ExtractColumnValue(row, index.ColumnName)
		if err != nil {
			return fmt.Errorf("failed to extract column value: %w", err)
		}

		indexKey, err := ValueToKey(colValue, colType)
		if err != nil {
			return fmt.Errorf("failed to convert value to key: %w", err)
		}

		indexValue := entry.Key.Encode()

		if err := indexTree.Insert(indexKey, indexValue); err != nil {
			if index.Unique && err.Error() == "duplicate key" {
				return fmt.Errorf("duplicate value '%s' for unique index on column %s",
					indexKey.String(), index.ColumnName)
			}
			return fmt.Errorf("failed to insert into index: %w", err)
		}
	}

	return nil
}

func stringToKey(s string) storage.Key {
	return storage.NewTextKey(s)
}

func (c *Catalog) GetTable(name string) (*Schema, error) {
	return c.getTableUnsafe(name)
}

func (c *Catalog) getTableUnsafe(name string) (*Schema, error) {
	if cached, exists := c.tableCache.Get(name); exists {
		return cached.(*Schema), nil
	}

	table, err := c.loadTableFromDisk(name)
	if err != nil {
		return nil, err
	}

	c.tableCache.Put(name, table)

	return table, nil
}

func (c *Catalog) LoadTable(name string) (*Table, error) {
	schema, err := c.getTableUnsafe(name)

	if err != nil {
		return nil, err
	}

	btree, err := storage.LoadBTree(c.pager, schema.RootPage, false)
	if err != nil {
		return nil, fmt.Errorf("failed to load table B-tree: %w", err)
	}

	return &Table{
		Catalog: c,
		schema:  schema,
		btree:   btree,
	}, nil
}

func (c *Catalog) GetIndex(name string) (*IndexMetadata, error) {

	return c.getIndexUnsafe(name)
}

func (c *Catalog) getIndexUnsafe(name string) (*IndexMetadata, error) {
	if cached, exists := c.indexCache.Get(name); exists {
		return cached.(*IndexMetadata), nil
	}

	index, err := c.loadIndexFromDisk(name)
	if err != nil {
		return nil, err
	}

	c.indexCache.Put(name, index)

	return index, nil
}

func (c *Catalog) TableExists(name string) bool {

	return c.tableExistsUnsafe(name)
}

func (c *Catalog) tableExistsUnsafe(name string) bool {
	if _, exists := c.tableCache.Get(name); exists {
		return true
	}

	_, err := c.loadTableFromDisk(name)
	return err == nil
}

func (c *Catalog) IndexExists(name string) bool {

	return c.indexExistsUnsafe(name)
}

func (c *Catalog) indexExistsUnsafe(name string) bool {
	if _, exists := c.indexCache.Get(name); exists {
		return true
	}

	_, err := c.loadIndexFromDisk(name)
	return err == nil
}

func (c *Catalog) ListTables() []string {

	entries, err := c.tree.Scan()
	if err != nil {
		return []string{}
	}

	tables := make([]string, 0)
	for _, entry := range entries {
		var meta metadataEntry
		if err := json.Unmarshal(entry.Value, &meta); err != nil {
			continue
		}

		if meta.Type != "table" {
			continue
		}

		var table Schema
		if err := json.Unmarshal(meta.Data, &table); err != nil {
			continue
		}

		if table.Name != SystemCatalogTable {
			tables = append(tables, table.Name)
		}
	}

	return tables
}

func (c *Catalog) ListIndexes() []string {
	entries, err := c.tree.Scan()
	if err != nil {
		return []string{}
	}

	indexes := make([]string, 0)
	for _, entry := range entries {
		var meta metadataEntry
		if err := json.Unmarshal(entry.Value, &meta); err != nil {
			continue
		}

		if meta.Type != "index" {
			continue
		}

		var index IndexMetadata
		if err := json.Unmarshal(meta.Data, &index); err != nil {
			continue
		}

		indexes = append(indexes, index.Name)
	}

	return indexes
}

func (c *Catalog) GetTableIndexes(tableName string) []*IndexMetadata {

	entries, err := c.tree.Scan()
	if err != nil {
		return []*IndexMetadata{}
	}

	result := make([]*IndexMetadata, 0)
	for _, entry := range entries {
		var meta metadataEntry
		if err := json.Unmarshal(entry.Value, &meta); err != nil {
			continue
		}

		if meta.Type != "index" {
			continue
		}

		var index IndexMetadata
		if err := json.Unmarshal(meta.Data, &index); err != nil {
			continue
		}

		if index.TableName == tableName {
			result = append(result, &index)
		}
	}

	return result
}

func (c *Catalog) DropTable(name string) error {

	if name == SystemCatalogTable {
		return errors.New("cannot drop system catalog")
	}
	if !c.tableExistsUnsafe(name) {
		return fmt.Errorf("table '%s' does not exist", name)
	}

	indexes := c.GetTableIndexes(name)
	for _, idx := range indexes {
		if err := c.dropIndexUnsafe(idx.Name); err != nil {
			return fmt.Errorf("failed to drop index '%s': %w", idx.Name, err)
		}
	}

	// TODO: Free all pages in the table's B-tree when freelist is implemented

	key := stringToKey(name)
	if err := c.tree.Delete(key); err != nil {
		return fmt.Errorf("failed to delete table metadata: %w", err)
	}

	c.tableCache.Delete(name)
	return nil
}

func (c *Catalog) DropIndex(name string) error {

	return c.dropIndexUnsafe(name)
}

func (c *Catalog) dropIndexUnsafe(name string) error {
	if !c.indexExistsUnsafe(name) {
		return fmt.Errorf("index '%s' does not exist", name)
	}

	// TODO: Free all pages in the index's B-tree when freelist is implemented

	key := stringToKey(name)
	if err := c.tree.Delete(key); err != nil {
		return fmt.Errorf("failed to delete index metadata: %w", err)
	}

	c.indexCache.Delete(name)
	return nil
}

func (c *Catalog) deleteTableUnsafe(name string) error {
	key := stringToKey(name)
	if err := c.tree.Delete(key); err != nil {
		return fmt.Errorf("failed to delete table from catalog: %w", err)
	}
	c.tableCache.Delete(name)
	return nil
}

func (t *Schema) GetColumn(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

func (t *Schema) GetColumnIndex(name string) int {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

func (t *Schema) ColumnCount() int {
	return len(t.Columns)
}

func (c *Catalog) Print() {
	fmt.Println("\n=== Database Catalog ===")

	tables := c.ListTables()
	fmt.Printf("\nTables (%d):\n", len(tables))
	for _, name := range tables {
		table, err := c.getTableUnsafe(name)
		if err != nil {
			continue
		}
		fmt.Printf("  %s (page %d, version %d)\n", name, table.RootPage, table.Version)
		for _, col := range table.Columns {
			flags := ""
			if col.PrimaryKey {
				flags += " PK"
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

	indexes := c.ListIndexes()
	if len(indexes) > 0 {
		fmt.Printf("\nIndexes (%d):\n", len(indexes))
		for _, name := range indexes {
			idx, err := c.getIndexUnsafe(name)
			if err != nil {
				continue
			}
			uniqueFlag := ""
			if idx.Unique {
				uniqueFlag = " [UNIQUE]"
			}
			fmt.Printf("  %s%s ON %s.%s (page %d)\n",
				name, uniqueFlag, idx.TableName, idx.ColumnName, idx.RootPage)
		}
	}

	fmt.Println()
}

func (c *Catalog) LoadIndexTree(indexName string) (*storage.BTree, error) {
	index, err := c.getIndexUnsafe(indexName)

	if err != nil {
		return nil, err
	}

	tree, err := storage.LoadBTree(c.pager, index.RootPage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to load index B-tree: %w", err)
	}

	return tree, nil
}
