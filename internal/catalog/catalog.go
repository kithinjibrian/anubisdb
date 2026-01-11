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
}

type CatalogEntry struct {
	Type     string
	Name     string
	TblName  string
	RootPage uint32
	Columns  []Column
}

type Catalog struct {
	pager       *storage.Pager
	schemas     map[string]*Schema
	systemBTree *storage.BTree
	nextEntryID uint64
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

func NewCatalog(pager *storage.Pager) (*Catalog, error) {
	c := &Catalog{
		pager:       pager,
		schemas:     make(map[string]*Schema),
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
		return err
	}

	c.systemBTree = btree

	systemSchema := &Schema{
		Name: SystemTableName,
		Columns: []Column{
			{Name: "type", Type: TypeText},
			{Name: "name", Type: TypeText},
			{Name: "tbl_name", Type: TypeText},
			{Name: "rootpage", Type: TypeInt},
			{Name: "columns", Type: TypeText},
		},
		RootPage: btree.GetRootPage(),
	}

	c.schemas[SystemTableName] = systemSchema

	entry := CatalogEntry{
		Type:     "table",
		Name:     SystemTableName,
		TblName:  SystemTableName,
		RootPage: systemSchema.RootPage,
		Columns:  systemSchema.Columns,
	}

	if err := c.insertCatalogEntry(entry); err != nil {
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

	entries, err := c.scanCatalogEntries()
	if err != nil {
		return err
	}

	maxID := uint64(0)

	for _, entry := range entries {
		if entry.Type == "table" {
			schema := &Schema{
				Name:     entry.Name,
				Columns:  entry.Columns,
				RootPage: entry.RootPage,
			}
			c.schemas[entry.Name] = schema
		}
	}

	c.nextEntryID = maxID + 1
	if c.nextEntryID < uint64(len(entries)+1) {
		c.nextEntryID = uint64(len(entries) + 1)
	}

	return nil
}

func (c *Catalog) scanCatalogEntries() ([]CatalogEntry, error) {
	var entries []CatalogEntry

	btreeEntries, err := c.systemBTree.Scan()
	if err != nil {
		return nil, fmt.Errorf("failed to scan system catalog: %w", err)
	}

	for _, btreeEntry := range btreeEntries {
		catalogEntry, err := c.deserializeCatalogEntry(btreeEntry.Value)

		if err != nil {

			fmt.Printf("Warning: failed to deserialize catalog entry (key %d): %v\n",
				btreeEntry.Key, err)
			continue
		}

		entries = append(entries, *catalogEntry)
	}

	return entries, nil
}

func (c *Catalog) CreateTable(tableName string, columns []Column) (*Schema, error) {
	if c.TableExists(tableName) {
		return nil, errors.New("table already exists")
	}

	if len(columns) == 0 {
		return nil, errors.New("table must have at least one column")
	}

	btree, err := storage.NewBTree(c.pager)
	if err != nil {
		return nil, err
	}

	schema := &Schema{
		Name:     tableName,
		Columns:  columns,
		RootPage: btree.GetRootPage(),
	}

	c.schemas[tableName] = schema

	entry := CatalogEntry{
		Type:     "table",
		Name:     tableName,
		TblName:  tableName,
		RootPage: schema.RootPage,
		Columns:  columns,
	}

	if err := c.insertCatalogEntry(entry); err != nil {
		delete(c.schemas, tableName)
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return schema, nil
}

func (c *Catalog) insertCatalogEntry(entry CatalogEntry) error {

	value, err := c.serializeCatalogEntry(entry)
	if err != nil {
		return err
	}

	key := c.nextEntryID
	c.nextEntryID++

	if err := c.systemBTree.Insert(key, value); err != nil {
		return err
	}

	return nil
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
		return errors.New("table does not exist")
	}

	delete(c.schemas, tableName)

	return nil
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

func (c *Catalog) serializeCatalogEntry(entry CatalogEntry) ([]byte, error) {
	data := make(map[string]interface{})
	data["type"] = entry.Type
	data["name"] = entry.Name
	data["tbl_name"] = entry.TblName
	data["rootpage"] = entry.RootPage

	columnsJSON, err := json.Marshal(entry.Columns)
	if err != nil {
		return nil, err
	}
	data["columns"] = string(columnsJSON)

	return json.Marshal(data)
}

func (c *Catalog) deserializeCatalogEntry(value []byte) (*CatalogEntry, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(value, &data); err != nil {
		return nil, err
	}

	entry := &CatalogEntry{
		Type:     data["type"].(string),
		Name:     data["name"].(string),
		TblName:  data["tbl_name"].(string),
		RootPage: uint32(data["rootpage"].(float64)),
	}

	columnsJSON := data["columns"].(string)
	if err := json.Unmarshal([]byte(columnsJSON), &entry.Columns); err != nil {
		return nil, err
	}

	return entry, nil
}

func (c *Catalog) PrintCatalog() error {
	fmt.Println("=== System Catalog ===")

	entries, err := c.scanCatalogEntries()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		fmt.Printf("Type: %s, Name: %s, RootPage: %d, Columns: %d\n",
			entry.Type, entry.Name, entry.RootPage, len(entry.Columns))

		for _, col := range entry.Columns {
			fmt.Printf("  - %s (%s)\n", col.Name, col.Type)
		}
	}

	return nil
}
