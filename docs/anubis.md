# AnubisDB Documentation

## 1. Introduction

### What is AnubisDB?

AnubisDB is a lightweight, educational relational database built from scratch in Go. It implements the fundamentals: B+ trees, a catalog system, indexing, and a query engine.

### Key Features

**What AnubisDB does well:**

- Full B+ tree implementation with automatic splitting and rebalancing
- Intelligent index usage for fast queries
- Support for multiple data types (integers, text, floats, booleans)
- Automatic indexing for primary keys and unique constraints
- Concurrent read access (with some caveats)
- All data persisted to a single file

**What AnubisDB doesn't do (yet):**

- Transactions (everything happens immediately, for better or worse)
- Network protocol

### Quick Start

```go
package main

import (
    "fmt"
    "github.com/kithinjibrian/anubisdb/internal/catalog"
    "github.com/kithinjibrian/anubisdb/internal/storage"
)

func main() {
    // Create a new database file
    pager, _ := storage.NewPager("anubis.db")
    defer pager.Close()

    // Initialize catalog
    cat, _ := catalog.NewCatalog(pager)

    // Create a table
    cat.CreateTable("users", []catalog.Column{
        {Name: "id", Type: catalog.TypeInt, PrimaryKey: true},
        {Name: "name", Type: catalog.TypeText, NotNull: true},
        {Name: "age", Type: catalog.TypeInt},
    })

    // Load and use the table
    table, _ := cat.LoadTable("users")
    table.Insert([]interface{}{int64(1), "Alice", int64(25)})

    // Query it
    rows, _ := table.Scan()
    fmt.Printf("Found %d rows\n", len(rows))
}
```

---

## 2. Architecture

### System Layers

AnubisDB is built in three main layers. Each layer has its own responsibility, and they work together to give us a functioning database.

**Storage Layer**

- Handles raw file I/O
- Manages pages (4KB chunks of data)
- Implements B+ trees for sorted storage
- Deals with keys and how they're encoded

**Catalog Layer**

- Manages database schema (tables, columns, indexes)
- Handles metadata (what tables exist, what columns they have)
- Provides the Table API
- Caches frequently accessed metadata

**Query Engine**

- Parses SQL statements
- Plans out the best way to do it (index vs table scan)
- Executes queries
- Returns formatted results

### Data Flow

Here's what happens when we insert a row:

1. **Engine** receives our INSERT statement
2. **Catalog** validates it against the schema
3. **Table** calls the storage layer to insert the row
4. **B+ Tree** finds the right place to put it
5. **Pager** writes the page to disk
6. **Indexes** get updated automatically

For queries, it's similar but in reverse, with an optimization step where the engine decides if it can use an index to speed things up.

---

## 3. File Format

### Database File Structure

Our entire database lives in a single file. It's just a bunch of pages stacked together.

**The very first page** (page 0) is special. It contains:

- Magic number: `AnubisDB` in bytes (so we know it's our file)
- Version: Currently 1
- Reserved space: For future features we haven't thought of yet

**All other pages** (pages 1+) store our actual data.

### Page Types

Each page is 4096 bytes (4KB) and has a type that tells us what it contains:

- **Interior Table (0x02)**: Internal B+ tree nodes for table data (contain pointers to child pages)
- **Leaf Table (0x05)**: The actual rows of our table (this is where our data lives)
- **Interior Index (0x0A)**: Internal B+ tree nodes for indexes
- **Leaf Index (0x0D)**: Index entries that point back to table rows

### Page Layout

Every page follows the same basic structure:

```
+------------------+
| Page Header      | (16-20 bytes depending on type)
+------------------+
| Cell Pointer     | (2 bytes per cell)
| Array            | (grows downward)
+------------------+
|                  |
| Free Space       | (the good stuff)
|                  |
+------------------+
| Cell Content     | (grows upward)
| Area             |
+------------------+
```

**Page Header** contains:

- `PageType`: What kind of page this is
- `NumCells`: How many cells (entries) are in this page
- `CellContentOffset`: Where the cell content area starts
- `FragmentedBytes`: How much space is wasted (happens after deletions)
- `RightmostPointer`: For interior pages, points to the rightmost child
- `NextLeaf/PrevLeaf`: For leaf pages, forms a linked list (makes scans fast)

**Cell Pointer Array** is just an array of 2-byte offsets. Each offset points to where the actual cell data is stored. The array grows downward from the header.

**Free Space** is the space between the pointer array and the cell content. When this runs out, we need to split the page.

**Cell Content Area** stores the actual cell data. It grows upward from the bottom of the page.

### Cell Format

Cells are the actual data entries. Their format depends on whether it's a leaf or interior cell.

**Leaf cells** (the ones with actual data):

```
+------------------+
| Key Length (4B)  |
+------------------+
| Key Data         | (variable)
+------------------+
| Value Length (4B)|
+------------------+
| Value Data       | (variable)
+------------------+
```

**Interior cells** (just pointers):

```
+------------------+
| Key Length (4B)  |
+------------------+
| Key Data         | (variable)
+------------------+
| Child Page (4B)  | (points to a page number)
+------------------+
```

### Keys

Keys can be one of four types:

**IntKey** (9 bytes total):

- 1 byte: type tag (0x01)
- 8 bytes: int64 value (big-endian)

**TextKey** (5+ bytes):

- 1 byte: type tag (0x02)
- 4 bytes: length
- N bytes: UTF-8 string data

**FloatKey** (9 bytes total):

- 1 byte: type tag (0x03)
- 8 bytes: float64 bits (big-endian)

**BooleanKey** (2 bytes total):

- 1 byte: type tag (0x04)
- 1 byte: value (0 or 1)

---

## 4. Core Components

### Storage Layer

This is where pages get written to disk.

#### Pager

The Pager is the interface to the raw database file.

**Key responsibilities:**

- Allocate new pages when we need them
- Read pages from disk when requested
- Write modified pages back to disk
- Keep track of how many pages exist
- Protect the sacred page 0 (the header)

**Important methods:**

```go
// Create a new pager for a database file
pager, err := storage.NewPager("anubis.db")

// Read a page (returns error if page doesn't exist)
page, err := pager.ReadPage(pageNum)

// Write a page back to disk
err := pager.WritePage(pageNum, page)

// Allocate a new page
pageNum, page, err := pager.AllocatePage(PageTypeLeafTable, parentPage)

// Clean up when done
pager.Close()
```

#### B+ Tree

The B+ tree is the heart of the storage system. It keeps everything sorted and makes searches fast.

**Why B+ trees?**

- All data lives in leaf nodes (makes scans easy)
- Internal nodes only have keys (saves space)
- Leaf nodes are linked together (makes range queries fast)
- Automatic balancing
- Disk-friendly

**Key operations:**

**Insert:**

```go
tree, _ := storage.NewBTree(pager, false) // false = not an index
key := storage.NewIntKey(42)
value := []byte("some data")
err := tree.Insert(key, value)
```

If the page is full, it automatically splits and propagates changes up the tree.

**Search:**

```go
value, err := tree.Search(key)
if err != nil {
    // Key not found
}
```

This is O(log n) because we're navigating down the tree, not scanning everything.

**Range Search:**

```go
startKey := storage.NewIntKey(10)
endKey := storage.NewIntKey(100)
entries, err := tree.RangeSearch(startKey, endKey)
```

This finds the starting point via tree navigation, then follows the leaf node linked list. Much faster than scanning the whole tree.

**Update:**

```go
newValue := []byte("updated data")
err := tree.Update(key, newValue)
```

This deletes the old value and inserts the new one. If the new value is bigger and doesn't fit, it might trigger a split.

**Delete:**

```go
err := tree.Delete(key)
```

This removes the cell and marks space as fragmented. The page might defragment itself if fragmentation gets too high. Note: we don't currently handle underflow (merging sparse pages), so deleted data leaves gaps. It's on the TODO list.

**Scan:**

```go
entries, err := tree.Scan()
for _, entry := range entries {
    fmt.Printf("Key: %v, Value: %v\n", entry.Key, entry.Value)
}
```

This walks the entire tree in sorted order by following the leaf linked list. It's O(n) but at least the data is sorted.

#### Keys

Keys implement a common interface:

```go
type Key interface {
    Compare(other Key) int  // -1, 0, or 1
    Encode() []byte         // Serialize to bytes
    Type() KeyType          // What type is this?
    String() string         // Human-readable representation
}
```

**Comparison rules:**

- Same type: compare values naturally
- Different types: compare type tags (Int < Text < Float < Boolean)
- This means we really shouldn't mix types in one tree

**Creating keys:**

```go
intKey := storage.NewIntKey(42)
textKey := storage.NewTextKey("hello")
floatKey := storage.NewFloatKey(3.14)
boolKey := storage.NewBooleanKey(true)
```

**Decoding keys:**

```go
keyBytes := someKey.Encode()
decodedKey, err := storage.DecodeKey(keyBytes)
```

### Catalog System

The catalog is like the database's brain. It remembers what tables exist, what columns they have, and where everything is stored.

#### Schema Management

**Tables** are defined by a schema:

```go
type Schema struct {
    Name     string
    Columns  []Column
    RootPage uint32  // Where the B+ tree starts
    Version  int
}
```

**Columns** have properties:

```go
type Column struct {
    Name       string
    Type       ColumnType  // INT, TEXT, FLOAT, BOOLEAN
    PrimaryKey bool
    NotNull    bool
    Unique     bool
}
```

**Creating a table:**

```go
schema, err := catalog.CreateTable("users", []catalog.Column{
    {Name: "id", Type: catalog.TypeInt, PrimaryKey: true},
    {Name: "name", Type: catalog.TypeText, NotNull: true},
    {Name: "email", Type: catalog.TypeText, Unique: true},
    {Name: "age", Type: catalog.TypeInt},
})
```

When we create a table:

1. A new B+ tree is allocated for the table data
2. The schema is saved to the system catalog
3. Indexes are automatically created for PRIMARY KEY and UNIQUE columns
4. Everything is cached in memory

**The system catalog** is just a special table (called "anubis_catalog") that stores metadata about all our tables and indexes. It's meta like that.

#### Caching

The catalog uses an LRU (Least Recently Used) cache to avoid constantly reading metadata from disk:

- Up to 100 table schemas cached
- Up to 500 index metadata entries cached
- Old entries evicted when cache fills up
- Cache is checked before hitting disk

This means our frequently-used tables stay in memory, but we won't run out of RAM if we have thousands of tables.

#### Indexes

Indexes are created automatically for:

- Primary key columns (named like `pk_tablename_columnname`)
- Unique columns (named like `uq_tablename_columnname`)

We can also create them manually:

```go
index, err := catalog.CreateIndex(
    "idx_users_age",  // index name
    "users",          // table name
    "age",            // column name
    false             // unique?
)
```

**Index structure:**

- Indexes are stored in their own B+ trees (separate from table data)
- Index keys are the column values
- Index values are the primary keys (so we can look up the full row)
- Unique indexes reject duplicate values

**Example:** If we have a table with columns `(id, name, email)` where `id` is the primary key and `email` is unique:

- Main table B+ tree: keyed by `id`, stores full rows
- Email index B+ tree: keyed by `email`, stores `id` values

To find a row by email:

1. Look up email in the index → get the `id`
2. Look up `id` in main table → get the full row

Two lookups, but both are O(log n), so still fast.

### Query Engine

#### Supported Queries

**CREATE TABLE:**

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INT
);
```

**INSERT:**

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25);
```

**SELECT:**

```sql
SELECT * FROM users WHERE id = 1;
SELECT name, email FROM users WHERE age >= 18;
```

**UPDATE:**

```sql
UPDATE users SET age = 26 WHERE id = 1;
UPDATE users SET name = 'Bob' WHERE age < 18;
```

**DELETE:**

```sql
DELETE FROM users WHERE id = 1;
DELETE FROM users WHERE age < 13;
```

#### Index Optimization

This is where things get smart. The engine tries to use indexes whenever possible:

**Primary key equality** (fastest):

```sql
SELECT * FROM users WHERE id = 1;
```

→ Direct B+ tree lookup using the primary key. O(log n).

**Secondary index equality**:

```sql
SELECT * FROM users WHERE email = 'alice@example.com';
```

→ If there's an index on email:

1. Look up email in index → get id
2. Look up id in main table → get row
   Two O(log n) operations, still fast.

**Range queries with indexes**:

```sql
SELECT * FROM users WHERE age >= 18 AND age <= 65;
```

→ If there's an index on age:

1. Find starting point (age=18) in index
2. Scan index until age=65
3. For each index entry, fetch the full row
   Much faster than scanning the whole table.

**No index available**:

```sql
SELECT * FROM users WHERE name = 'Alice';
```

→ If there's no index on name:

1. Scan entire table
2. Check each row's name
3. Return matches
   This is O(n). Not great, but it works.

The engine automatically picks the best strategy based on what indexes are available.

#### Filter Evaluation

Filters (WHERE clauses) are evaluated with proper type handling:

**Type conversions:**

- Strings to numbers: `"42"` → `42`
- Booleans: accepts `true`, `false`, `1`, `0`, `yes`, `no`, `t`, `f`
- NULLs: follow SQL semantics (NULL != NULL)

**Operators:**

- Equality: `=`
- Inequality: `!=`, `<>`
- Comparison: `<`, `<=`, `>`, `>=`
- Floats use epsilon comparison for `=` (because 0.1 + 0.2 != 0.3 in binary)

**Multiple conditions:**

- All conditions must match (AND logic)
- OR and NOT are not supported yet (sorry)

Example:

```sql
WHERE age >= 18 AND age <= 65
```

Both conditions must be true for a row to match.

---

## 5. Usage Guide

### Creating Tables

Tables are defined with a schema specifying columns and their types.

```go
catalog, _ := catalog.NewCatalog(pager)

schema, err := catalog.CreateTable("products", []catalog.Column{
    {
        Name:       "id",
        Type:       catalog.TypeInt,
        PrimaryKey: true,
        NotNull:    true,  // PK is automatically NOT NULL
    },
    {
        Name:    "name",
        Type:    catalog.TypeText,
        NotNull: true,
    },
    {
        Name:   "price",
        Type:   catalog.TypeFloat,
    },
    {
        Name:   "in_stock",
        Type:   catalog.TypeBoolean,
    },
})
```

**What happens behind the scenes:**

1. Schema is validated (no duplicate columns, at most one PK)
2. New B+ tree allocated for table data
3. Primary key index created automatically
4. Schema saved to system catalog
5. Schema cached in memory

**Constraints:**

- Each table must have at least one column
- Column names must be unique within a table
- At most one primary key per table
- Primary key columns are automatically NOT NULL

### Inserting Data

```go
table, _ := catalog.LoadTable("products")

// Values must match the column order in the schema
err := table.Insert([]interface{}{
    int64(1),           // id (must be int64, not int)
    "Widget",           // name
    float64(19.99),     // price
    true,               // in_stock
})
```

**Important notes:**

- Values must be in the same order as columns in the schema
- Use `int64` for integers, not `int` (Go quirk)
- Use `float64` for floats
- String for text, bool for boolean
- Pass `nil` for NULL values (if column allows it)

**What happens:**

1. Values validated against schema
2. Primary key extracted
3. Row inserted into main B+ tree
4. All indexes updated
5. If any index constraint fails, everything rolls back

**Common errors:**

- Wrong number of values
- Type mismatch (e.g., passing string for int column)
- NULL for NOT NULL column
- Duplicate primary key
- Duplicate unique value

### Querying Data

#### Get by Primary Key

Fastest way to retrieve a single row:

```go
key := storage.NewIntKey(1)
row, err := table.Get(key)
if err != nil {
    // Row not found
}

// Access values
idValue := row.Values["id"].Value.(int64)
nameValue := row.Values["name"].Value.(string)
```

#### Get by Index

If we have an index on a column:

```go
row, err := table.GetByIndex("uq_products_name", "Widget")
if err != nil {
    // Not found
}
```

#### Range Queries

```go
rows, err := table.RangeByIndex(
    "idx_products_price",
    float64(10.0),   // start
    float64(50.0),   // end
)

for _, row := range rows {
    fmt.Printf("Product: %s, Price: %.2f\n",
        row.Values["name"].Value,
        row.Values["price"].Value)
}
```

#### Full Table Scan

When we need all rows or don't have an index:

```go
rows, err := table.Scan()
for _, row := range rows {
    // Process each row
}
```

**Pagination** (for large tables):

```go
// Get rows 10-19 (offset=10, limit=10)
rows, err := table.ScanLimit(10, 10)
```

### Updating Data

Updates require the primary key:

```go
key := storage.NewIntKey(1)

// New values in schema column order
newValues := []interface{}{
    int64(1),        // id (must not change)
    "Super Widget",  // name
    float64(29.99),  // price
    false,           // in_stock
}

err := table.Update(key, newValues)
```

**What happens:**

1. Old row fetched (to get old index values)
2. New values validated
3. Primary key verified (can't change)
4. Indexes updated (old entries deleted, new ones inserted)
5. Main table updated

**Restrictions:**

- Cannot change primary key (will return error)
- Must provide values for all columns
- Must satisfy all constraints

### Deleting Data

```go
key := storage.NewIntKey(1)
err := table.Delete(key)
```

**What happens:**

1. Row fetched (to get index values)
2. All index entries deleted
3. Main table entry deleted

After deletion, the space becomes fragmented. If fragmentation gets too high (>64 bytes), the page automatically defragments itself.

### Indexes

#### Automatic Indexes

Created automatically for:

- Primary key columns
- Unique columns

```go
catalog.CreateTable("users", []catalog.Column{
    {Name: "id", Type: catalog.TypeInt, PrimaryKey: true},
    // → Creates index "pk_users_id"

    {Name: "email", Type: catalog.TypeText, Unique: true},
    // → Creates index "uq_users_email"
})
```

#### Manual Indexes

```go
index, err := catalog.CreateIndex(
    "idx_users_age",  // name
    "users",          // table
    "age",            // column
    false,            // unique?
)
```

**When to create indexes:**

- Columns frequently used in WHERE clauses
- Columns used for sorting (when ORDER BY is implemented)
- Columns with high selectivity (many distinct values)

**When NOT to create indexes:**

- Columns rarely queried
- Columns with few distinct values (e.g., boolean columns)
- Small tables (overhead not worth it)
- Tables with heavy writes (indexes slow down INSERT/UPDATE/DELETE)

#### Listing Indexes

```go
// All indexes in database
indexes := catalog.ListIndexes()

// Indexes for a specific table
tableIndexes := catalog.GetTableIndexes("users")
```

### Data Types

#### INT / INTEGER

Stored as `int64` (64-bit signed integer).

Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

```go
{Name: "age", Type: catalog.TypeInt}

// Insert
table.Insert([]interface{}{int64(25)})  // Note: int64, not int

// Read
age := row.Values["age"].Value.(int64)
```

#### TEXT / VARCHAR / STRING

Stored as UTF-8 strings.

```go
{Name: "name", Type: catalog.TypeText}

// Insert
table.Insert([]interface{}{"Alice"})

// Read
name := row.Values["name"].Value.(string)
```

#### FLOAT / REAL / DOUBLE

Stored as `float64` (64-bit floating point).

```go
{Name: "price", Type: catalog.TypeFloat}

// Insert
table.Insert([]interface{}{float64(19.99)})

// Read
price := row.Values["price"].Value.(float64)
```

**Note:** Floating point equality comparisons use an epsilon (0.0000001) to handle precision issues.

#### BOOLEAN / BOOL

Stored as `bool`.

```go
{Name: "active", Type: catalog.TypeBoolean}

// Insert
table.Insert([]interface{}{true})

// Read
active := row.Values["active"].Value.(bool)
```

### Constraints

#### PRIMARY KEY

- At most one per table
- Automatically creates an index
- Automatically NOT NULL
- Values must be unique

```go
{Name: "id", Type: catalog.TypeInt, PrimaryKey: true}
```

#### NOT NULL

- Column must have a value
- Cannot insert NULL

```go
{Name: "name", Type: catalog.TypeText, NotNull: true}
```

#### UNIQUE

- Values must be unique across all rows
- Automatically creates an index
- NULL values are allowed (and can appear multiple times)

```go
{Name: "email", Type: catalog.TypeText, Unique: true}
```

### NULL Handling

NULL values are supported (unless column is NOT NULL).

**Inserting NULL:**

```go
table.Insert([]interface{}{
    int64(1),
    "Alice",
    nil,  // price is NULL
})
```

**SQL semantics:**

- NULL != NULL (comparing NULLs returns false)
- NULL in WHERE clauses is never matched
- NULL displays as "NULL" in query results

---

## 6. Performance & Limitations

### Performance Characteristics

Understanding when things are fast and when they're slow helps us write better queries.

#### Index Lookups: O(log n)

When we query by primary key or an indexed column with equality:

```go
// Fast: uses primary key index
table.Get(storage.NewIntKey(42))

// Fast: uses email index
table.GetByIndex("uq_users_email", "alice@example.com")
```

For a table with 1,000,000 rows:

- Index lookup: ~20 comparisons (log₂(1,000,000))
- Takes microseconds

#### Full Scans: O(n)

When there's no applicable index:

```go
// Slow: scans entire table
rows, _ := table.Scan()

// Also slow: no index on 'name'
SELECT * FROM users WHERE name = 'Alice';
```

For a table with 1,000,000 rows:

- Must read and check all 1,000,000 rows
- Takes milliseconds to seconds depending on disk speed

#### Range Scans: O(log n + k)

When using an indexed range query:

```go
// Fast: uses index to find start, then scans k matching rows
table.RangeByIndex("idx_users_age", 18, 65)
```

- O(log n) to find the starting point
- O(k) to scan through k matching rows
- Much faster than full table scan if k << n

#### Write Operations

**INSERT:** O(log n) per index

```go
table.Insert(values)
```

With one primary key index: O(log n)
With 5 indexes: O(5 × log n)

**UPDATE:** Similar to INSERT

If we're updating an indexed column:

- Delete old index entries: O(log n) per index
- Insert new index entries: O(log n) per index
- Update main table: O(log n)

**DELETE:** Similar to UPDATE

- Delete from all indexes: O(log n) per index
- Delete from main table: O(log n)

**Lesson:** Indexes make reads faster but writes slower. Choose wisely.

### Memory Usage

#### Catalog Cache

- LRU cache for table schemas (max 100 entries)
- LRU cache for index metadata (max 500 entries)
- Each schema: ~1-10 KB depending on columns

For typical use (tens of tables), this is negligible. For thousands of tables, only frequently-used ones stay cached.

#### Index Cache

The Table struct caches loaded index B+ trees:

- Avoids reloading from disk on every operation
- Cleared when Table is closed
- Can be manually cleared: `table.ClearIndexCache()`

#### Result Sets

Query results are fully materialized in memory:

```go
rows, _ := table.Scan()  // Loads ALL rows into memory
```

For a table with 1,000,000 rows, this could use hundreds of megabytes.

**Mitigation:**

```go
// Use pagination for large results
rows, _ := table.ScanLimit(offset, 100)
```

### Storage Efficiency

#### Page Utilization

Pages are 4KB. After splits, pages are roughly 50% full.

**Example:**

- Page can hold ~80 small rows
- After split: two pages with ~40 rows each
- Utilization: 50%
- Wasted space: 50%

This is normal for B+ trees. The tradeoff is that future inserts don't require immediate splits.

#### Fragmentation

After deletions, pages have "holes":

```
Before delete:  [AAABBBCCCDDD] (12 bytes used)
After delete B: [AAA___CCCDDD] (9 bytes used, 3 fragmented)
```

When fragmented bytes exceed 64, the page automatically defragments:

```
After defrag:   [AAACCCDDD___] (9 bytes used, 0 fragmented)
```

#### Space Reclamation

**Current limitation:** Deleted data leaves pages sparse, but pages are never freed back to the OS.

A table that once had 1,000,000 rows and now has 100 rows will still use the same amount of disk space.

**Workaround:** Export data and reimport into a fresh database.

**Future:** Implement a freelist to reuse deleted pages.

### Optimization Tips

#### Schema Design

**Use appropriate types:**

```go
// Bad: using TEXT for numbers
{Name: "age", Type: catalog.TypeText}

// Good: using INT
{Name: "age", Type: catalog.TypeInt}
```

Integers are smaller and faster to compare than text.

**Choose good primary keys:**

```go
// Bad: using compound values as text
{Name: "id", Type: catalog.TypeText}  // stores "user_12345"

// Good: using integers
{Name: "id", Type: catalog.TypeInt}
```

Integers make better keys: smaller, faster comparisons, natural ordering.

#### Indexing Strategy

**Index selective columns:**

```go
// Good: high selectivity
{Name: "email", Type: catalog.TypeText, Unique: true}

// Bad: low selectivity (only true/false)
{Name: "is_active", Type: catalog.TypeBoolean}
```

Indexing a boolean column wastes space since half the table matches either value.

**Don't over-index:**

- Each index speeds up reads but slows down writes
- Each index uses disk space
- For a write-heavy table, minimize indexes

**Index columns used in WHERE:**

```sql
-- If we frequently query by age, index it
SELECT * FROM users WHERE age >= 18;
```

```go
catalog.CreateIndex("idx_users_age", "users", "age", false)
```
