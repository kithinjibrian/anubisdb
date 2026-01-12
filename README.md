<h1 align="center">AnubisDB</h1>
<p align="center">
<img src="https://i.postimg.cc/jqJL78gp/anubis.png" alt="anubis-logo" width="300px"/>
<br>
<em>A persistent, B+Tree backed SQL database engine written from scratch in Go.</em>
</p>

---

## Overview

AnubisDB is a lightweight relational database built entirely from scratch in Go, featuring its own SQL parser, query planner, and storage engine. It demonstrates core database concepts including B+Tree indexing, cost-based query optimization, and persistent storage - all without relying on external database libraries.

---

## Features

### Core SQL Operations

- **CRUD Operations**: Full support for `SELECT`, `INSERT`, `UPDATE`, and `DELETE`
- **Schema Management**: `CREATE TABLE` with typed columns and constraints
- **Data Types**: `INT`, `VARCHAR/TEXT`, `FLOAT`, `BOOLEAN`
- **Constraints**: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `AUTO_INCREMENT`

### Query Features

- **Filtering**: `WHERE` clauses with multiple conditions (`AND`, `OR`)
- **Sorting**: `ORDER BY` with `ASC`/`DESC` on multiple columns
- **Pagination**: `LIMIT` and `OFFSET` support
- **Deduplication**: `DISTINCT` keyword
- **Joins**: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`
- **Aggregation**: `GROUP BY` with `HAVING` clause
- **Qualified Names**: Table aliases and qualified column references (e.g., `users.id`)

### Storage & Performance

- **Persistent Storage**: Data survives restarts via custom binary file format
- **B+Tree Indexing**: Automatic indexing on Primary Keys + manual index creation
- **Query Optimization**: Cost-based planner chooses optimal execution strategy
- **Index Types**: Regular and `UNIQUE` indexes for fast lookups
- **Query Explainer**: Visualize query execution plans and costs

---

## Architecture

AnubisDB follows a classic database architecture with clear separation of concerns:

```
SQL Query → Parser → Planner → Executor → Storage Engine
                        ↓
                    Catalog (Metadata)
                        ↓
                B+Tree Index / Pager (Disk I/O)
```

### The Query Pipeline

1. **Lexer & Parser**: Tokenize and validate SQL against EBNF grammar
2. **Query Planner**: Generate optimized execution plan using table statistics and available indexes
3. **Executor**: Execute plan using B+Tree iterators and page-based storage
4. **Formatter**: Project and format results for display

---

## Quick Start

### Installation

```bash
git clone https://github.com/kithinjibrian/anubisdb.git
cd anubisdb
go build -o anubisdb ./cmd/anubisdb/main.go
```

---

## Usage Examples

### 1. Basic Table Operations

```sql
$ ./anubisdb anubis.db

anubis> CREATE TABLE users (id INT PRIMARY KEY, username VARCHAR UNIQUE, password VARCHAR, age INT)
Table 'users' created successfully

anubis> INSERT INTO users (id, username, password, age) VALUES (1, john, john1234, 25)
1 row inserted

anubis> INSERT INTO users (id, username, password, age) VALUES (2, jane, jane1234, 30)
1 row inserted

anubis> INSERT INTO users (id, username, password, age) VALUES (3, bob, bob1234, 22)
1 row inserted

anubis> INSERT INTO users (id, username, password, age) VALUES (4, alice, alice1234, 28)
1 row inserted

anubis> SELECT * FROM users
users.id        | users.username  | users.password  | users.age
----------------------------------------------------------------
1               | john            | john1234        | 25
2               | jane            | jane1234        | 30
3               | bob             | bob1234         | 22
4               | alice           | alice1234       | 28

4 row(s) returned
```

### 2. Basic Filtering and Updates

```sql
anubis> SELECT username, age FROM users WHERE age > 25
username        | age
--------------------------------
jane            | 30
alice           | 28

2 row(s) returned

anubis> UPDATE users SET age = 26 WHERE username = john
1 row(s) updated

anubis> SELECT * FROM users WHERE username = john
users.id        | users.username  | users.password  | users.age
----------------------------------------------------------------
1               | john            | john1234        | 26

1 row(s) returned
```

### 3. CREATE INDEX

```sql
anubis> CREATE INDEX idx_age ON users (age)
INDEX 'idx_age' created successfully on users([age])

anubis> CREATE UNIQUE INDEX idx_username ON users (username)
UNIQUE INDEX 'idx_username' created successfully on users([username])

anubis> SELECT * FROM users WHERE age = 30
users.id        | users.username  | users.password  | users.age
----------------------------------------------------------------
2               | jane            | jane1234        | 30

1 row(s) returned
```

### 4. ORDER BY

```sql
anubis> SELECT username, age FROM users ORDER BY age ASC
username        | age
--------------------------------
bob             | 22
john            | 26
alice           | 28
jane            | 30

4 row(s) returned

anubis> SELECT username, age FROM users ORDER BY age DESC
username        | age
--------------------------------
jane            | 30
alice           | 28
john            | 26
bob             | 22

4 row(s) returned

anubis> SELECT * FROM users ORDER BY username ASC
users.id        | users.username  | users.password  | users.age
----------------------------------------------------------------
4               | alice           | alice1234       | 28
3               | bob             | bob1234         | 22
2               | jane            | jane1234        | 30
1               | john            | john1234        | 26

4 row(s) returned
```

### 5. LIMIT and OFFSET

```sql
anubis> SELECT username, age FROM users ORDER BY age DESC LIMIT 2
username        | age
--------------------------------
jane            | 30
alice           | 28

2 row(s) returned

anubis> SELECT username, age FROM users ORDER BY age DESC LIMIT 2 OFFSET 2
username        | age
--------------------------------
john            | 26
bob             | 22

2 row(s) returned

anubis> SELECT * FROM users LIMIT 1
users.id        | users.username  | users.password  | users.age
----------------------------------------------------------------
1               | john            | john1234        | 26

1 row(s) returned
```

### 6. JOIN Operations

```sql
anubis> CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, total FLOAT, status VARCHAR)
Table 'orders' created successfully

anubis> INSERT INTO orders (order_id, user_id, total, status) VALUES (101, 1, 99.99, pending)
1 row inserted

anubis> INSERT INTO orders (order_id, user_id, total, status) VALUES (102, 2, 149.50, completed)
1 row inserted

anubis> INSERT INTO orders (order_id, user_id, total, status) VALUES (103, 1, 75.00, completed)
1 row inserted

anubis> INSERT INTO orders (order_id, user_id, total, status) VALUES (104, 4, 200.00, pending)
1 row inserted

anubis> INSERT INTO orders (order_id, user_id, total, status) VALUES (105, 3, 50.25, completed)
1 row inserted
```

#### 6a. INNER JOIN

```sql
anubis> SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id
u.id            | u.username      | u.password      | u.age           | o.order_id      | o.user_id       | o.total         | o.status
--------------------------------------------------------------------------------------------------------------------------------
1               | john            | john1234        | 26              | 101             | 1               | 99.99           | pending
1               | john            | john1234        | 26              | 103             | 1               | 75              | completed
2               | jane            | jane1234        | 30              | 102             | 2               | 149.5           | completed
3               | bob             | bob1234         | 22              | 105             | 3               | 50.25           | completed
4               | alice           | alice1234       | 28              | 104             | 4               | 200             | pending

5 row(s) returned

anubis> SELECT u.username, o.order_id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id
u.username      | o.order_id      | o.total
------------------------------------------------
john            | 101             | 99.99
john            | 103             | 75
jane            | 102             | 149.5
bob             | 105             | 50.25
alice           | 104             | 200

5 row(s) returned
```

#### 6b. LEFT JOIN

```sql
anubis> SELECT u.username, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id
u.username      | o.order_id
--------------------------------
john            | 101
john            | 103
jane            | 102
bob             | 105
alice           | 104

5 row(s) returned
```

### 7. Persistence Check

```sql
anubis> DELETE FROM users WHERE age < 25
1 row(s) deleted

anubis> SELECT username, age FROM users ORDER BY age ASC
username        | age
--------------------------------
john            | 26
alice           | 28
jane            | 30

3 row(s) returned

anubis> exit
```

```sql
$ ./anubisdb anubis.db

anubis> SELECT username, age FROM users ORDER BY age ASC
username        | age
--------------------------------
john            | 26
alice           | 28
jane            | 30

3 row(s) returned

anubis> exit
```

## Query Optimization

AnubisDB includes a cost-based query planner that automatically chooses efficient execution strategies:

```go
// Example: Using the planner
planner := engine.NewPlanner(catalog)
planner.RegisterTable("users", 10000) // 10k rows
planner.RegisterIndex("users", "idx_age", []string{"age"}, false)

plan, _ := planner.Plan(parsedQuery)
fmt.Println(engine.Explain(plan))
```

**Output:**

```
Execution Plan:
Project([username, age], cost=15.20) <-
  Sort([age ASC], cost=12.50) <-
    Scan(users, type=IndexScan, index=idx_age, rows=500, cost=5.00)
Total Cost: 15.20
```

The planner considers:

- Table row counts and selectivity estimates
- Available indexes and their uniqueness
- Join algorithms (nested loop, hash join planned for future)
- Sort and aggregation costs

---

## Data Persistence

All data is automatically persisted to disk using a custom page-based storage format:

```bash
$ ./anubisdb data.db
anubis> CREATE TABLE test (id INT PRIMARY KEY, value TEXT)
anubis> INSERT INTO test (id, value) VALUES (1, hello)
anubis> exit

$ ./anubisdb data.db  # Reopen the same database
anubis> SELECT * FROM test
id              | value
--------------------------------
1               | hello

1 row(s) returned
```

### Storage Architecture

- **Pager**: Manages fixed-size pages (4KB default) with LRU caching
- **B+Tree**: Provides O(log n) lookups and range scans
- **Catalog**: Stores table schemas and index metadata
- **Binary Format**: Custom serialization for efficient disk I/O

---

## Roadmap

AnubisDB is under active development. Here's what's coming:

### Short-Term (v0.2)

- [ ] Additional aggregate functions (`SUM`, `AVG`, `MIN`, `MAX`)
- [ ] `LIKE` operator for pattern matching
- [ ] `IN` operator for value lists
- [ ] Composite indexes (multi-column)
- [ ] Better error messages and SQL validation

### Medium-Term (v0.3)

- [ ] Write-Ahead Log (WAL) for crash recovery
- [ ] ACID transaction support (`BEGIN`, `COMMIT`, `ROLLBACK`)
- [ ] Foreign key constraints
- [ ] `ALTER TABLE` support
- [ ] Hash join algorithm

### Long-Term (v1.0)

- [ ] Multi-version concurrency control (MVCC)
- [ ] Query result caching
- [ ] Network protocol (PostgreSQL wire protocol)
- [ ] Replication and high availability
- [ ] Query parallelization

---

## Limitations

### Current Constraints

- **No Transactions**: Changes are immediately committed; no rollback support
- **Single-Threaded**: No concurrent query execution
- **Memory-Based Operations**: Joins, sorts, and groups happen entirely in memory
- **Limited Aggregates**: Only `COUNT(*)` is currently implemented
- **No Subqueries**: Nested SELECT statements not yet supported

### Known Issues

- Crash during write operations may corrupt the database file
- Large result sets may cause memory pressure
- No query timeout mechanism

---

## Acknowledgments

Built with inspiration from:

- SQLite's architecture and B+Tree implementation
- PostgreSQL's query planner design
- "Database Internals" by Alex Petrov
- "CMU 15-445: Database Systems" course materials

---

<p align="center">
<em>AnubisDB: Because sometimes you need to build your own database to truly understand how they work.</em>
</p>
