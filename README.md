<h1 align="center">AnubisDB</h1>
<p align="center">
<img src="https://i.postimg.cc/jqJL78gp/anubis.png" alt="anubis-logo" width="300px"/>
<br>
<em>A persistent, B+Tree backed SQL database engine written from scratch in Go.</em>
</p>

---

## Current Features

- **Standard SQL CRUD**: Full support for `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.
- **Schema Management**: Create tables with typed columns (`INT`, `TEXT`), `PRIMARY KEY`, and `NOT NULL` constraints.
- **Persistent Storage**: Data survives restarts by being persisted into a custom binary file format managed by the Pager.
- **Indexing**: Automatic B+Tree indexing on Primary Keys for high-speed lookups.
- **Query Explainer**: Use the `Explain()` function to visualize the cost and strategy chosen by the planner before execution.

---

## Roadmap & Future Vision

AnubisDB is an evolving project. Below is the path toward becoming a production-grade engine:

### What's Missing

- **ACID Transactions**: Currently, there is no Write-Ahead Log (WAL). If the process crashes mid-insert, the file may become corrupted.

### Short-Term Goals

1. **More Data Types**: Add support for `TIMESTAMP`, `BOOLEAN`, and `BLOB`.
2. **Concurrency**: Implement a Page-level locking mechanism to allow multiple readers and one writer.

---

## The Query Pipeline

The lifecycle of a query in AnubisDB is a highly orchestrated process:

1. **Parse**: The SQL string is validated against the EBNF grammar.
2. **Plan**: The Planner asks the Catalog: _"Does this table exist? Is there an index on the filtered column?"_
3. **Execute**: The Engine opens a B+Tree iterator, fetches pages via the Pager, deserializes the bytes, and filters them.
4. **Format**: The results are projected into the requested columns and returned as a formatted table.

## Getting Started

### Installation

```bash
git clone https://github.com/kithinjibrian/anubisdb.git
cd anubisdb
go build -o anubisdb ./cmd/anubisdb/main.go
```

### Running the CLI

```sql
$ ./anubisdb pyramid.db

anubis> CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)
Table 'test' created successfully

anubis> INSERT INTO test (id, val) VALUES (1, hello)
1 row inserted

anubis> INSERT INTO test (id, val) VALUES (2, world)
1 row inserted

anubis> SELECT * FROM test
id              | val
--------------------------------
1               | hello
2               | world

2 row(s) returned

anubis> UPDATE test SET val = kenya WHERE id = 2
1 row(s) updated

anubis> SELECT * FROM test
id              | val
--------------------------------
1               | hello
2               | kenya

2 row(s) returned

anubis> DELETE FROM test WHERE id = 1
1 row(s) deleted

anubis> SELECT * FROM test
id              | val
--------------------------------
2               | kenya

1 row(s) returned

anubis> exit

$ ./anubisdb pyramid.db

anubis> SELECT * FROM test
id              | val
--------------------------------
2               | kenya

1 row(s) returned
```
