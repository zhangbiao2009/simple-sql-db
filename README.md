# Simple In-Memory SQL Database

A minimal in-memory SQL database implementation in Go that supports basic SQL operations.

## Overview

This project implements a simple SQL database engine that stores data in memory. It follows good software engineering practices with a clear separation of concerns using a component-based architecture.

## Features

- In-memory storage
- Basic SQL support:
  - `CREATE TABLE`
  - `DROP TABLE`
  - `INSERT`
  - `UPDATE`
  - `DELETE`
  - `SELECT` with basic `WHERE` conditions

## Architecture

The database is composed of the following components:

1. **SQL Parser**: Parses SQL strings into an internal representation
2. **Catalog/Schema Manager**: Manages metadata about tables (columns, types)
3. **Storage Engine**: Stores data in memory and provides operations for manipulation
4. **Query Executor**: Orchestrates the interactions between components to execute queries
5. **Database API**: Provides a clean interface for applications to interact with the database

## Building and Running

### Prerequisites

- Go 1.16 or higher

### Build the Project

```bash
go build -o sqldb
```

### Run the Interactive Shell

```bash
./sqldb
```

## Usage Examples

```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT, email TEXT);

-- Insert data
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');

-- Select data
SELECT id, name, email FROM users;
SELECT name FROM users WHERE id = 1;

-- Update data
UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 2;

-- Drop table
DROP TABLE users;
```

## Component Interactions

1. When a SQL statement is submitted, the **Parser** converts it into a structured representation.
2. The **Executor** receives the parsed statement and orchestrates its execution.
3. For DDL statements (CREATE, DROP), the **Catalog** is updated along with the **Storage Engine**.
4. For DML statements (INSERT, UPDATE, DELETE), the **Storage Engine** handles data manipulation after validation against the schema from the **Catalog**.
5. For query statements (SELECT), the **Storage Engine** retrieves matching rows.

## Testing

Run tests with:

```bash
go test ./...
```

## Extensions

This is a minimal implementation. Potential extensions include:

- Add support for indexes
- Implement transactions
- Persist data to disk
- Support more complex SQL operations (JOIN, GROUP BY, etc.)
- Add security features (authentication, authorization)