package parser

import (
	"testing"

	"github.com/zhangbiao2009/simple-sql-db/types"
)

func TestParseCreateTable(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name     string
		sql      string
		wantName string
		wantCols int
		wantErr  bool
	}{
		{
			name:     "Valid CREATE TABLE",
			sql:      "CREATE TABLE users (id INT, name TEXT, age INT);",
			wantName: "users",
			wantCols: 3,
			wantErr:  false,
		},
		{
			name:     "CREATE TABLE with constraints",
			sql:      "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT);",
			wantName: "users",
			wantCols: 3,
			wantErr:  false,
		},
		{
			name:    "Invalid CREATE TABLE syntax",
			sql:     "CREATE TABLE users id INT, name TEXT);",
			wantErr: true,
		},
		{
			name:    "Invalid column definition",
			sql:     "CREATE TABLE users (id);",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse() error = nil, wantErr %v", tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if stmt.Type() != types.StmtCreate {
				t.Errorf("Statement type = %v, want %v", stmt.Type(), types.StmtCreate)
			}

			createStmt := stmt.(CreateTableStatement)
			if createStmt.TableName() != tt.wantName {
				t.Errorf("TableName() = %v, want %v", createStmt.TableName(), tt.wantName)
			}

			if len(createStmt.Columns()) != tt.wantCols {
				t.Errorf("len(Columns()) = %v, want %v", len(createStmt.Columns()), tt.wantCols)
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name      string
		sql       string
		wantTable string
		wantCols  int
		wantRows  int
		wantErr   bool
	}{
		{
			name:      "Valid INSERT",
			sql:       "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);",
			wantTable: "users",
			wantCols:  3,
			wantRows:  1,
			wantErr:   false,
		},
		{
			name:      "Multiple rows INSERT",
			sql:       "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');",
			wantTable: "users",
			wantCols:  2,
			wantRows:  2,
			wantErr:   false,
		},
		{
			name:    "Invalid INSERT syntax",
			sql:     "INSERT users (id, name) VALUES (1, 'Alice');",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse() error = nil, wantErr %v", tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if stmt.Type() != types.StmtInsert {
				t.Errorf("Statement type = %v, want %v", stmt.Type(), types.StmtInsert)
			}

			insertStmt := stmt.(InsertStatement)
			if insertStmt.TableName() != tt.wantTable {
				t.Errorf("TableName() = %v, want %v", insertStmt.TableName(), tt.wantTable)
			}

			if len(insertStmt.Columns()) != tt.wantCols {
				t.Errorf("len(Columns()) = %v, want %v", len(insertStmt.Columns()), tt.wantCols)
			}

			if len(insertStmt.Values()) != tt.wantRows {
				t.Errorf("len(Values()) = %v, want %v", len(insertStmt.Values()), tt.wantRows)
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name      string
		sql       string
		wantTable string
		wantCols  int
		wantErr   bool
	}{
		{
			name:      "Simple SELECT",
			sql:       "SELECT id, name FROM users;",
			wantTable: "users",
			wantCols:  2,
			wantErr:   false,
		},
		{
			name:      "SELECT with WHERE clause",
			sql:       "SELECT id, name FROM users WHERE age = 30;",
			wantTable: "users",
			wantCols:  2,
			wantErr:   false,
		},
		{
			name:    "Invalid SELECT syntax",
			sql:     "SELECT FROM users;",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse() error = nil, wantErr %v", tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if stmt.Type() != types.StmtSelect {
				t.Errorf("Statement type = %v, want %v", stmt.Type(), types.StmtSelect)
			}

			selectStmt := stmt.(SelectStatement)
			if selectStmt.TableName() != tt.wantTable {
				t.Errorf("TableName() = %v, want %v", selectStmt.TableName(), tt.wantTable)
			}

			if len(selectStmt.Columns()) != tt.wantCols {
				t.Errorf("len(Columns()) = %v, want %v", len(selectStmt.Columns()), tt.wantCols)
			}
		})
	}
}
