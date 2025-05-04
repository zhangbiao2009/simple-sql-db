package db

import (
	"testing"
)

func TestDB_Execute(t *testing.T) {
	db := New()

	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "Create Table",
			sql:       "CREATE TABLE users (id INT, name TEXT, active BOOL);",
			wantError: false,
		},
		{
			name:      "Invalid SQL",
			sql:       "INVALID SQL STATEMENT;",
			wantError: true,
		},
		{
			name:      "Insert Row",
			sql:       "INSERT INTO users (id, name, active) VALUES (1, 'Alice', TRUE);",
			wantError: false,
		},
		{
			name:      "Insert Multiple Rows",
			sql:       "INSERT INTO users (id, name, active) VALUES (2, 'Bob', TRUE), (3, 'Charlie', FALSE);",
			wantError: false,
		},
		{
			name:      "Select All",
			sql:       "SELECT id, name, active FROM users;",
			wantError: false,
		},
		{
			name:      "Select With Filter",
			sql:       "SELECT name FROM users WHERE id = 2;",
			wantError: false,
		},
		{
			name:      "Update",
			sql:       "UPDATE users SET active = FALSE WHERE id = 1;",
			wantError: false,
		},
		{
			name:      "Delete",
			sql:       "DELETE FROM users WHERE id = 3;",
			wantError: false,
		},
		{
			name:      "Drop Table",
			sql:       "DROP TABLE users;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := db.Execute(tt.sql)

			if tt.wantError && result.Success {
				t.Errorf("Execute(%s) = success, want error", tt.sql)
			} else if !tt.wantError && !result.Success {
				t.Errorf("Execute(%s) error = %v, want success", tt.sql, result.Error)
			}
		})
	}
}

func TestDB_Integration(t *testing.T) {
	// Create a new database for this test
	db := New()

	// Create a table
	result := db.Execute("CREATE TABLE products (id INT, name TEXT, price FLOAT, inStock BOOL);")
	if !result.Success {
		t.Fatalf("Failed to create table: %v", result.Error)
	}

	// Insert data
	result = db.Execute("INSERT INTO products (id, name, price, inStock) VALUES (1, 'Laptop', 999.99, TRUE);")
	if !result.Success {
		t.Fatalf("Failed to insert row: %v", result.Error)
	}

	result = db.Execute("INSERT INTO products (id, name, price, inStock) VALUES (2, 'Phone', 499.99, TRUE), (3, 'Tablet', 299.99, FALSE);")
	if !result.Success {
		t.Fatalf("Failed to insert multiple rows: %v", result.Error)
	}

	// Select and verify data
	result = db.Execute("SELECT id, name, price FROM products WHERE inStock = TRUE;")
	if !result.Success {
		t.Fatalf("Failed to select data: %v", result.Error)
	}

	// The SELECT results might not be in a predictable order, so we need to check differently
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	} else {
		// Create a map of products by name for easier checking
		products := make(map[string]map[string]string)
		for _, row := range result.Rows {
			products[row["name"]] = row
		}

		// Check laptop
		laptop, ok := products["Laptop"]
		if !ok {
			t.Errorf("Expected to find product 'Laptop'")
		} else if laptop["price"] != "999.99" {
			t.Errorf("Expected Laptop price = 999.99, got %s", laptop["price"])
		}

		// Check phone
		phone, ok := products["Phone"]
		if !ok {
			t.Errorf("Expected to find product 'Phone'")
		} else if phone["price"] != "499.99" {
			t.Errorf("Expected Phone price = 499.99, got %s", phone["price"])
		}
	}

	// Update data
	result = db.Execute("UPDATE products SET price = 449.99 WHERE name = 'Phone';")
	if !result.Success {
		t.Fatalf("Failed to update data: %v", result.Error)
	}

	// Verify update
	result = db.Execute("SELECT price FROM products WHERE name = 'Phone';")
	if !result.Success || len(result.Rows) != 1 {
		t.Fatalf("Failed to select updated data")
	}

	if result.Rows[0]["price"] != "449.99" {
		t.Errorf("Expected updated price = 449.99, got %s", result.Rows[0]["price"])
	}

	// Delete data
	result = db.Execute("DELETE FROM products WHERE inStock = FALSE;")
	if !result.Success {
		t.Fatalf("Failed to delete data: %v", result.Error)
	}

	// Verify delete
	result = db.Execute("SELECT * FROM products;")
	if !result.Success {
		t.Fatalf("Failed to select after delete: %v", result.Error)
	}

	// Count rows after delete
	foundLaptop := false
	foundPhone := false

	for _, row := range result.Rows {
		name := row["name"]
		if name == "Laptop" {
			foundLaptop = true
		} else if name == "Phone" {
			foundPhone = true
		} else {
			t.Errorf("Unexpected product after delete: %s", name)
		}
	}

	if !foundLaptop {
		t.Errorf("Expected to find 'Laptop' after delete")
	}

	if !foundPhone {
		t.Errorf("Expected to find 'Phone' after delete")
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after delete, got %d", len(result.Rows))
	}

	// Drop table
	result = db.Execute("DROP TABLE products;")
	if !result.Success {
		t.Fatalf("Failed to drop table: %v", result.Error)
	}
}

func TestFormatResult(t *testing.T) {
	// Test a successful result
	successResult := Result{
		Success:      true,
		RowsAffected: 3,
	}
	formatted := FormatResult(successResult)
	if formatted != "3 rows affected\n" {
		t.Errorf("FormatResult() = %s, want '3 rows affected\\n'", formatted)
	}

	// Test an error result
	errorResult := Result{
		Success: false,
		Error:   &databaseError{message: "table not found"},
	}
	formatted = FormatResult(errorResult)
	if formatted != "Error: table not found" {
		t.Errorf("FormatResult() = %s, want 'Error: table not found'", formatted)
	}

	// Test a result with rows
	rowsResult := Result{
		Success: true,
		Columns: []string{"id", "name"},
		Rows: []map[string]string{
			{"id": "1", "name": "Alice"},
			{"id": "2", "name": "Bob"},
		},
	}
	formatted = FormatResult(rowsResult)

	// Just check that it contains the expected data
	if len(formatted) == 0 {
		t.Errorf("FormatResult() returned empty string")
	}
}

// Custom error type for testing
type databaseError struct {
	message string
}

func (e *databaseError) Error() string {
	return e.message
}
