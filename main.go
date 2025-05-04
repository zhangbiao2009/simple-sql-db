package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/zhangbiao2009/simple-sql-db/db"
)

func main() {
	fmt.Println("Simple SQL Database (In-Memory)")
	fmt.Println("Enter SQL statements terminated by a semicolon (;)")
	fmt.Println("Type 'exit' or 'quit' to exit the program")
	fmt.Println()

	// Create a new database instance
	database := db.New()

	// Create a scanner for reading input
	scanner := bufio.NewScanner(os.Stdin)
	var sqlBuffer strings.Builder

	// Start the REPL
	for {
		// Print the prompt if we're starting a new statement
		if sqlBuffer.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print("  -> ")
		}

		// Read a line of input
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()

		// Check for exit commands
		if sqlBuffer.Len() == 0 && (strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit")) {
			break
		}

		// Add the line to the buffer
		sqlBuffer.WriteString(line)

		// Check if the statement is complete (ends with a semicolon)
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			// Execute the statement
			sql := sqlBuffer.String()
			result := database.Execute(sql)

			// Print the result
			fmt.Println(db.FormatResult(result))

			// Reset the buffer for the next statement
			sqlBuffer.Reset()
		} else {
			// Add a newline for multiline statements
			sqlBuffer.WriteString("\n")
		}
	}

	fmt.Println("Goodbye!")
}
