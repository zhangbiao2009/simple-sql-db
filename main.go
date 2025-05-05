package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/zhangbiao2009/simple-sql-db/db"
)

const (
	historyFile = ".simple_sql_history"
)

// SQLCompleter provides autocompletion for SQL keywords and statements
type SQLCompleter struct {
	keywords []string
}

// NewSQLCompleter creates a new SQL completer with common SQL keywords
func NewSQLCompleter() *SQLCompleter {
	return &SQLCompleter{
		keywords: []string{
			"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
			"UPDATE", "SET", "DELETE", "CREATE", "TABLE", "DROP",
			"AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "INT",
			"TEXT", "FLOAT", "BOOL", "VARCHAR", "PRIMARY", "KEY",
		},
	}
}

// Do provides completion candidates for the given line
func (c *SQLCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	lineStr := string(line[:pos])
	parts := strings.Fields(lineStr)

	// If we're at the start of a new word or completing an existing word
	if len(parts) == 0 || (len(lineStr) > 0 && lineStr[len(lineStr)-1] != ' ') {
		var lastWord string
		if len(parts) > 0 {
			lastWord = strings.ToUpper(parts[len(parts)-1])
		} else {
			lastWord = ""
		}

		// Find matching keywords
		var matches []string
		for _, kw := range c.keywords {
			if strings.HasPrefix(kw, lastWord) {
				matches = append(matches, kw)
			}
		}

		// Convert matches to readline format
		if len(matches) > 0 {
			length = len(lastWord)
			for _, match := range matches {
				newLine = append(newLine, []rune(match))
			}
			return
		}
	}

	return nil, 0
}

func main() {
	fmt.Println("Simple SQL Database (In-Memory)")
	fmt.Println("Enter SQL statements terminated by a semicolon (;)")
	fmt.Println("Type 'exit' or 'quit' to exit the program")
	fmt.Println("Press Tab for keyword completion, Up/Down arrows for history")
	fmt.Println()

	// Create a new database instance
	database := db.New()

	// Setup readline with history
	homeDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("Warning: Could not determine home directory: %v\n", err)
		homeDir = "."
	}
	histPath := filepath.Join(homeDir, historyFile)

	// Create SQL completer
	completer := NewSQLCompleter()

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "sql> ",
		HistoryFile:       histPath,
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true, // Case-insensitive history search
		AutoComplete:      completer,
	})
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	var sqlBuffer strings.Builder

	// Start the REPL
	for {
		var prompt string
		if sqlBuffer.Len() == 0 {
			prompt = "sql> "
		} else {
			prompt = "  -> "
		}
		rl.SetPrompt(prompt)

		// Read a line of input
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			// Reset the buffer on Ctrl-C
			sqlBuffer.Reset()
			continue
		} else if err == io.EOF {
			// Exit on Ctrl-D
			break
		} else if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// Handle exit commands
		line = strings.TrimSpace(line)
		if sqlBuffer.Len() == 0 && (strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit")) {
			break
		}

		// Add the line to the buffer
		sqlBuffer.WriteString(line)

		// Check if the statement is complete (ends with a semicolon)
		if strings.HasSuffix(line, ";") {
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
