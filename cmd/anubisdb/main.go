// cmd/anubisdb/main.go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/kithinjibrian/anubisdb/internal/engine"
	"github.com/kithinjibrian/anubisdb/internal/parser"
)

func main() {
	fmt.Println("Welcome to AnubisDB! Type 'exit' to quit.")

	db, err := engine.NewEngine("anubis.db")
	if err != nil {
		fmt.Println("Error initializing database:", err)
		return
	}
	defer db.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			break
		}

		ast, err := parser.Parse(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		result := db.Execute(ast)
		fmt.Println(result)
	}
}
