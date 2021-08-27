package main

import (
	"example.com/m/internal/cli"
	"fmt"
	"os"
)

func main() {
	err := cli.New().Execute()

	if err != nil {
		fmt.Printf("\nexit: %s\n", err)
		os.Exit(1)
	}
}
