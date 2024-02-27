package main

import (
	"fmt"
	"os"

	generate "github.com/bitcoin-sv/ubsv/cmd/testUtil/compose/generator"
	"github.com/bitcoin-sv/ubsv/cmd/testUtil/compose/runner"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{Use: "app"}
	generate.AddGenerateCommand(rootCmd)
	runner.AddRunCommand(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
