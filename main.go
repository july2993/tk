package main

import (
	"fmt"
	"os"

	"github.com/july2993/tk/cmd/amend"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "tk",
}

func main() {
	rootCmd.AddCommand(amend.AmendCMD())

	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
