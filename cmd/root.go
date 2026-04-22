package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kiwifs",
	Short: "KiwiFS — the knowledge filesystem",
	Long: `KiwiFS is a filesystem-based knowledge system.
Agents write with cat. Humans read in the web UI. Same files.

One binary. Storage-agnostic. Git-versioned. Embeddable.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(initCmd)
}
