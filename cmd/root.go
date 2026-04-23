package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Version is set by main.go during initialization
var Version = "dev"

var rootCmd = &cobra.Command{
	Use:   "kiwifs",
	Short: "KiwiFS — the knowledge filesystem",
	Long: `KiwiFS is a filesystem-based knowledge system.
Agents write with cat. Humans read in the web UI. Same files.

One binary. Storage-agnostic. Git-versioned. Embeddable.`,
}

func Execute() {
	// Set version here to ensure it's picked up after ldflags have been applied
	rootCmd.Version = Version
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(mountCmd)
}
