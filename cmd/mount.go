package cmd

import (
	"fmt"
	"log"

	"github.com/kiwifs/kiwifs/internal/fuse"
	"github.com/spf13/cobra"
)

var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount a remote KiwiFS server as a local filesystem (FUSE)",
	Long: `Mount a remote KiwiFS server as a local filesystem using FUSE.

This allows you to access a remote KiwiFS server as if it were a local folder.
All standard Unix tools (cat, grep, ls, etc.) will work transparently.

Examples:
  kiwifs mount --remote http://localhost:3333 ~/knowledge
  kiwifs mount --remote http://kiwifs.example.com ~/remote-wiki`,
	Args: cobra.ExactArgs(1),
	RunE: runMount,
}

func init() {
	mountCmd.Flags().String("remote", "", "remote KiwiFS server URL (required)")
	mountCmd.MarkFlagRequired("remote")
}

func runMount(cmd *cobra.Command, args []string) error {
	remote, _ := cmd.Flags().GetString("remote")
	mountpoint := args[0]

	client := fuse.NewClient(remote)
	if err := client.Mount(mountpoint); err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}

	log.Println("Unmounted successfully")
	return nil
}
