package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kiwifs/kiwifs/internal/fuse"
	"github.com/spf13/cobra"
)

var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount a remote KiwiFS server as a local filesystem (FUSE)",
	Long: `Mount a remote KiwiFS server as a local filesystem using FUSE.

This allows you to access a remote KiwiFS server as if it were a local folder.
All standard Unix tools (cat, grep, ls, etc.) will work transparently.

Authentication:
  --api-key <value>     sends X-API-Key (preferred for self-hosted deploys)
  --bearer <value>      sends Authorization: Bearer <value>
  --basic-user/--basic-pass  uses HTTP Basic auth
  KIWIFS_API_KEY env var is picked up automatically when --api-key is empty

Examples:
  kiwifs mount --remote http://localhost:3333 ~/knowledge
  kiwifs mount --remote https://wiki.example.com --api-key $TOKEN ~/wiki
  kiwifs mount --remote https://wiki.example.com --space acme ~/acme-wiki`,
	Args: cobra.ExactArgs(1),
	RunE: runMount,
}

func init() {
	mountCmd.Flags().String("remote", "", "remote KiwiFS server URL (required)")
	mountCmd.Flags().String("api-key", "", "API key for X-API-Key auth (falls back to KIWIFS_API_KEY env)")
	mountCmd.Flags().String("bearer", "", "bearer token for Authorization header (falls back to KIWIFS_BEARER env)")
	mountCmd.Flags().String("basic-user", "", "username for HTTP Basic auth")
	mountCmd.Flags().String("basic-pass", "", "password for HTTP Basic auth (falls back to KIWIFS_BASIC_PASS env)")
	mountCmd.Flags().String("space", "", "name of a non-default knowledge space to mount")
	mountCmd.MarkFlagRequired("remote")
}

func runMount(cmd *cobra.Command, args []string) error {
	remote, _ := cmd.Flags().GetString("remote")
	apiKey, _ := cmd.Flags().GetString("api-key")
	bearer, _ := cmd.Flags().GetString("bearer")
	basicUser, _ := cmd.Flags().GetString("basic-user")
	basicPass, _ := cmd.Flags().GetString("basic-pass")
	space, _ := cmd.Flags().GetString("space")
	mountpoint := args[0]

	// Env-var fallback keeps secrets out of shell history and systemd unit
	// files. CLI flag always wins when both are set.
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("KIWIFS_API_KEY"))
	}
	if bearer == "" {
		bearer = strings.TrimSpace(os.Getenv("KIWIFS_BEARER"))
	}
	if basicPass == "" {
		basicPass = os.Getenv("KIWIFS_BASIC_PASS")
	}

	auth := &fuse.ClientAuth{
		APIKey:    apiKey,
		Bearer:    bearer,
		BasicUser: basicUser,
		BasicPass: basicPass,
	}

	client := fuse.NewClientWithAuth(remote, auth, space)
	if err := client.Mount(mountpoint); err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}

	log.Println("Unmounted successfully")
	return nil
}
