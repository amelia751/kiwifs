package cmd

import (
	"fmt"
	"log"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/mcpserver"
	"github.com/kiwifs/kiwifs/internal/tracing"
	"github.com/spf13/cobra"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start the KiwiFS MCP server",
	Long: `Start a Model Context Protocol server for KiwiFS.

The MCP server gives any AI agent (Claude, Cursor, custom) a structured
tool interface to KiwiFS — read, write, search, query metadata — over
the standard MCP protocol.

Backend modes:
  --root  <path>   In-process mode — opens the knowledge directory directly
  --remote <url>   Proxy mode — talks to a running KiwiFS server over REST

Transports:
  stdio            Default transport for clients that launch KiwiFS as a subprocess
  --http           Streamable HTTP transport served at /mcp`,
	Example: `  kiwifs mcp --root ~/knowledge
  kiwifs mcp --remote http://localhost:3333
  kiwifs mcp --remote http://kiwifs.example.com:3333 --api-key $KIWI_API_KEY
  kiwifs mcp --root ~/knowledge --http
  kiwifs mcp --remote http://localhost:3333 --http --port 8080`,
	RunE: runMCP,
}

func init() {
	mcpCmd.Flags().String("root", "", "knowledge root directory (in-process mode)")
	mcpCmd.Flags().String("remote", "", "KiwiFS server URL (proxy mode)")
	mcpCmd.Flags().String("api-key", "", "API key for remote server")
	mcpCmd.Flags().String("space", "default", "space to scope operations to")
	mcpCmd.Flags().Bool("http", false, "serve MCP over Streamable HTTP instead of stdio")
	mcpCmd.Flags().Int("port", 8181, "HTTP MCP port (used with --http)")
}

func runMCP(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	remote, _ := cmd.Flags().GetString("remote")
	apiKey, _ := cmd.Flags().GetString("api-key")
	space, _ := cmd.Flags().GetString("space")
	httpTransport, _ := cmd.Flags().GetBool("http")
	port, _ := cmd.Flags().GetInt("port")

	if root == "" && remote == "" {
		return fmt.Errorf("exactly one of --root or --remote is required")
	}
	if root != "" && remote != "" {
		return fmt.Errorf("--root and --remote are mutually exclusive — pick one")
	}
	if root != "" && space != "default" && cmd.Flags().Changed("space") {
		return fmt.Errorf("--space is only meaningful with --remote; local mode operates on the --root directory directly")
	}
	if !httpTransport && cmd.Flags().Changed("port") {
		return fmt.Errorf("--port requires --http")
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("--port must be between 1 and 65535")
	}

	var em tracing.Emitter
	if root != "" {
		if cfg, err := config.Load(root); err == nil {
			em = tracing.NewEmitter(cfg.Tracing.IsEnabled(), cfg.Tracing.Output, cfg.Tracing.File)
		} else {
			log.Printf("mcp: config load (%v) — tracing disabled", err)
		}
	}

	return mcpserver.Serve(mcpserver.Options{
		Remote:  remote,
		Root:    root,
		APIKey:  apiKey,
		Space:   space,
		HTTP:    httpTransport,
		Port:    port,
		Emitter: em,
	})
}
