package cmd

import (
	"fmt"
	"log"

	"github.com/kiwifs/kiwifs/internal/api"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the KiwiFS server",
	Example: `  kiwifs serve --root ~/my-knowledge --port 3333
  kiwifs serve --root /data/knowledge --port 3333 --host 0.0.0.0`,
	RunE: runServe,
}

func init() {
	serveCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	serveCmd.Flags().IntP("port", "p", 3333, "HTTP port")
	serveCmd.Flags().String("host", "0.0.0.0", "bind address")
	serveCmd.Flags().String("search", "grep", "search engine: grep | sqlite")
	serveCmd.Flags().String("versioning", "git", "versioning strategy: git | none")
	serveCmd.Flags().String("auth", "none", "auth type: none | apikey")
	serveCmd.Flags().String("api-key", "", "API key (required if auth=apikey)")
}

func runServe(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	port, _ := cmd.Flags().GetInt("port")
	host, _ := cmd.Flags().GetString("host")
	searchEngine, _ := cmd.Flags().GetString("search")
	versioningStrategy, _ := cmd.Flags().GetString("versioning")
	authType, _ := cmd.Flags().GetString("auth")
	apiKey, _ := cmd.Flags().GetString("api-key")

	cfg := &config.Config{
		Server: config.ServerConfig{Host: host, Port: port},
		Storage: config.StorageConfig{Root: root},
		Search: config.SearchConfig{Engine: searchEngine},
		Versioning: config.VersioningConfig{Strategy: versioningStrategy},
		Auth: config.AuthConfig{Type: authType, APIKey: apiKey},
	}

	store, err := storage.NewLocal(root)
	if err != nil {
		return fmt.Errorf("storage init: %w", err)
	}

	var versioner versioning.Versioner
	switch versioningStrategy {
	case "git":
		versioner, err = versioning.NewGit(root)
		if err != nil {
			log.Printf("warning: git versioning unavailable (%v) — running without versioning", err)
			versioner = versioning.NewNoop()
		}
	default:
		versioner = versioning.NewNoop()
	}

	var searcher search.Searcher
	switch searchEngine {
	default:
		searcher = search.NewGrep(root)
	}

	srv := api.NewServer(cfg, store, versioner, searcher)
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("KiwiFS serving %s on http://%s", root, addr)
	return srv.Start(addr)
}
