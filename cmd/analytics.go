package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kiwifs/kiwifs/internal/api"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/spf13/cobra"
)

var analyticsCmd = &cobra.Command{
	Use:   "analytics",
	Short: "Show knowledge base analytics and health metrics",
	Example: `  kiwifs analytics
  kiwifs analytics --scope students/ --stale-threshold 14
  kiwifs analytics --format json`,
	RunE: runAnalytics,
}

func init() {
	analyticsCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	analyticsCmd.Flags().String("scope", "", "path prefix to scope results")
	analyticsCmd.Flags().Int("stale-threshold", 30, "days to consider a page stale")
	analyticsCmd.Flags().String("format", "text", "output format: text or json")
	rootCmd.AddCommand(analyticsCmd)
}

func runAnalytics(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	scope, _ := cmd.Flags().GetString("scope")
	staleThreshold, _ := cmd.Flags().GetInt("stale-threshold")
	if staleThreshold <= 0 {
		staleThreshold = 30
	}
	format, _ := cmd.Flags().GetString("format")

	store, err := storage.NewLocal(root)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	sq, err := search.NewSQLite(root, store)
	if err != nil {
		return fmt.Errorf("open sqlite index: %w", err)
	}
	defer sq.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := api.BuildAnalytics(ctx, sq, nil, scope, staleThreshold)
	if err != nil {
		return err
	}

	if format == "json" {
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Println("Knowledge Base Health")
	fmt.Println("─────────────────────")
	fmt.Printf("Total pages:     %d\n", resp.TotalPages)
	fmt.Printf("Total words:     %d\n", resp.TotalWords)
	fmt.Printf("Stale (>%dd):    %d pages\n", staleThreshold, resp.Health.Stale.Count)
	fmt.Printf("Orphans:         %d pages\n", resp.Health.Orphans.Count)
	fmt.Printf("Broken links:    %d\n", resp.Health.BrokenLinks.Count)
	fmt.Printf("Empty pages:     %d\n", resp.Health.Empty.Count)
	fmt.Printf("No frontmatter:  %d\n", resp.Health.NoFrontmatter.Count)

	fmt.Println()
	fmt.Println("Coverage")
	fmt.Println("────────")
	total := resp.Coverage.PagesWithLinks + resp.Coverage.PagesWithoutLinks
	pct := 0.0
	if total > 0 {
		pct = float64(resp.Coverage.PagesWithLinks) / float64(total) * 100
	}
	fmt.Printf("Pages with links:    %d (%.1f%%)\n", resp.Coverage.PagesWithLinks, pct)
	fmt.Printf("Avg links/page:      %.1f\n", resp.Coverage.AvgLinksPerPage)

	if len(resp.TopUpdated) > 0 {
		fmt.Println()
		fmt.Println("Recently Updated")
		fmt.Println("────────────────")
		for _, p := range resp.TopUpdated {
			fmt.Printf("  %-40s  %s\n", p.Path, p.UpdatedAt)
		}
	}

	return nil
}
