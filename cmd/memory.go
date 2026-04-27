package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/memory"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/spf13/cobra"
)

var memoryCmd = &cobra.Command{
	Use:   "memory",
	Short: "Episodic and consolidation memory report",
	Long: `Inspect episodic (per-run) knowledge and whether it is referenced
from merged-from on semantic or other pages. Does not run LLM consolidation;
it is a report for schedulers, CI, and custom merge pipelines.`,
}

var memoryReportCmd = &cobra.Command{
	Use:   "report",
	Short: "List episodic files and unmerged coverage",
	RunE:  runMemoryReport,
}

func init() {
	memoryReportCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	memoryReportCmd.Flags().BoolP("json", "j", false, "print JSON instead of a table")
	memoryReportCmd.Flags().String("episodes-prefix", "", "override [memory] episodes_path_prefix in config (default: episodes/)")
	memoryCmd.AddCommand(memoryReportCmd)
	rootCmd.AddCommand(memoryCmd)
}

func runMemoryReport(cmd *cobra.Command, _ []string) error {
	root, _ := cmd.Flags().GetString("root")
	wantJSON, _ := cmd.Flags().GetBool("json")
	prefixFlag, _ := cmd.Flags().GetString("episodes-prefix")

	s, err := storage.NewLocal(root)
	if err != nil {
		return err
	}

	opt := memory.Options{}
	if prefixFlag != "" {
		opt.EpisodesPathPrefix = prefixFlag
	} else if cfg, lerr := config.Load(root); lerr == nil {
		p := cfg.Memory.EpisodesPathPrefix
		if p != "" {
			opt.EpisodesPathPrefix = p
		}
	}

	rep, err := memory.Scan(context.Background(), s, opt)
	if err != nil {
		return err
	}

	if wantJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}

	fmt.Printf("episodic files:          %d\n", rep.EpisodicCount)
	fmt.Printf("merged-from references:  %d\n", rep.MergedFromRefs)
	fmt.Printf("unmerged (no merged-from): %d\n", len(rep.Unmerged))
	if len(rep.Unmerged) == 0 {
		fmt.Fprintln(os.Stdout, "all episodic files are referenced by at least one merged-from list")
	} else {
		for _, e := range rep.Unmerged {
			if e.EpisodeID != "" {
				fmt.Printf("  - %s  (episode_id=%s)\n", e.Path, e.EpisodeID)
			} else {
				fmt.Printf("  - %s  (set episode_id)\n", e.Path)
			}
		}
	}
	for _, w := range rep.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	return nil
}
