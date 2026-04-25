package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	osexec "os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/spf13/cobra"
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run storage and latency benchmarks",
	Long: `Benchmarks the local knowledge stack: single writes, bulk writes,
reads, search queries, and storage overhead. Runs in a temp directory with
an in-process server (no TCP) so results reflect pipeline latency, not
network overhead.

Output is a markdown table suitable for pasting into issues or docs.`,
	Example: `  kiwifs bench
  kiwifs bench --files 200 --search-files 500`,
	RunE: runBench,
}

func init() {
	benchCmd.Flags().Int("files", 50, "number of files for write/read benchmarks")
	benchCmd.Flags().Int("bulk", 100, "number of files for bulk write benchmark")
	benchCmd.Flags().Int("search-files", 1000, "number of files to index for search benchmark")
	benchCmd.Flags().Int("search-queries", 50, "number of search queries to run")
	rootCmd.AddCommand(benchCmd)
}

func runBench(cmd *cobra.Command, args []string) error {
	nFiles, _ := cmd.Flags().GetInt("files")
	nBulk, _ := cmd.Flags().GetInt("bulk")
	nSearchFiles, _ := cmd.Flags().GetInt("search-files")
	nSearchQueries, _ := cmd.Flags().GetInt("search-queries")

	dir, err := os.MkdirTemp("", "kiwifs-bench-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	cfg := &config.Config{}
	cfg.Storage.Root = dir
	cfg.Search.Engine = "sqlite"
	cfg.Versioning.Strategy = "git"
	cfg.Auth.Type = "none"

	stack, err := bootstrap.Build("bench", dir, cfg)
	if err != nil {
		return fmt.Errorf("build stack: %w", err)
	}
	defer stack.Close()

	handler := stack.Server

	fmt.Println("# KiwiFS Benchmark Results")
	fmt.Printf("Date: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	writeLats := benchWrite(handler, nFiles)
	printLatencyTable("Single write (PUT)", writeLats)

	bulkLat := benchBulkWrite(handler, nBulk)
	fmt.Printf("| Bulk write (%d files) | %.1fms total | %.2fms per file |\n\n",
		nBulk, float64(bulkLat)/float64(time.Millisecond),
		float64(bulkLat)/float64(time.Millisecond)/float64(nBulk))

	readLats := benchRead(handler, nFiles)
	printLatencyTable("Read (GET)", readLats)

	searchLats := benchSearch(handler, nSearchFiles, nSearchQueries)
	printLatencyTable("Search", searchLats)

	benchStorageOverhead(dir, nFiles+nBulk+nSearchFiles)

	return nil
}

func benchWrite(handler http.Handler, n int) []time.Duration {
	lats := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("bench/write-%04d.md", i)
		body := fmt.Sprintf("# Benchmark %d\n\nContent for file %d with some body text for indexing.", i, i)
		start := time.Now()
		req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path="+path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		lats = append(lats, time.Since(start))
		if rec.Code != http.StatusOK {
			fmt.Printf("  warn: PUT %s returned %d\n", path, rec.Code)
		}
	}
	return lats
}

func benchBulkWrite(handler http.Handler, n int) time.Duration {
	var sb strings.Builder
	sb.WriteString(`{"files":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"path":"bench/bulk-%04d.md","content":"# Bulk %d\n\nBulk content %d."}`, i, i, i)
	}
	sb.WriteString(`]}`)

	start := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/bulk", strings.NewReader(sb.String()))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK {
		fmt.Printf("  warn: bulk PUT returned %d: %s\n", rec.Code, rec.Body.String())
	}
	return elapsed
}

func benchRead(handler http.Handler, n int) []time.Duration {
	lats := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("bench/write-%04d.md", i)
		start := time.Now()
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path="+path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		lats = append(lats, time.Since(start))
	}
	return lats
}

func benchSearch(handler http.Handler, nFiles, nQueries int) []time.Duration {
	words := []string{"benchmark", "content", "indexed", "knowledge", "system",
		"pipeline", "storage", "search", "engine", "performance"}

	for i := 0; i < nFiles; i++ {
		path := fmt.Sprintf("bench/search-%04d.md", i)
		w1 := words[rand.Intn(len(words))]
		w2 := words[rand.Intn(len(words))]
		body := fmt.Sprintf("# Search doc %d\n\nThis document covers %s and %s topics for benchmarking the search engine.", i, w1, w2)
		req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path="+path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	lats := make([]time.Duration, 0, nQueries)
	for i := 0; i < nQueries; i++ {
		q := words[i%len(words)]
		start := time.Now()
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/search?q="+q, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		lats = append(lats, time.Since(start))
	}
	return lats
}

func benchStorageOverhead(root string, totalFiles int) {
	var contentSize, kiwiSize, gitSize int64
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		size := info.Size()
		switch {
		case strings.HasPrefix(rel, ".git"+string(filepath.Separator)):
			gitSize += size
		case strings.HasPrefix(rel, ".kiwi"+string(filepath.Separator)):
			kiwiSize += size
		default:
			contentSize += size
		}
		return nil
	})

	overhead := float64(kiwiSize+gitSize) / float64(max(contentSize, 1)) * 100
	fmt.Println("\n| Storage | Size |")
	fmt.Println("|---------|------|")
	fmt.Printf("| Raw content (%d files) | %s |\n", totalFiles, humanSize(contentSize))
	fmt.Printf("| .kiwi/ (index) | %s |\n", humanSize(kiwiSize))
	fmt.Printf("| .git/ | %s |\n", humanSize(gitSize))
	fmt.Printf("| Overhead | %.0f%% |\n", overhead)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = gcRepo(ctx, root)

	var gitSizeAfterGC int64
	_ = filepath.Walk(filepath.Join(root, ".git"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		gitSizeAfterGC += info.Size()
		return nil
	})
	fmt.Printf("| .git/ after gc | %s |\n", humanSize(gitSizeAfterGC))
}

func gcRepo(ctx context.Context, root string) error {
	cmd := execCommand(ctx, root, "git", "gc")
	return cmd.Run()
}

func execCommand(ctx context.Context, dir, name string, args ...string) *osexec.Cmd {
	cmd := osexec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	return cmd
}

func printLatencyTable(label string, lats []time.Duration) {
	if len(lats) == 0 {
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	median := lats[len(lats)/2]
	p95 := lats[int(float64(len(lats))*0.95)]
	mn := lats[0]
	mx := lats[len(lats)-1]
	fmt.Printf("\n| %s | Value |\n", label)
	fmt.Println("|---|---|")
	fmt.Printf("| Median | %.2fms |\n", ms(median))
	fmt.Printf("| p95 | %.2fms |\n", ms(p95))
	fmt.Printf("| Min | %.2fms |\n", ms(mn))
	fmt.Printf("| Max | %.2fms |\n", ms(mx))
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func humanSize(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
