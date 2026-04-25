package cmd

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

//go:embed all:templates
var templates embed.FS

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a knowledge directory",
	Example: `  kiwifs init --root ~/my-knowledge
  kiwifs init --root ~/my-knowledge --template knowledge
  kiwifs init --root ~/my-knowledge --template wiki`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringP("root", "r", "./knowledge", "directory to initialize")
	initCmd.Flags().String("template", "knowledge", "template: knowledge | wiki | runbook | research | blank")
}

func runInit(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	template, _ := cmd.Flags().GetString("template")

	if err := os.MkdirAll(root, 0755); err != nil {
		return fmt.Errorf("create root: %w", err)
	}

	switch template {
	case "knowledge", "wiki", "runbook", "research":
		if err := copyEmbedDir("templates/"+template, root); err != nil {
			return err
		}
	case "blank":
		// just the directory
	default:
		return fmt.Errorf("unknown template %q (want knowledge | wiki | runbook | research | blank)", template)
	}

	kiwiDir := filepath.Join(root, ".kiwi")
	if err := os.MkdirAll(kiwiDir, 0755); err != nil {
		return fmt.Errorf("create .kiwi: %w", err)
	}

	gitignorePath := filepath.Join(root, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) {
		data, _ := fs.ReadFile(templates, "templates/gitignore.txt")
		if err := os.WriteFile(gitignorePath, data, 0644); err != nil {
			return fmt.Errorf("write .gitignore: %w", err)
		}
	}

	templatesDir := filepath.Join(kiwiDir, "templates")
	if err := os.MkdirAll(templatesDir, 0755); err != nil {
		return fmt.Errorf("create .kiwi/templates: %w", err)
	}

	if err := copyEmbedDir("templates/workflow", templatesDir); err != nil {
		return err
	}

	configPath := filepath.Join(kiwiDir, "config.toml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		data, _ := fs.ReadFile(templates, "templates/config.toml")
		if err := os.WriteFile(configPath, data, 0644); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
	}

	fmt.Printf("Initialized knowledge at %s (template: %s)\n", root, template)
	fmt.Printf("Run: kiwifs serve --root %s\n", root)
	return nil
}

func copyEmbedDir(srcDir, destRoot string) error {
	return fs.WalkDir(templates, srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(path, srcDir+"/")
		if rel == srcDir {
			return nil
		}
		dest := filepath.Join(destRoot, rel)
		if d.IsDir() {
			return os.MkdirAll(dest, 0755)
		}
		if _, err := os.Stat(dest); err == nil {
			return nil
		}
		data, err := fs.ReadFile(templates, path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return err
		}
		return os.WriteFile(dest, data, 0644)
	})
}
