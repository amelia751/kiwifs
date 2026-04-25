package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/importer"
	"github.com/spf13/cobra"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from external sources into the knowledge base",
	Example: `  kiwifs import --from csv --file students.csv
  kiwifs import --from json --file data.json
  kiwifs import --from jsonl --file data.jsonl
  kiwifs import --from yaml --file data.yaml
  kiwifs import --from excel --file students.xlsx --sheet "Sheet1"
  kiwifs import --from sqlite --db /path/to/data.db --table students
  kiwifs import --from postgres --dsn "postgres://user:pass@host/db" --table students
  kiwifs import --from mysql --dsn "user:pass@tcp(host)/db" --table students
  kiwifs import --from firestore --project my-project --collection students
  kiwifs import --from mongodb --uri "mongodb://host/db" --database mydb --collection students
  kiwifs import --from notion --database-id abc123
  kiwifs import --from airtable --base-id appXXX --table-id tblYYY
  kiwifs import --from gsheets --spreadsheet-id "1BxiM..." --sheet "Sheet1" --credentials creds.json
  kiwifs import --from obsidian --path /path/to/vault
  kiwifs import --from confluence --path /path/to/export
  kiwifs import --from dynamodb --region us-east-1 --table students
  kiwifs import --from redis --addr localhost:6379 --pattern "students:*"
  kiwifs import --from elasticsearch --url http://localhost:9200 --index students`,
	RunE: runImport,
}

func init() {
	rootCmd.AddCommand(importCmd)

	importCmd.Flags().String("from", "", "source type: postgres, mysql, firestore, sqlite, mongodb, csv, json, jsonl, yaml, excel, notion, airtable, gsheets, obsidian, confluence, dynamodb, redis, elasticsearch")
	importCmd.MarkFlagRequired("from")

	importCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	importCmd.Flags().String("dsn", "", "database connection string (postgres, mysql)")
	importCmd.Flags().String("uri", "", "connection URI (mongodb)")
	importCmd.Flags().String("db", "", "database file path (sqlite)")
	importCmd.Flags().String("file", "", "file path (csv, json, jsonl, yaml, excel)")
	importCmd.Flags().String("table", "", "table name (postgres, mysql, sqlite, dynamodb)")
	importCmd.Flags().String("collection", "", "collection name (firestore, mongodb)")
	importCmd.Flags().String("database", "", "database name (mongodb)")
	importCmd.Flags().String("database-id", "", "database ID (notion)")
	importCmd.Flags().String("base-id", "", "base ID (airtable)")
	importCmd.Flags().String("table-id", "", "table ID (airtable)")
	importCmd.Flags().String("project", "", "GCP project ID (firestore)")
	importCmd.Flags().String("query", "", "custom SQL query (overrides --table)")
	importCmd.Flags().String("columns", "", "comma-separated list of columns to include")
	importCmd.Flags().String("id-column", "", "column to use as filename (default: auto-detect)")
	importCmd.Flags().String("prefix", "", "path prefix in kiwifs (default: table/collection name)")
	importCmd.Flags().Bool("dry-run", false, "show what would be imported without writing")
	importCmd.Flags().Int("limit", 0, "max rows to import (0 = unlimited)")
	importCmd.Flags().String("sheet", "", "sheet name (excel, gsheets)")
	importCmd.Flags().String("spreadsheet-id", "", "Google Spreadsheet ID (gsheets)")
	importCmd.Flags().String("credentials", "", "credentials file path (gsheets)")
	importCmd.Flags().String("path", "", "directory path (obsidian, confluence)")
	importCmd.Flags().String("region", "", "AWS region (dynamodb)")
	importCmd.Flags().String("addr", "", "server address (redis)")
	importCmd.Flags().String("password", "", "auth password (redis)")
	importCmd.Flags().Int("redis-db", 0, "Redis database number")
	importCmd.Flags().String("pattern", "", "key pattern (redis)")
	importCmd.Flags().String("url", "", "server URL (elasticsearch)")
	importCmd.Flags().String("index", "", "index name (elasticsearch)")
}

func runImport(cmd *cobra.Command, _ []string) error {
	from, _ := cmd.Flags().GetString("from")
	root, _ := cmd.Flags().GetString("root")

	src, err := buildSource(cmd, from)
	if err != nil {
		return err
	}
	defer src.Close()

	cfg, err := config.Load(root)
	if err != nil {
		cfg = &config.Config{}
	}
	cfg.Storage.Root = root

	stack, err := bootstrap.Build("import", root, cfg)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	defer stack.Close()

	columnsStr, _ := cmd.Flags().GetString("columns")
	var columns []string
	if columnsStr != "" {
		columns = strings.Split(columnsStr, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	prefix, _ := cmd.Flags().GetString("prefix")
	idColumn, _ := cmd.Flags().GetString("id-column")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	limit, _ := cmd.Flags().GetInt("limit")

	opts := importer.Options{
		Prefix:   prefix,
		IDColumn: idColumn,
		Columns:  columns,
		DryRun:   dryRun,
		Limit:    limit,
		Actor:    "import",
	}

	ctx := cmd.Context()
	stats, err := importer.Run(ctx, src, stack.Pipeline, opts)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	if dryRun {
		fmt.Printf("Dry run: would import %d records\n", stats.Imported)
	} else {
		fmt.Printf("Imported %d records, skipped %d\n", stats.Imported, stats.Skipped)
	}
	if len(stats.Errors) > 0 {
		fmt.Fprintf(os.Stderr, "%d errors:\n", len(stats.Errors))
		for _, e := range stats.Errors {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
	}
	return nil
}

func buildSource(cmd *cobra.Command, from string) (importer.Source, error) {
	switch from {
	case "postgres":
		dsn, _ := cmd.Flags().GetString("dsn")
		table, _ := cmd.Flags().GetString("table")
		query, _ := cmd.Flags().GetString("query")
		columnsStr, _ := cmd.Flags().GetString("columns")
		var cols []string
		if columnsStr != "" {
			cols = strings.Split(columnsStr, ",")
		}
		if dsn == "" {
			return nil, fmt.Errorf("--dsn is required for postgres")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("--table or --query is required for postgres")
		}
		return importer.NewPostgres(dsn, table, query, cols)

	case "mysql":
		dsn, _ := cmd.Flags().GetString("dsn")
		table, _ := cmd.Flags().GetString("table")
		query, _ := cmd.Flags().GetString("query")
		columnsStr, _ := cmd.Flags().GetString("columns")
		var cols []string
		if columnsStr != "" {
			cols = strings.Split(columnsStr, ",")
		}
		if dsn == "" {
			return nil, fmt.Errorf("--dsn is required for mysql")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("--table or --query is required for mysql")
		}
		return importer.NewMySQL(dsn, table, query, cols)

	case "firestore":
		project, _ := cmd.Flags().GetString("project")
		collection, _ := cmd.Flags().GetString("collection")
		if project == "" {
			return nil, fmt.Errorf("--project is required for firestore")
		}
		if collection == "" {
			return nil, fmt.Errorf("--collection is required for firestore")
		}
		return importer.NewFirestore(project, collection)

	case "sqlite":
		dbPath, _ := cmd.Flags().GetString("db")
		table, _ := cmd.Flags().GetString("table")
		query, _ := cmd.Flags().GetString("query")
		if dbPath == "" {
			return nil, fmt.Errorf("--db is required for sqlite")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("--table or --query is required for sqlite")
		}
		return importer.NewSQLiteSource(dbPath, table, query)

	case "mongodb":
		uri, _ := cmd.Flags().GetString("uri")
		database, _ := cmd.Flags().GetString("database")
		collection, _ := cmd.Flags().GetString("collection")
		if uri == "" {
			return nil, fmt.Errorf("--uri is required for mongodb")
		}
		if collection == "" {
			return nil, fmt.Errorf("--collection is required for mongodb")
		}
		if database == "" {
			return nil, fmt.Errorf("--database is required for mongodb")
		}
		return importer.NewMongoDB(uri, database, collection)

	case "csv":
		filePath, _ := cmd.Flags().GetString("file")
		if filePath == "" {
			return nil, fmt.Errorf("--file is required for csv")
		}
		return importer.NewCSV(filePath, true)

	case "json", "jsonl":
		filePath, _ := cmd.Flags().GetString("file")
		if filePath == "" {
			return nil, fmt.Errorf("--file is required for json/jsonl")
		}
		return importer.NewJSON(filePath)

	case "notion":
		apiKey := os.Getenv("NOTION_API_KEY")
		databaseID, _ := cmd.Flags().GetString("database-id")
		if databaseID == "" {
			return nil, fmt.Errorf("--database-id is required for notion")
		}
		return importer.NewNotion(apiKey, databaseID)

	case "airtable":
		apiKey := os.Getenv("AIRTABLE_API_KEY")
		baseID, _ := cmd.Flags().GetString("base-id")
		tableID, _ := cmd.Flags().GetString("table-id")
		if baseID == "" {
			return nil, fmt.Errorf("--base-id is required for airtable")
		}
		if tableID == "" {
			return nil, fmt.Errorf("--table-id is required for airtable")
		}
		return importer.NewAirtable(apiKey, baseID, tableID)

	case "gsheets":
		spreadsheetID, _ := cmd.Flags().GetString("spreadsheet-id")
		sheet, _ := cmd.Flags().GetString("sheet")
		credentials, _ := cmd.Flags().GetString("credentials")
		if spreadsheetID == "" {
			return nil, fmt.Errorf("--spreadsheet-id is required for gsheets")
		}
		return importer.NewGoogleSheets(spreadsheetID, sheet, credentials)

	case "excel":
		filePath, _ := cmd.Flags().GetString("file")
		sheet, _ := cmd.Flags().GetString("sheet")
		if filePath == "" {
			return nil, fmt.Errorf("--file is required for excel")
		}
		return importer.NewExcel(filePath, sheet)

	case "yaml":
		filePath, _ := cmd.Flags().GetString("file")
		if filePath == "" {
			return nil, fmt.Errorf("--file is required for yaml")
		}
		return importer.NewYAML(filePath)

	case "obsidian":
		path, _ := cmd.Flags().GetString("path")
		if path == "" {
			return nil, fmt.Errorf("--path is required for obsidian")
		}
		return importer.NewObsidian(path)

	case "confluence":
		path, _ := cmd.Flags().GetString("path")
		if path == "" {
			return nil, fmt.Errorf("--path is required for confluence")
		}
		return importer.NewConfluence(path)

	case "dynamodb":
		region, _ := cmd.Flags().GetString("region")
		table, _ := cmd.Flags().GetString("table")
		if region == "" {
			return nil, fmt.Errorf("--region is required for dynamodb")
		}
		if table == "" {
			return nil, fmt.Errorf("--table is required for dynamodb")
		}
		return importer.NewDynamoDB(region, table)

	case "redis":
		addr, _ := cmd.Flags().GetString("addr")
		password, _ := cmd.Flags().GetString("password")
		redisDB, _ := cmd.Flags().GetInt("redis-db")
		pattern, _ := cmd.Flags().GetString("pattern")
		if addr == "" {
			return nil, fmt.Errorf("--addr is required for redis")
		}
		return importer.NewRedis(addr, password, redisDB, pattern)

	case "elasticsearch":
		esURL, _ := cmd.Flags().GetString("url")
		index, _ := cmd.Flags().GetString("index")
		if esURL == "" {
			return nil, fmt.Errorf("--url is required for elasticsearch")
		}
		if index == "" {
			return nil, fmt.Errorf("--index is required for elasticsearch")
		}
		return importer.NewElasticsearch(esURL, index, nil)

	default:
		return nil, fmt.Errorf("unknown source type: %s (supported: postgres, mysql, firestore, sqlite, mongodb, csv, json, jsonl, yaml, excel, notion, airtable, gsheets, obsidian, confluence, dynamodb, redis, elasticsearch)", from)
	}
}
