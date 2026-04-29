package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/kiwifs/kiwifs/internal/importer"
	"github.com/labstack/echo/v4"
)

type importRequest struct {
	From       string   `json:"from"`
	DSN        string   `json:"dsn"`
	URI        string   `json:"uri"`
	DB         string   `json:"db"`
	File       string   `json:"file"`
	Table      string   `json:"table"`
	Collection string   `json:"collection"`
	Database   string   `json:"database"`
	DatabaseID string   `json:"database_id"`
	BaseID     string   `json:"base_id"`
	TableID    string   `json:"table_id"`
	Project    string   `json:"project"`
	Query      string   `json:"query"`
	Columns    []string `json:"columns"`
	IDColumn   string   `json:"id_column"`
	Prefix     string   `json:"prefix"`
	DryRun     bool     `json:"dry_run"`
	Limit      int      `json:"limit"`
}

type importResponse struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors"`
}

func (h *Handlers) Import(c echo.Context) error {
	var req importRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if req.From == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "from is required")
	}

	src, err := buildAPISource(req)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	defer src.Close()

	var columns []string
	if len(req.Columns) > 0 {
		columns = req.Columns
	}

	actor := sanitizeActor(c.Request().Header.Get("X-Actor"))
	if actor == "anonymous" {
		actor = "api-import"
	}

	opts := importer.Options{
		Prefix:   req.Prefix,
		IDColumn: req.IDColumn,
		Columns:  columns,
		DryRun:   req.DryRun,
		Limit:    req.Limit,
		Actor:    actor,
	}

	ctx := c.Request().Context()
	stats, err := importer.Run(ctx, src, h.pipe, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("import failed: %v", err))
	}

	return c.JSON(http.StatusOK, importResponse{
		Imported: stats.Imported,
		Skipped:  stats.Skipped,
		Errors:   stats.Errors,
	})
}

func buildAPISource(req importRequest) (importer.Source, error) {
	switch req.From {
	case "postgres":
		if req.DSN == "" {
			return nil, fmt.Errorf("dsn is required for postgres")
		}
		if req.Table == "" && req.Query == "" {
			return nil, fmt.Errorf("table or query is required for postgres")
		}
		return importer.NewPostgres(req.DSN, req.Table, req.Query, req.Columns)
	case "mysql":
		if req.DSN == "" {
			return nil, fmt.Errorf("dsn is required for mysql")
		}
		if req.Table == "" && req.Query == "" {
			return nil, fmt.Errorf("table or query is required for mysql")
		}
		return importer.NewMySQL(req.DSN, req.Table, req.Query, req.Columns)
	case "firestore":
		if req.Project == "" {
			return nil, fmt.Errorf("project is required for firestore")
		}
		if req.Collection == "" {
			return nil, fmt.Errorf("collection is required for firestore")
		}
		return importer.NewFirestore(req.Project, req.Collection)
	case "sqlite":
		if req.DB == "" {
			return nil, fmt.Errorf("db is required for sqlite")
		}
		if req.Table == "" && req.Query == "" {
			return nil, fmt.Errorf("table or query is required for sqlite")
		}
		return importer.NewSQLiteSource(req.DB, req.Table, req.Query)
	case "mongodb":
		uri := req.URI
		if uri == "" {
			uri = req.DSN
		}
		if uri == "" {
			return nil, fmt.Errorf("uri is required for mongodb")
		}
		if req.Collection == "" {
			return nil, fmt.Errorf("collection is required for mongodb")
		}
		db := req.Database
		if db == "" {
			return nil, fmt.Errorf("database is required for mongodb")
		}
		return importer.NewMongoDB(uri, db, req.Collection)
	case "csv":
		if req.File == "" {
			return nil, fmt.Errorf("file is required for csv")
		}
		return importer.NewCSV(req.File, true)
	case "json", "jsonl":
		if req.File == "" {
			return nil, fmt.Errorf("file is required for json/jsonl")
		}
		return importer.NewJSON(req.File)
	case "notion":
		apiKey := os.Getenv("NOTION_API_KEY")
		if req.DatabaseID == "" {
			return nil, fmt.Errorf("database_id is required for notion")
		}
		return importer.NewNotion(apiKey, req.DatabaseID)
	case "airtable":
		apiKey := os.Getenv("AIRTABLE_API_KEY")
		if req.BaseID == "" {
			return nil, fmt.Errorf("base_id is required for airtable")
		}
		if req.TableID == "" {
			return nil, fmt.Errorf("table_id is required for airtable")
		}
		return importer.NewAirtable(apiKey, req.BaseID, req.TableID)
	default:
		supported := strings.Join([]string{"postgres", "mysql", "firestore", "sqlite", "mongodb", "csv", "json", "jsonl", "notion", "airtable"}, ", ")
		return nil, fmt.Errorf("unknown source type %q (supported: %s)", req.From, supported)
	}
}

// RunImport is used by the MCP tool to trigger an import programmatically.
func RunImport(ctx context.Context, req importRequest, pipe interface {
	Write(ctx context.Context, path string, content []byte, actor string) (interface{ ETag() string }, error)
}) (*importResponse, error) {
	return nil, fmt.Errorf("use the REST API for import")
}
