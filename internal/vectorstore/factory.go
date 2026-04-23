package vectorstore

import (
	"context"
	"fmt"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/embed"
	"github.com/kiwifs/kiwifs/internal/storage"
)

// Build constructs a Service from resolved config. Returns (nil, nil) when
// config.Enabled is false so callers can treat "disabled" and "missing
// config" uniformly. Any configuration error returns (nil, error). source
// is the Storage used by Reindex — the factory takes it so callers don't
// need to know anything about how vectors get refilled.
func Build(root string, source storage.Storage, cfg config.VectorConfig) (*Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	ctx := context.Background()
	embedder, err := buildEmbedder(ctx, cfg.Embedder)
	if err != nil {
		return nil, fmt.Errorf("embedder: %w", err)
	}
	store, err := buildStore(ctx, root, cfg.Store, embedder.Dimensions())
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return NewService(root, source, embedder, store, Options{
		ChunkSize:    cfg.Chunk.Size,
		ChunkOverlap: cfg.Chunk.Overlap,
	}), nil
}

func buildEmbedder(ctx context.Context, cfg config.EmbedderConfig) (embed.Embedder, error) {
	switch cfg.Provider {
	case "", "openai", "azure-openai":
		return embed.NewOpenAI(cfg.APIKey, cfg.Model, cfg.BaseURL, cfg.Dimensions)
	case "ollama":
		return embed.NewOllama(cfg.BaseURL, cfg.Model, cfg.Dimensions)
	case "http":
		return embed.NewHTTP(cfg.URL, cfg.Headers, cfg.Dimensions)
	case "cohere":
		return embed.NewCohere(cfg.APIKey, cfg.Model, cfg.BaseURL, cfg.Dimensions)
	case "bedrock":
		return embed.NewBedrock(ctx, cfg.Model, cfg.Region, cfg.Dimensions)
	case "vertex", "vertex-ai":
		return embed.NewVertex(ctx, cfg.Project, cfg.Location, cfg.Model, cfg.CredentialsFile, cfg.Dimensions)
	case "onnx":
		// The model is passed via base_url so we don't need to grow the
		// config surface just for ONNX. (Ollama and OpenAI already reuse
		// base_url as "where the inference thing lives".)
		return embed.NewONNX(cfg.BaseURL, cfg.Dimensions)
	default:
		return nil, fmt.Errorf("unknown embedder provider %q (want openai | ollama | http | cohere | bedrock | vertex | onnx)", cfg.Provider)
	}
}

func buildStore(ctx context.Context, root string, cfg config.VectorStoreConfig, dims int) (Store, error) {
	switch cfg.Provider {
	case "", "sqlite", "sqlite-vec":
		return NewSQLite(root)
	case "qdrant":
		return NewQdrant(cfg.URL, cfg.APIKey, cfg.Collection, dims)
	case "pinecone":
		return NewPinecone(cfg.URL, cfg.APIKey, cfg.Namespace, dims)
	case "weaviate":
		return NewWeaviate(cfg.URL, cfg.APIKey, cfg.Collection, dims)
	case "pgvector":
		return NewPgvector(ctx, cfg.DSN, cfg.Table, dims)
	case "milvus":
		return NewMilvus(cfg.URL, cfg.APIKey, cfg.Collection, dims)
	default:
		return nil, fmt.Errorf("unknown store provider %q (want sqlite | qdrant | pinecone | weaviate | pgvector | milvus)", cfg.Provider)
	}
}
