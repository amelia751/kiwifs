package embed

import (
	"context"
	"encoding/json"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// Bedrock calls AWS Bedrock's InvokeModel endpoint for Titan / Cohere-on-Bedrock
// embedding models. Auth uses the standard AWS credential chain (env vars,
// shared config, IAM role, IRSA, etc.) — no API key in config.
//
// Supported models:
//   - amazon.titan-embed-text-v1          (1536 dims, single input per call)
//   - amazon.titan-embed-text-v2:0        (1024 dims, single input per call)
//   - cohere.embed-english-v3             (1024 dims, batch via "texts")
//   - cohere.embed-multilingual-v3        (1024 dims, batch via "texts")
//
// Titan v1/v2 only accept one inputText at a time, so we loop client-side.
// Cohere-on-Bedrock accepts a batch; we send one call.
type Bedrock struct {
	client *bedrockruntime.Client
	model  string
	dims   int
}

// NewBedrock creates a Bedrock embedder. Region defaults to the SDK's default
// (AWS_REGION env var or shared config). Dimensions default based on the model.
func NewBedrock(ctx context.Context, model, region string, dims int) (*Bedrock, error) {
	if model == "" {
		model = "amazon.titan-embed-text-v2:0"
	}
	if dims <= 0 {
		dims = bedrockDims(model)
	}

	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("bedrock embedder: load aws config: %w", err)
	}
	return &Bedrock{
		client: bedrockruntime.NewFromConfig(cfg),
		model:  model,
		dims:   dims,
	}, nil
}

func (e *Bedrock) Dimensions() int { return e.dims }

func (e *Bedrock) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	// Cohere-on-Bedrock supports batch; Titan does not.
	if len(e.model) >= 6 && e.model[:6] == "cohere" {
		return e.embedCohereBatch(ctx, texts)
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v, err := e.embedTitanOne(ctx, t)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (e *Bedrock) embedTitanOne(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(map[string]any{"inputText": text})
	if err != nil {
		return nil, err
	}
	resp, err := e.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     &e.model,
		ContentType: ptr("application/json"),
		Accept:      ptr("application/json"),
		Body:        body,
	})
	if err != nil {
		return nil, fmt.Errorf("bedrock invoke: %w", err)
	}
	var parsed struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("bedrock decode: %w", err)
	}
	if e.dims == 0 {
		e.dims = len(parsed.Embedding)
	}
	return parsed.Embedding, nil
}

func (e *Bedrock) embedCohereBatch(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(map[string]any{
		"texts":      texts,
		"input_type": "search_document",
	})
	if err != nil {
		return nil, err
	}
	resp, err := e.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     &e.model,
		ContentType: ptr("application/json"),
		Accept:      ptr("application/json"),
		Body:        body,
	})
	if err != nil {
		return nil, fmt.Errorf("bedrock invoke: %w", err)
	}
	var parsed struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("bedrock decode: %w", err)
	}
	if e.dims == 0 && len(parsed.Embeddings) > 0 {
		e.dims = len(parsed.Embeddings[0])
	}
	return parsed.Embeddings, nil
}

func bedrockDims(model string) int {
	switch model {
	case "amazon.titan-embed-text-v1":
		return 1536
	case "amazon.titan-embed-text-v2:0":
		return 1024
	case "cohere.embed-english-v3", "cohere.embed-multilingual-v3":
		return 1024
	default:
		return 1024
	}
}

func ptr(s string) *string { return &s }
