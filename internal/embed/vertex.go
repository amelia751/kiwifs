package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Vertex calls Google Vertex AI's text-embedding endpoint. Auth uses Google
// Application Default Credentials — either the GOOGLE_APPLICATION_CREDENTIALS
// env var, a service-account JSON path passed in config, or the ambient
// credentials on a GCE/GKE/Cloud Run workload.
type Vertex struct {
	client   *http.Client
	project  string
	location string
	model    string
	dims     int
}

// NewVertex creates a Vertex AI embedder. project and location are required;
// model defaults to text-embedding-004. If credentialsFile is non-empty, it's
// read and used; otherwise ADC is consulted.
func NewVertex(ctx context.Context, project, location, model, credentialsFile string, dims int) (*Vertex, error) {
	if project == "" {
		return nil, fmt.Errorf("vertex embedder: project is required")
	}
	if location == "" {
		location = "us-central1"
	}
	if model == "" {
		model = "text-embedding-004"
	}
	if dims <= 0 {
		dims = vertexDims(model)
	}

	scope := "https://www.googleapis.com/auth/cloud-platform"
	var ts oauth2.TokenSource
	if credentialsFile != "" {
		raw, err := os.ReadFile(credentialsFile)
		if err != nil {
			return nil, fmt.Errorf("vertex embedder: read credentials: %w", err)
		}
		creds, err := google.CredentialsFromJSON(ctx, raw, scope)
		if err != nil {
			return nil, fmt.Errorf("vertex embedder: parse credentials: %w", err)
		}
		ts = creds.TokenSource
	} else {
		creds, err := google.FindDefaultCredentials(ctx, scope)
		if err != nil {
			return nil, fmt.Errorf("vertex embedder: no application default credentials: %w", err)
		}
		ts = creds.TokenSource
	}

	return &Vertex{
		client:   oauth2.NewClient(context.Background(), ts),
		project:  project,
		location: location,
		model:    model,
		dims:     dims,
	}, nil
}

func (e *Vertex) Dimensions() int { return e.dims }

func (e *Vertex) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	instances := make([]map[string]string, len(texts))
	for i, t := range texts {
		instances[i] = map[string]string{"content": t}
	}
	body, err := json.Marshal(map[string]any{"instances": instances})
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf(
		"https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		e.location, e.project, e.location, e.model,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vertex embed: %s: %s", resp.Status, truncate(string(raw), 200))
	}
	var parsed struct {
		Predictions []struct {
			Embeddings struct {
				Values []float32 `json:"values"`
			} `json:"embeddings"`
		} `json:"predictions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("vertex embed: decode: %w", err)
	}
	out := make([][]float32, len(parsed.Predictions))
	for i, p := range parsed.Predictions {
		out[i] = p.Embeddings.Values
	}
	if e.dims == 0 && len(out) > 0 {
		e.dims = len(out[0])
	}
	return out, nil
}

func vertexDims(model string) int {
	switch model {
	case "text-embedding-004", "text-embedding-005":
		return 768
	case "text-multilingual-embedding-002":
		return 768
	case "gemini-embedding-001":
		return 3072
	default:
		return 768
	}
}
