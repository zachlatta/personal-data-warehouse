package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Embeddings defaults. The dimensions default matches the pgvector column
// timeline.search_hybrid indexes, so a deployment that overrides
// SEARCH_EMBEDDINGS_DIMENSIONS must also rebuild the vector corpus.
const (
	DefaultEmbeddingsBaseURL    = "https://api.openai.com/v1"
	DefaultEmbeddingsModel      = "text-embedding-3-small"
	DefaultEmbeddingsDimensions = 512
)

const embeddingsRequestTimeout = 30 * time.Second

// Embedder turns a query string into the embedding vector timeline.search_hybrid
// compares against. *EmbeddingsClient implements it; tests substitute fakes.
type Embedder interface {
	Model() string
	Embed(ctx context.Context, text string) ([]float64, error)
}

// EmbeddingsOptions configures an OpenAI-compatible embeddings endpoint.
type EmbeddingsOptions struct {
	// BaseURL is the API root (the client POSTs to BaseURL + "/embeddings").
	// Empty means DefaultEmbeddingsBaseURL; setting it explicitly counts as
	// "configured" even without an API key, because self-hosted embedding
	// servers need no key.
	BaseURL string
	// APIKey is sent as a bearer token when set.
	APIKey string
	// Model defaults to DefaultEmbeddingsModel.
	Model string
	// Dimensions defaults to DefaultEmbeddingsDimensions. The response vector
	// must come back with exactly this many dimensions.
	Dimensions int
	// QueryPrefix is prepended to every text before embedding. Instruction-
	// tuned retrieval models (Qwen3-Embedding) embed documents raw but expect
	// queries wrapped in a task instruction; this client only ever embeds
	// queries, so the prefix applies to everything it sends.
	QueryPrefix string
	// QueryRawWeight blends the raw-query embedding with the prefixed-query
	// embedding when QueryPrefix is set. Zero uses only the instructed vector;
	// 0.5 gives each representation equal weight. Values outside [0,1] are
	// clamped defensively; application config rejects them before this layer.
	QueryRawWeight float64
	// HTTPClient overrides the default 30s-timeout client (tests).
	HTTPClient *http.Client
}

// EmbeddingsClient calls an OpenAI-compatible POST /embeddings endpoint.
type EmbeddingsClient struct {
	baseURL        string
	apiKey         string
	model          string
	dimensions     int
	queryPrefix    string
	queryRawWeight float64
	httpClient     *http.Client
}

// NewEmbeddingsClient returns a client, or nil when embeddings are not
// configured (no API key and no explicit base URL). Callers treat a nil client
// as "hybrid search unavailable" and fall back to keyword search.
func NewEmbeddingsClient(opts EmbeddingsOptions) *EmbeddingsClient {
	baseURL := strings.TrimSpace(opts.BaseURL)
	apiKey := strings.TrimSpace(opts.APIKey)
	if baseURL == "" && apiKey == "" {
		return nil
	}
	if baseURL == "" {
		baseURL = DefaultEmbeddingsBaseURL
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = DefaultEmbeddingsModel
	}
	dimensions := opts.Dimensions
	if dimensions <= 0 {
		dimensions = DefaultEmbeddingsDimensions
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: embeddingsRequestTimeout}
	}
	queryRawWeight := opts.QueryRawWeight
	if math.IsNaN(queryRawWeight) || queryRawWeight < 0 {
		queryRawWeight = 0
	} else if queryRawWeight > 1 {
		queryRawWeight = 1
	}
	return &EmbeddingsClient{
		baseURL:        strings.TrimRight(baseURL, "/"),
		apiKey:         apiKey,
		model:          model,
		dimensions:     dimensions,
		queryPrefix:    opts.QueryPrefix,
		queryRawWeight: queryRawWeight,
		httpClient:     httpClient,
	}
}

func (c *EmbeddingsClient) Model() string { return c.model }

// Embed returns the embedding vector for text. It retries once on a 429 or
// 5xx response; every other failure is returned immediately.
func (c *EmbeddingsClient) Embed(ctx context.Context, text string) ([]float64, error) {
	inputs := []string{c.queryPrefix + text}
	blend := c.queryPrefix != "" && c.queryRawWeight > 0 && c.queryRawWeight < 1
	if blend {
		inputs = append(inputs, text)
	} else if c.queryPrefix != "" && c.queryRawWeight >= 1 {
		inputs[0] = text
	}
	body, err := json.Marshal(map[string]any{
		"model":      c.model,
		"input":      inputs,
		"dimensions": c.dimensions,
	})
	if err != nil {
		return nil, fmt.Errorf("encode embeddings request: %w", err)
	}
	vectors, retryable, err := c.embedOnce(ctx, body)
	if err != nil && retryable {
		time.Sleep(500 * time.Millisecond)
		vectors, _, err = c.embedOnce(ctx, body)
	}
	if err != nil {
		return nil, err
	}
	if len(vectors) != len(inputs) {
		return nil, fmt.Errorf("embeddings response carried %d data entries for %d inputs", len(vectors), len(inputs))
	}
	for index := range vectors {
		vectors[index], err = c.fitDimensions(vectors[index])
		if err != nil {
			return nil, err
		}
	}
	if blend {
		return blendNormalized(vectors[0], vectors[1], c.queryRawWeight), nil
	}
	return vectors[0], nil
}

// embedOnce performs a single POST /embeddings round trip. retryable reports
// whether the failure is worth one more attempt (rate limit or server error).
func (c *EmbeddingsClient) embedOnce(ctx context.Context, body []byte) (vectors [][]float64, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("build embeddings request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Transport failures (refused/reset dials, transient tailnet blips)
		// are exactly as retryable as a 503 — returning them as terminal made
		// half an eval run fall back to keyword over momentary dial errors.
		return nil, true, fmt.Errorf("embeddings request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, false, fmt.Errorf("read embeddings response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return nil, retryable, fmt.Errorf("embeddings endpoint returned status %d: %s", resp.StatusCode, truncateForError(string(respBody)))
	}
	var decoded struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return nil, false, fmt.Errorf("decode embeddings response: %w", err)
	}
	if len(decoded.Data) == 0 {
		return nil, false, fmt.Errorf("embeddings response carried no data entries")
	}
	sort.SliceStable(decoded.Data, func(i, j int) bool { return decoded.Data[i].Index < decoded.Data[j].Index })
	vectors = make([][]float64, 0, len(decoded.Data))
	for _, item := range decoded.Data {
		vectors = append(vectors, item.Embedding)
	}
	return vectors, false, nil
}

func (c *EmbeddingsClient) fitDimensions(embedding []float64) ([]float64, error) {
	if len(embedding) < c.dimensions {
		return nil, fmt.Errorf("embeddings endpoint returned a %d-dimension vector, want %d; SEARCH_EMBEDDINGS_DIMENSIONS must match the model's output (and the pgvector column timeline.search_hybrid indexes)", len(embedding), c.dimensions)
	}
	if len(embedding) > c.dimensions {
		// Servers that ignore the `dimensions` request parameter (several
		// self-hosted backends) return the model's native width. For
		// Matryoshka-trained models prefix truncation + L2 renormalization is
		// the defined way to shorten, and the Python indexing runner does the
		// same, so query and document vectors stay in the same space.
		embedding = embedding[:c.dimensions]
		embedding = normalize(embedding)
	}
	return embedding, nil
}

func blendNormalized(instructed, raw []float64, rawWeight float64) []float64 {
	instructed = normalize(instructed)
	raw = normalize(raw)
	result := make([]float64, len(instructed))
	for index := range instructed {
		result[index] = (1-rawWeight)*instructed[index] + rawWeight*raw[index]
	}
	return normalize(result)
}

func normalize(vector []float64) []float64 {
	var norm float64
	for _, value := range vector {
		norm += value * value
	}
	if norm == 0 {
		norm = 1
	} else {
		norm = math.Sqrt(norm)
	}
	normalized := make([]float64, len(vector))
	for index, value := range vector {
		normalized[index] = value / norm
	}
	return normalized
}

func truncateForError(body string) string {
	body = strings.TrimSpace(body)
	const maxChars = 300
	if len(body) > maxChars {
		return body[:maxChars] + "..."
	}
	return body
}

// VectorLiteral renders a vector in pgvector's input syntax, e.g. "[0.1,-0.2]",
// suitable for binding as the text argument timeline.search_hybrid casts to
// vector.
func VectorLiteral(vector []float64) string {
	var out strings.Builder
	out.Grow(len(vector)*10 + 2)
	out.WriteByte('[')
	for i, value := range vector {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	}
	out.WriteByte(']')
	return out.String()
}
