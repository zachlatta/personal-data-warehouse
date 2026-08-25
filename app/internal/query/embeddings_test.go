package query

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

func embeddingsResponseBody(t *testing.T, vector []float64) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"data": []map[string]any{{"embedding": vector}},
	})
	if err != nil {
		t.Fatalf("marshal embeddings response: %v", err)
	}
	return body
}

func TestNewEmbeddingsClientRequiresKeyOrExplicitBaseURL(t *testing.T) {
	if client := NewEmbeddingsClient(EmbeddingsOptions{}); client != nil {
		t.Fatalf("expected nil client with no key and no base URL, got %#v", client)
	}
	if client := NewEmbeddingsClient(EmbeddingsOptions{APIKey: "sk-test"}); client == nil {
		t.Fatal("expected client when only the API key is set")
	}
	// A self-hosted embeddings server needs no key, so an explicit base URL
	// alone must count as configured.
	if client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: "http://localhost:11434/v1"}); client == nil {
		t.Fatal("expected client when only the base URL is set")
	}
}

func TestEmbeddingsClientEmbedSendsOpenAICompatibleRequest(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		_, _ = w.Write(embeddingsResponseBody(t, []float64{0.5, -1.25, 0}))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, APIKey: "sk-test", Model: "test-model", Dimensions: 3})
	vectors, err := client.Embed(context.Background(), "offer letter")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vectors) != 1 {
		t.Fatalf("want one vector without a query prefix, got %d", len(vectors))
	}
	vector := vectors[0]
	if len(vector) != 3 || vector[0] != 0.5 || vector[1] != -1.25 || vector[2] != 0 {
		t.Fatalf("vector = %v", vector)
	}
	if gotPath != "/embeddings" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer sk-test" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotBody["model"] != "test-model" {
		t.Fatalf("model = %v", gotBody["model"])
	}
	if inputs, ok := gotBody["input"].([]any); !ok || len(inputs) != 1 || inputs[0] != "offer letter" {
		t.Fatalf("input = %v", gotBody["input"])
	}
	if gotBody["dimensions"] != float64(3) {
		t.Fatalf("dimensions = %v", gotBody["dimensions"])
	}
}

func TestEmbeddingsClientReturnsInstructedAndRawQueryVectors(t *testing.T) {
	// Instruction-asymmetric models put the instructed and the raw form of one
	// question in different neighbourhoods, each holding answers the other
	// misses. The client returns BOTH so retrieval can scan both and fuse by
	// rank; blending them into one vector averaged the difference away and
	// measured materially worse (MRR 0.234 vs 0.300 on the labeled benchmark).
	var gotInputs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		gotInputs = body.Input
		// Return out of order to ensure the OpenAI `index` field, rather than
		// response order, decides which vector is instructed versus raw.
		_, _ = w.Write([]byte(`{"data":[{"index":1,"embedding":[0,3]},{"index":0,"embedding":[2,0]}]}`))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{
		BaseURL:     srv.URL,
		Dimensions:  2,
		QueryPrefix: "Instruct: retrieve personal data\nQuery:",
	})
	vectors, err := client.Embed(context.Background(), "lunch plans")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vectors) != 2 {
		t.Fatalf("want the instructed and the raw vector, got %d", len(vectors))
	}
	// Instructed first: a caller that can only use one vector gets the better one.
	if vectors[0][0] != 2 || vectors[0][1] != 0 {
		t.Fatalf("instructed vector = %v", vectors[0])
	}
	if vectors[1][0] != 0 || vectors[1][1] != 3 {
		t.Fatalf("raw vector = %v", vectors[1])
	}
	if len(gotInputs) != 2 || gotInputs[0] != "Instruct: retrieve personal data\nQuery:lunch plans" || gotInputs[1] != "lunch plans" {
		t.Fatalf("inputs = %#v", gotInputs)
	}
	// One round trip, not two: both forms ride in the same batched request.
}

func TestEmbeddingsClientAlsoEmbedsTheQueriesTermBag(t *testing.T) {
	// Sentence-shaped queries are the benchmark's weak stratum. Searching the
	// instructed+raw vectors for both the original sentence and its deterministic
	// content-word form improved labeled MRR without losing hit@5/hit@10/recall.
	// All four inputs must ride in the same GPU request; only the independent ANN
	// scans are fanned out by Search.
	var gotInputs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		gotInputs = body.Input
		_, _ = w.Write([]byte(`{"data":[` +
			`{"index":0,"embedding":[1,0]},` +
			`{"index":1,"embedding":[0,1]},` +
			`{"index":2,"embedding":[2,0]},` +
			`{"index":3,"embedding":[0,2]}]}`))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{
		BaseURL:     srv.URL,
		Dimensions:  2,
		QueryPrefix: "Instruct: retrieve personal data\nQuery:",
	})
	vectors, err := client.Embed(
		context.Background(),
		"what is still owed to the vet clinic",
	)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vectors) != 4 {
		t.Fatalf("want original+term-bag instructed/raw vectors, got %d", len(vectors))
	}
	want := []string{
		"Instruct: retrieve personal data\nQuery:what is still owed to the vet clinic",
		"what is still owed to the vet clinic",
		"Instruct: retrieve personal data\nQuery:still owed vet clinic",
		"still owed vet clinic",
	}
	if !slices.Equal(gotInputs, want) {
		t.Fatalf("inputs = %#v, want %#v", gotInputs, want)
	}
}

func TestEmbeddingsClientDoesNotDuplicateAnExistingTermBag(t *testing.T) {
	var gotInputs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotInputs = body.Input
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[2]}]}`))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{
		BaseURL: srv.URL, Dimensions: 1, QueryPrefix: "Query:",
	})
	if _, err := client.Embed(context.Background(), "runway burn rate months cash remaining"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(gotInputs) != 2 {
		t.Fatalf("an existing term bag must keep two inputs, got %#v", gotInputs)
	}
}

func TestEmbeddingsClientDoesNotExpandAShortEntityQuery(t *testing.T) {
	var gotInputs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotInputs = body.Input
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[2]}]}`))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{
		BaseURL: srv.URL, Dimensions: 1, QueryPrefix: "Query:",
	})
	if _, err := client.Embed(context.Background(), "the kernel magazine"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(gotInputs) != 2 {
		t.Fatalf("short entity query must keep two inputs, got %#v", gotInputs)
	}
}

func TestEmbeddingsClientOmitsAuthorizationWithoutKey(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write(embeddingsResponseBody(t, []float64{1, 2}))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, Dimensions: 2})
	if _, err := client.Embed(context.Background(), "q"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want unset", gotAuth)
	}
}

func TestEmbeddingsClientRejectsWrongDimensionCount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(embeddingsResponseBody(t, []float64{1, 2}))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, Dimensions: 3})
	_, err := client.Embed(context.Background(), "q")
	if err == nil {
		t.Fatal("expected dimension-mismatch error")
	}
	if !strings.Contains(err.Error(), "SEARCH_EMBEDDINGS_DIMENSIONS") {
		t.Fatalf("error should name SEARCH_EMBEDDINGS_DIMENSIONS: %v", err)
	}
}

func TestEmbeddingsClientRetriesOnceOnServerError(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(embeddingsResponseBody(t, []float64{1, 2}))
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, Dimensions: 2})
	vectors, err := client.Embed(context.Background(), "q")
	if err != nil {
		t.Fatalf("Embed after retry: %v", err)
	}
	if len(vectors) != 1 || len(vectors[0]) != 2 {
		t.Fatalf("vectors = %v", vectors)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestEmbeddingsClientRetriesOnceOnRateLimitThenFails(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, Dimensions: 2})
	if _, err := client.Embed(context.Background(), "q"); err == nil {
		t.Fatal("expected error after exhausted retry")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want exactly one retry", calls.Load())
	}
}

func TestEmbeddingsClientDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	client := NewEmbeddingsClient(EmbeddingsOptions{BaseURL: srv.URL, Dimensions: 2})
	if _, err := client.Embed(context.Background(), "q"); err == nil {
		t.Fatal("expected error on 400")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want no retry on a 4xx", calls.Load())
	}
}

func TestEmbeddingsClientAppliesDefaults(t *testing.T) {
	client := NewEmbeddingsClient(EmbeddingsOptions{APIKey: "sk-test"})
	if client.baseURL != DefaultEmbeddingsBaseURL {
		t.Fatalf("baseURL = %q", client.baseURL)
	}
	if client.Model() != DefaultEmbeddingsModel {
		t.Fatalf("model = %q", client.Model())
	}
	if client.dimensions != DefaultEmbeddingsDimensions {
		t.Fatalf("dimensions = %d", client.dimensions)
	}
}

func TestVectorLiteralRendersPgvectorInput(t *testing.T) {
	got := VectorLiteral([]float64{0.5, -1.25, 0, 3})
	if got != "[0.5,-1.25,0,3]" {
		t.Fatalf("VectorLiteral = %q", got)
	}
	if got := VectorLiteral(nil); got != "[]" {
		t.Fatalf("VectorLiteral(nil) = %q", got)
	}
}
