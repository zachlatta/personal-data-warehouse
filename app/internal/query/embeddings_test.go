package query

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	vector, err := client.Embed(context.Background(), "offer letter")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
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
	vector, err := client.Embed(context.Background(), "q")
	if err != nil {
		t.Fatalf("Embed after retry: %v", err)
	}
	if len(vector) != 2 {
		t.Fatalf("vector = %v", vector)
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
