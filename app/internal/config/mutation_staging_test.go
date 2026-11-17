package config

import "testing"

func mutationStagingEnv(endpoint string) map[string]string {
	return map[string]string{"PDW_MUTATION_S3_ENDPOINT_URL": endpoint, "PDW_MUTATION_S3_BUCKET": "pending-uploads", "PDW_MUTATION_S3_ACCESS_KEY": "test-access", "PDW_MUTATION_S3_SECRET_KEY": "test-secret", "PDW_MUTATION_S3_REGION": "test-region"}
}

func TestMutationStagingLoadFromEnv(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv(nil))
	if err != nil || cfg.MutationStaging != nil {
		t.Fatalf("staging should be optional: %v", err)
	}
	for _, tc := range []struct {
		endpoint string
		allowed  bool
	}{
		{"https://storage.example.test", true}, {"http://100.64.0.1:3900", true}, {"http://100.127.255.254:3900", true},
		{"http://nas.example-tailnet.ts.net:3900", true}, {"http://100.63.255.255:3900", false},
		{"http://100.128.0.1:3900", false}, {"http://192.168.1.1:3900", false}, {"http://example.com", false},
		{"http://example.ts.net.attacker.test", false}, {"http://example.ts.net@attacker.test", false},
		{"http://nas.example-tailnet.ts.net/path", false}, {"http://nas.example-tailnet.ts.net?x=y", false},
	} {
		cfg, err := LoadFromEnv(baseEnv(mutationStagingEnv(tc.endpoint)))
		if (err == nil) != tc.allowed {
			t.Errorf("endpoint %s: allowed=%v err=%v", tc.endpoint, tc.allowed, err)
		}
		if err == nil && (cfg.MutationStaging.Bucket != "pending-uploads" || cfg.MutationStaging.Region != "test-region" || cfg.MutationStaging.Endpoint.String() != tc.endpoint || cfg.MutationStaging.AccessKey != "test-access" || cfg.MutationStaging.SecretKey != "test-secret") {
			t.Fatal("staging values not loaded")
		}
	}
	for name := range mutationStagingEnv("") {
		values := mutationStagingEnv("https://storage.example.test")
		delete(values, name)
		if _, err := LoadFromEnv(baseEnv(values)); err == nil {
			t.Errorf("accepted partial staging without %s", name)
		}
	}
}
