package main

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func withFakeMutationWorker(t *testing.T, provider string, code int) *capturedLocal {
	t.Helper()
	prev, ok := mutationWorkers[provider]
	if !ok {
		t.Fatalf("provider %q is not in the mutations table", provider)
	}
	cap := &capturedLocal{}
	mutationWorkers[provider] = fakeLocal(cap, code)
	t.Cleanup(func() { mutationWorkers[provider] = prev })
	return cap
}

func TestMutationsTableIsTheTwoLocalOnlyProviders(t *testing.T) {
	if got := mutationWorkerNames(); !reflect.DeepEqual(got, []string{"apple-contacts", "apple-notes"}) {
		t.Fatalf("providers = %v", got)
	}
	if !sort.StringsAreSorted(mutationWorkerNames()) {
		t.Fatal("names must be sorted")
	}
}

func TestMutationsForwardsOnceFlagToTheWorker(t *testing.T) {
	cap := withFakeMutationWorker(t, "apple-notes", 0)
	var out, errBuf bytes.Buffer
	code := run([]string{"mutations", "apple-notes", "--once"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil))
	if code != 0 || !cap.called || !reflect.DeepEqual(cap.args, []string{"--once"}) {
		t.Fatalf("exit=%d called=%v args=%v stderr=%s", code, cap.called, cap.args, errBuf.String())
	}
}

func TestMutationsPropagatesExitCodeAndConfig(t *testing.T) {
	cap := withFakeMutationWorker(t, "apple-contacts", 1)
	var out, errBuf bytes.Buffer
	code := runMutations([]string{"apple-contacts"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "https://w.example", "tok")
	if code != 1 || cap.cfg.BaseURL != "https://w.example" || cap.cfg.Token != "tok" {
		t.Fatalf("exit=%d cfg=%+v", code, cap.cfg)
	}
}

func TestMutationsUnknownProviderAndHelp(t *testing.T) {
	cap := withFakeMutationWorker(t, "apple-notes", 0)
	var out, errBuf bytes.Buffer
	if code := runMutations([]string{"gmail"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 2 || cap.called {
		t.Fatalf("unknown provider exit=%d called=%v", code, cap.called)
	}
	if !strings.Contains(errBuf.String(), "gmail") || !strings.Contains(errBuf.String(), "apple-notes") {
		t.Fatalf("stderr = %s", errBuf.String())
	}
	errBuf.Reset()
	if code := runMutations(nil, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 2 || cap.called {
		t.Fatalf("no provider exit=%d called=%v", code, cap.called)
	}
	out.Reset()
	if code := runMutations([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 0 || cap.called {
		t.Fatalf("--help exit=%d called=%v", code, cap.called)
	}
	if !strings.Contains(out.String(), "--once") || !strings.Contains(out.String(), "apple-contacts") {
		t.Fatalf("help: %s", out.String())
	}
}

func TestRealMutationWorkersAnswerHelpAndRefuseUnknownFlags(t *testing.T) {
	for name, run := range mutationWorkers {
		var out, errBuf bytes.Buffer
		if code := run([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), resolveLocalConfig(isolatedEnv(t, nil), "", "")); code != 0 {
			t.Fatalf("%s --help exit=%d", name, code)
		}
		if !strings.Contains(out.String(), "--once") {
			t.Fatalf("%s help lacks --once: %s", name, out.String())
		}
		if code := run([]string{"--bogus"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), resolveLocalConfig(isolatedEnv(t, nil), "", "")); code != 2 {
			t.Fatalf("%s --bogus exit=%d", name, code)
		}
	}
}
