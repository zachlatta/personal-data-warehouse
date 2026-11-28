package x_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	x "github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/x"
)

func syntheticTxHTML() string {
	keyBytes := make([]byte, 48)
	for i := range keyBytes {
		keyBytes[i] = byte((i*17 + 5) % 256)
	}
	keyB64 := base64.StdEncoding.EncodeToString(keyBytes)

	rows := make([]string, 16)
	for r := 0; r < 16; r++ {
		var v [11]int
		for j := 0; j < 11; j++ {
			v[j] = (r*13 + j*29 + 11) % 256
		}
		rows[r] = fmt.Sprintf(" %d,%d %d,%d %d,%d h %d s %d,%d %d,%d",
			v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10])
	}
	pathD := "M 10,30 C" + strings.Join(rows, " C")
	var svgs strings.Builder
	for i := 0; i < 4; i++ {
		fmt.Fprintf(&svgs, `<svg id="loading-x-anim-%d"><g><path d="M0 0"></path><path d="%s"></path></g></svg>`, i, pathD)
	}
	return fmt.Sprintf(`<html><head><meta name="twitter-site-verification" content="%s"/></head><body>%s</body></html>`, keyB64, svgs.String())
}

func TestTransactionGeneratorMatchesReferenceAlgorithm(t *testing.T) {
	html := syntheticTxHTML()
	cases := []struct {
		name     string
		js       string
		wantAnim string
		wantTxID string
	}{
		{
			name:     "indices 2,3,4,7",
			js:       `var x = (e[2], 16) + (e[3], 16) + (e[4], 16) + (e[7], 16);`,
			wantAnim: "5b78950fae147ae147ae02e147ae147ae1402e147ae147ae140fae147ae147ae00",
			wantTxID: "Wl9MfWITADEm18T1mou4qV5PfG0SAzAh1sf05Yq7qFlOf2wdAjMg0cb35JW6q1hJflq7r1+FW1jbL4/nUEBDS2hPsI2lWQ",
		},
		{
			name:     "indices 1,6,8,9",
			js:       `var x = (e[1], 16) + (e[6], 16) + (e[8], 16) + (e[9], 16);`,
			wantAnim: "6784a10fae147ae147ae030a3d70a3d70a4030a3d70a3d70a40fae147ae147ae00",
			wantTxID: "Wl9MfWITADEm18T1mou4qV5PfG0SAzAh1sf05Yq7qFlOf2wdAjMg0cb35JW6q1hJflq7r18nBjB/KmvBMwiMuRzhwAnrWQ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen, err := x.NewTransactionGenerator(html, tc.js)
			if err != nil {
				t.Fatal(err)
			}
			if got := gen.AnimationKey(); got != tc.wantAnim {
				t.Fatalf("animation key = %q, want %q", got, tc.wantAnim)
			}
			got := gen.GenerateAt(http.MethodGet, "/i/api/graphql/query123/UserTweetsAndReplies", 1682924400+100000000, 0x5a)
			if got != tc.wantTxID {
				t.Fatalf("txid = %q, want %q", got, tc.wantTxID)
			}
		})
	}
}

func TestParseOndemandScriptURL(t *testing.T) {
	cases := []struct {
		name    string
		html    string
		baseURL string
		want    string
	}{
		{
			name:    "webpack chunk index map",
			html:    `{...,42:"ondemand.s",...}{...,42:"deadbeef",...}`,
			baseURL: x.DefaultBaseURL,
			want:    "https://abs.twimg.com/responsive-web/client-web/ondemand.s.deadbeefa.js",
		},
		{
			name:    "webpack chunk index map with test base URL",
			html:    `{...,42:"ondemand.s",...}{...,42:"deadbeef",...}`,
			baseURL: "http://127.0.0.1:9999",
			want:    "http://127.0.0.1:9999/responsive-web/client-web/ondemand.s.deadbeefa.js",
		},
		{
			name:    "direct ondemand map",
			html:    `{"ondemand.s":"0123abcd"}`,
			baseURL: x.DefaultBaseURL,
			want:    "https://abs.twimg.com/responsive-web/client-web/ondemand.s.0123abcda.js",
		},
		{
			name:    "explicit script URL",
			html:    `<script src="https://abs.twimg.com/responsive-web/client-web/ondemand.s.cafebabea.js"></script>`,
			baseURL: x.DefaultBaseURL,
			want:    "https://abs.twimg.com/responsive-web/client-web/ondemand.s.cafebabea.js",
		},
		{
			name:    "missing",
			html:    `<html>no ondemand script</html>`,
			baseURL: x.DefaultBaseURL,
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := x.ParseOndemandScriptURL(tc.html, tc.baseURL); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientSendsTransactionIDWhenDiscovered(t *testing.T) {
	var gotTxID string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/home":
			html := syntheticTxHTML()
			fmt.Fprintf(w, `%s<script src="%s/responsive-web/client-web/main.abc.js"></script><script>{7:"ondemand.s",7:"abcdef01"}</script>`,
				html, srv.URL)
		case "/responsive-web/client-web/main.abc.js":
			fmt.Fprintf(w, `var a="Bearer %s";e.exports={queryId:"q-replies",operationName:"UserTweetsAndReplies",operationType:"query",metadata:{featureSwitches:["f1"],fieldToggles:[]}}`, fakeBearer)
		case "/responsive-web/client-web/ondemand.s.abcdef01a.js":
			fmt.Fprint(w, `var x=(e[2], 16)+(e[3], 16)+(e[4], 16)+(e[7], 16);`)
		case "/i/api/graphql/q-replies/UserTweetsAndReplies":
			gotTxID = r.Header.Get("x-client-transaction-id")
			if gotTxID == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, `{"data":{"user":{"result":{"timeline":{"timeline":{"instructions":[]}}}}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := x.NewClient(testSession(), srv.URL, "")
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	if !c.HasTransactionID() {
		t.Fatal("expected Discover to initialize x-client-transaction-id generator")
	}
	if _, err := c.Query("UserTweetsAndReplies", map[string]any{"userId": myID}); err != nil {
		t.Fatalf("UserTweetsAndReplies should succeed with x-client-transaction-id: %v", err)
	}
	if len(gotTxID) < 60 {
		t.Fatalf("x-client-transaction-id = %q, expected base64 transaction id", gotTxID)
	}
}

func TestClientDiscoversOndemandFromMainBundle(t *testing.T) {
	var gotTxID string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/home":
			fmt.Fprintf(w, `%s<script src="%s/responsive-web/client-web/main.abc.js"></script>`, syntheticTxHTML(), srv.URL)
		case "/responsive-web/client-web/main.abc.js":
			fmt.Fprintf(w, `var a="Bearer %s";var c={9:"ondemand.s"};var h={9:"1234abcd"};e.exports={queryId:"q-replies",operationName:"UserTweetsAndReplies",operationType:"query",metadata:{featureSwitches:["f1"],fieldToggles:[]}}`, fakeBearer)
		case "/responsive-web/client-web/ondemand.s.1234abcda.js":
			fmt.Fprint(w, `var x=(e[2], 16)+(e[3], 16)+(e[4], 16)+(e[7], 16);`)
		case "/i/api/graphql/q-replies/UserTweetsAndReplies":
			gotTxID = r.Header.Get("x-client-transaction-id")
			fmt.Fprint(w, `{"data":{"user":{"result":{"timeline":{"timeline":{"instructions":[]}}}}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := x.NewClient(testSession(), srv.URL, "")
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	if !c.HasTransactionID() {
		t.Fatal("expected Discover to find ondemand.s in main bundle")
	}
	if _, err := c.Query("UserTweetsAndReplies", map[string]any{"userId": myID}); err != nil {
		t.Fatal(err)
	}
	if gotTxID == "" {
		t.Fatal("expected x-client-transaction-id header")
	}
}
