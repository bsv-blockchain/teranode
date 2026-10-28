package probe

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/probe/ui"
	"github.com/stretchr/testify/require"
)

const wantCSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

func getUI(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()

	resp, err := http.Get(url) //nolint:gosec,noctx // test URL on a local listener
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp, body
}

func requireSecurityHeaders(t *testing.T, resp *http.Response, path string) {
	t.Helper()

	require.Equal(t, wantCSP, resp.Header.Get("Content-Security-Policy"), path)
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"), path)
	require.Equal(t, "no-referrer", resp.Header.Get("Referrer-Policy"), path)
	require.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"), path)
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"), path)
}

func TestUIShellServedWithoutToken(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	cases := []struct {
		path        string
		contentType string
		file        string
		marker      string
	}{
		{"/", "text/html; charset=utf-8", "index.html", `src="app.js"`},
		{"/index.html", "text/html; charset=utf-8", "index.html", `src="app.js"`},
		{"/style.css", "text/css; charset=utf-8", "style.css", "prefers-reduced-motion"},
		{"/view.js", "text/javascript; charset=utf-8", "view.js", "export function renderHome"},
		{"/app.js", "text/javascript; charset=utf-8", "app.js", "export function createClient"},
	}

	for _, c := range cases {
		resp, body := getUI(t, apiURL(srv, c.path))
		require.Equal(t, http.StatusOK, resp.StatusCode, c.path)
		require.Equal(t, c.contentType, resp.Header.Get("Content-Type"), c.path)
		requireSecurityHeaders(t, resp, c.path)

		want, err := ui.Files.ReadFile(c.file)
		require.NoError(t, err)
		require.Equal(t, string(want), string(body), c.path)
		require.Contains(t, string(body), c.marker, c.path)
	}

	resp, body := getUI(t, apiURL(srv, "/"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), testToken)
}

func TestUIDataStillRequiresToken(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	for _, r := range restRoutes {
		require.Equal(t, http.StatusUnauthorized, doRaw(t, r.method, apiURL(srv, r.path), "", r.body), r.path)
	}

	resp, _ := getUI(t, apiURL(srv, "/api/snapshot"))
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	status, body := doRequest(t, http.MethodGet, apiURL(srv, "/api/snapshot"), testToken, nil)
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, string(body), `"node"`)
}

func TestUIServesOnlyShippedFiles(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	for _, path := range []string{
		"/view.test.js",
		"/app.test.js",
		"/testdata/snapshot.json",
		"/package.json",
		"/ui.go",
		"/testdata/",
		"/no-such-file.js",
		"/../ui.go",
		"/%2e%2e/ui.go",
		"/style.css/x",
		"/index.html/",
	} {
		resp, _ := getUI(t, apiURL(srv, path))
		require.Equal(t, http.StatusNotFound, resp.StatusCode, path)
	}

	for _, path := range []string{"/", "/app.js"} {
		status, _ := doRequest(t, http.MethodPost, apiURL(srv, path), "", nil)
		require.NotEqual(t, http.StatusOK, status, "POST %s", path)
	}

	entries, err := ui.Files.ReadDir(".")
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	require.ElementsMatch(t, []string{"index.html", "style.css", "view.js", "app.js"}, names)
}

func TestUIHasNoInlineScriptOrHandlers(t *testing.T) {
	raw, err := ui.Files.ReadFile("index.html")
	require.NoError(t, err)

	dec := xml.NewDecoder(strings.NewReader(string(raw)))
	dec.Strict = false
	dec.AutoClose = xml.HTMLAutoClose
	dec.Entity = xml.HTMLEntity

	var (
		scripts  []map[string]string
		inScript bool
		elements int
	)

	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		switch v := tok.(type) {
		case xml.StartElement:
			elements++

			attrs := map[string]string{}

			for _, a := range v.Attr {
				name := strings.ToLower(a.Name.Local)
				require.False(t, strings.HasPrefix(name, "on"), "inline handler %s on <%s>", name, v.Name.Local)
				require.NotEqual(t, "style", name, "inline style on <%s>", v.Name.Local)
				require.False(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(a.Value)), "javascript:"), "javascript URL on <%s>", v.Name.Local)
				attrs[name] = a.Value
			}

			require.NotEqual(t, "style", strings.ToLower(v.Name.Local), "inline <style> element")

			if strings.ToLower(v.Name.Local) == "script" {
				scripts = append(scripts, attrs)
				inScript = true
			}
		case xml.EndElement:
			if strings.ToLower(v.Name.Local) == "script" {
				inScript = false
			}
		case xml.CharData:
			if inScript {
				require.Empty(t, strings.TrimSpace(string(v)), "inline script body")
			}
		}
	}

	require.Greater(t, elements, 5)
	require.Len(t, scripts, 1)
	require.Equal(t, map[string]string{"type": "module", "src": "app.js"}, scripts[0])
}
