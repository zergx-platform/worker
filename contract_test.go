package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Contract tests: every assertion mirrors the TypeScript consumers in
// zergx — lib-executor/worker-channel.ts (RpcResponseSchema /
// RpcEventSchema), lib-ai/tool-ops.ts (ExecuteResponseSchema /
// JobCompletedParamsSchema), lib-ai/worker-sync.ts (sync/check manifest and
// missing shape), server/container-routes.ts (jobs proxy, JobInfoSchema).

var testRoot string

func newTestServer(t *testing.T) (*httptest.Server, *State) {
	t.Helper()
	root := t.TempDir()
	testRoot = root
	store := NewStore()
	jobs := NewManager(store, root)
	state := &State{store: store, jobs: jobs, workdir: root, syncedRev: ""}
	srv := httptest.NewServer(buildHandler(state))
	t.Cleanup(srv.Close)

	// Production main() chdirs into the workspace; RPC file_write and job
	// commands resolve relative paths against it.
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(testRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWd) })
	return srv, state
}

func rpcCall(t *testing.T, c *websocket.Conn, id int, method string, params map[string]any) map[string]any {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal rpc: %v", err)
	}
	if err := c.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write rpc: %v", err)
	}
	return readUntil(t, c, 5*time.Second, func(m map[string]any) bool {
		v, ok := m["id"]
		return ok && v == float64(id)
	})
}

func readUntil(t *testing.T, c *websocket.Conn, timeout time.Duration, match func(map[string]any) bool) map[string]any {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read ws message within %v: %v (last raw=%s)", timeout, err, raw)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("worker sent invalid JSON (%s): %v", raw, err)
		}
		if match(m) {
			return m
		}
	}
}

func dialWs(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/ws"
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("tar write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func untarNames(t *testing.T, body []byte) map[string]string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatalf("tar read: %v", err)
			}
			out[hdr.Name] = string(b)
		}
	}
	return out
}

func writeTestFile(t *testing.T, rel, content string) {
	t.Helper()
	full := filepath.Join(testRoot, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func setStateRevHttp(t *testing.T, srv *httptest.Server, rev string) {
	t.Helper()
	resp, err := srv.Client().Post(srv.URL+"/api/v1/sync/check?rev="+rev, "application/json", strings.NewReader(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func waitForJobCompleted(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ev := readUntil(t, c, 5*time.Second, func(m map[string]any) bool {
		return m["event"] == "job.completed"
	})
	params, ok := ev["params"].(map[string]any)
	if !ok {
		t.Fatalf("event params = %v", ev["params"])
	}
	return params
}

func TestHealthContract(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, err := srv.Client().Get(srv.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Healthy bool `json:"healthy"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !body.Healthy {
		t.Fatal("healthy must be true")
	}
}

func TestSyncCheckContract(t *testing.T) {
	srv, state := newTestServer(t)

	manifest := `[{"path":"src/a.ts","size":10},{"path":"b.txt","size":4}]`
	resp, err := srv.Client().Post(srv.URL+"/api/v1/sync/check?rev=rev-1", "application/json", strings.NewReader(manifest))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	// worker-sync.ts parses { missing: z.array(z.string()).optional() }:
	// the field must be a JSON array, never null.
	var parsed struct {
		Missing []string `json:"missing"`
		Extra   []string `json:"extra"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("sync/check body not contract JSON (%s): %v", raw, err)
	}
	if parsed.Missing == nil || parsed.Extra == nil {
		t.Fatalf("missing/extra must serialize as [] not null, got %s", raw)
	}
	if len(parsed.Missing) != 2 {
		t.Fatalf("missing = %v, want both manifest entries", parsed.Missing)
	}
	if state.syncedRev != "rev-1" {
		t.Fatalf("syncedRev = %q, want rev-1", state.syncedRev)
	}

	// Second call: b.txt exists with matching size (not missing); stale.txt
	// exists out-of-band and must be reported as extra and removed.
	writeTestFile(t, "b.txt", "abcd")
	writeTestFile(t, "stale.txt", "old")
	resp2, err := srv.Client().Post(srv.URL+"/api/v1/sync/check", "application/json", strings.NewReader(`[{"path":"b.txt","size":4}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var parsed2 struct {
		Missing []string `json:"missing"`
		Extra   []string `json:"extra"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&parsed2); err != nil {
		t.Fatal(err)
	}
	if len(parsed2.Missing) != 0 {
		t.Fatalf("missing = %v, want empty", parsed2.Missing)
	}
	if len(parsed2.Extra) != 1 || parsed2.Extra[0] != "stale.txt" {
		t.Fatalf("extra = %v, want [stale.txt]", parsed2.Extra)
	}
	if _, err := os.Stat(filepath.Join(testRoot, "stale.txt")); !os.IsNotExist(err) {
		t.Fatal("extra file must be removed")
	}
}

func TestSyncFilesContract(t *testing.T) {
	srv, state := newTestServer(t)
	body := tarGz(t, map[string]string{"b.txt": "hello", "sub/c.txt": "world"})

	resp, err := srv.Client().Post(srv.URL+"/api/v1/sync/files?rev=rev-2", "application/gzip", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var parsed struct {
		OK    bool `json:"ok"`
		Files int  `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatal(err)
	}
	if !parsed.OK || parsed.Files != 2 {
		t.Fatalf("sync/files = %+v, want ok=true files=2", parsed)
	}
	if state.syncedRev != "rev-2" {
		t.Fatalf("syncedRev = %q, want rev-2", state.syncedRev)
	}
	for _, f := range []struct{ path, want string }{{"b.txt", "hello"}, {"sub/c.txt", "world"}} {
		b, err := os.ReadFile(filepath.Join(testRoot, f.path))
		if err != nil || string(b) != f.want {
			t.Fatalf("file %s = %q err=%v, want %q", f.path, b, err, f.want)
		}
	}

	// Malformed gzip: sync failures are real errors — 500 + {ok:false,error}.
	resp3, err := srv.Client().Post(srv.URL+"/api/v1/sync/files", "application/gzip", strings.NewReader("not-gzip"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != 500 {
		t.Fatalf("malformed sync/files status = %d, want 500", resp3.StatusCode)
	}
	var bad struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp3.Body).Decode(&bad); err != nil || bad.OK {
		t.Fatalf("malformed sync/files = %+v err=%v, want ok=false", bad, err)
	}
}

func TestFileEndpointContract(t *testing.T) {
	srv, _ := newTestServer(t)
	writeTestFile(t, "b.txt", "payload")

	resp, err := srv.Client().Get(srv.URL + "/api/v1/file?path=b.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("file status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("content-type"); ct != "application/gzip" {
		t.Fatalf("content-type = %q, want application/gzip", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	files := untarNames(t, raw)
	if files["b.txt"] != "payload" {
		t.Fatalf("tar contents = %v", files)
	}

	resp2, err := srv.Client().Get(srv.URL + "/api/v1/file?path=../etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("traversal status = %d, want 404", resp2.StatusCode)
	}
}

func TestExecuteJobLifecycleContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)

	// rev mismatch -> ExecuteResponseSchema { need_sync: true }, no job.
	resp := rpcCall(t, c, 1, "execute", map[string]any{"command": "echo hi", "rev": "other"})
	if ns, ok := resp["result"].(map[string]any); !ok || ns["need_sync"] != true {
		t.Fatalf("rev mismatch result = %v, want {need_sync:true}", resp["result"])
	}

	// Align rev out-of-band via HTTP (same as lib-ai worker-sync), then
	// execute: every command is registered as a backgrounded job (no
	// fast/slow split), so the response carries job_id + backgrounded.
	setStateRevHttp(t, srv, "rev-x")
	resp2 := rpcCall(t, c, 2, "execute", map[string]any{"command": "echo contract-hello", "rev": "rev-x"})
	res2, ok := resp2["result"].(map[string]any)
	if !ok {
		t.Fatalf("execute result = %v", resp2["result"])
	}
	if _, hasErr := resp2["error"]; hasErr {
		t.Fatalf("unexpected error field: %v", resp2)
	}
	jobID, _ := res2["job_id"].(string)
	if jobID == "" || res2["backgrounded"] != true {
		t.Fatalf("execute result = %v, want job_id + backgrounded:true", res2)
	}
	if _, hasOutput := res2["output"]; hasOutput {
		t.Fatalf("execute must not return a merged output field: %v", res2)
	}

	// A job was registered (no longer an empty list) and, once the command
	// completes, a job.completed event is broadcast.
	jr := rpcCall(t, c, 3, "jobs", map[string]any{})
	if jres, ok := jr["result"].(map[string]any); ok {
		if j, ok := jres["jobs"].([]any); !ok || len(j) == 0 {
			t.Fatalf("jobs after execute = %v, want non-empty", jres["jobs"])
		}
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("expected job.completed broadcast, got %v", err)
		}
		if strings.Contains(string(msg), "job.completed") {
			break
		}
	}
	_ = c.SetReadDeadline(time.Time{})
}

func TestRpcErrorEnvelopeContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)

	// Unknown method: RpcResponseSchema requires id:number + error:string.
	resp := rpcCall(t, c, 7, "bogus", map[string]any{})
	if resp["id"] != float64(7) {
		t.Fatalf("id = %v (%T), want number 7", resp["id"], resp["id"])
	}
	errMsg, ok := resp["error"].(string)
	if !ok || !strings.Contains(errMsg, "unknown method") {
		t.Fatalf("error = %v, want 'unknown method: bogus'", resp["error"])
	}

	// Missing command: TS worker schema required command and rejected the call.
	resp2 := rpcCall(t, c, 8, "execute", map[string]any{})
	if _, ok := resp2["error"].(string); !ok {
		t.Fatalf("execute without command must error, got %v", resp2)
	}
}

func TestJobsHttpContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-j")

	// Every command is backgrounded as a job (no fast/slow split). A long
	// sleep keeps the job running through the HTTP + kill assertions below.
	resp := rpcCall(t, c, 1, "execute", map[string]any{"command": "sleep 30", "rev": "rev-j"})
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatal(resp)
	}
	jobID, _ := res["job_id"].(string)
	if jobID == "" || res["backgrounded"] != true {
		t.Fatalf("backgrounded result = %v, want job_id + backgrounded", res)
	}
	if note, _ := res["note"].(string); !strings.Contains(note, jobID) || !strings.Contains(note, "job_output") {
		t.Fatalf("note = %v, want guidance containing jid and job_output", res["note"])
	}

	// container-routes proxies this response verbatim; the list is
	// lightweight: no stdout payload per entry.
	resp2, err := srv.Client().Get(srv.URL + "/api/v1/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var list struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	var target map[string]any
	for _, j := range list.Jobs {
		for _, key := range []string{"id", "command", "state", "exit_code", "started_at"} {
			if _, ok := j[key]; !ok {
				t.Fatalf("job entry missing %q: %v", key, j)
			}
		}
		if _, hasStdout := j["stdout"]; hasStdout {
			t.Fatalf("jobs list must be lightweight (no stdout): %v", j)
		}
		if j["id"] == jobID {
			target = j
		}
	}
	if target == nil {
		t.Fatalf("job %s not found in list", jobID)
	}

	// DELETE /api/v1/jobs {job_id} -> {ok: bool}.
	req, _ := http.NewRequest("DELETE", srv.URL+"/api/v1/jobs", strings.NewReader(`{"job_id":"`+jobID+`"}`))
	resp3, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	var kill struct {
		OK bool `json:"ok"`
	}
	if err := json.NewDecoder(resp3.Body).Decode(&kill); err != nil {
		t.Fatal(err)
	}
	if !kill.OK {
		t.Fatalf("kill via DELETE = %+v, want ok", kill)
	}
	waitForJobCompleted(t, c)
}

func TestKillContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-k")
	resp := rpcCall(t, c, 1, "execute", map[string]any{"command": "sleep 30", "rev": "rev-k"})
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatal(resp)
	}
	jobID, _ := res["job_id"].(string)

	// Kill may race with proc registration in the manager; retry until ok.
	deadline := time.Now().Add(3 * time.Second)
	killed := false
	for time.Now().Before(deadline) {
		kr := rpcCall(t, c, 2, "kill", map[string]any{"job_id": jobID})
		if kres, ok := kr["result"].(map[string]any); ok && kres["ok"] == true {
			killed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !killed {
		t.Fatal("kill never returned ok:true")
	}

	params := waitForJobCompleted(t, c)
	if ec, ok := params["exit_code"].(float64); !ok || int(ec) != -9 {
		t.Fatalf("killed exit_code = %v, want -9", params["exit_code"])
	}
}

func TestFileReadWriteRpcContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)

	// file_write content is base64 (worker-channel sends raw strings only).
	w := rpcCall(t, c, 1, "file_write", map[string]any{"path": "out/bin.txt", "content": "aGVsbG8="})
	wres, ok := w["result"].(map[string]any)
	if !ok || wres["ok"] != true {
		t.Fatalf("file_write result = %v", w["result"])
	}
	b, err := os.ReadFile(filepath.Join(testRoot, "out/bin.txt"))
	if err != nil || string(b) != "hello" {
		t.Fatalf("written file = %q err=%v, want hello", b, err)
	}

	// file_read returns {content: base64}.
	r := rpcCall(t, c, 2, "file_read", map[string]any{"path": "out/bin.txt"})
	rres, ok := r["result"].(map[string]any)
	if !ok || rres["content"] != "aGVsbG8=" {
		t.Fatalf("file_read result = %v", r["result"])
	}

	// file_read of missing file -> error string.
	r2 := rpcCall(t, c, 3, "file_read", map[string]any{"path": "nope.txt"})
	if _, ok := r2["error"].(string); !ok {
		t.Fatalf("file_read missing must error, got %v", r2)
	}
}

func TestFileRpcSandboxContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)

	// Relative inside workspace: allowed.
	w := rpcCall(t, c, 1, "file_write", map[string]any{"path": "ok.txt", "content": "aGk="})
	if wres, ok := w["result"].(map[string]any); !ok || wres["ok"] != true {
		t.Fatalf("in-workspace write = %v", w["result"])
	}

	// Parent traversal: rejected.
	evil := rpcCall(t, c, 2, "file_write", map[string]any{"path": "../evil.txt", "content": "aGk="})
	if _, ok := evil["error"].(string); !ok {
		t.Fatalf("traversal write must error, got %v", evil)
	}

	// Absolute path outside workspace: rejected.
	abs := rpcCall(t, c, 3, "file_read", map[string]any{"path": "/etc/passwd"})
	if _, ok := abs["error"].(string); !ok {
		t.Fatalf("absolute out-of-workspace read must error, got %v", abs)
	}

	// Sneaky traversal via inner segments: rejected.
	sneaky := rpcCall(t, c, 4, "file_write", map[string]any{"path": "sub/../../evil2.txt", "content": "aGk="})
	if _, ok := sneaky["error"].(string); !ok {
		t.Fatalf("sneaky traversal write must error, got %v", sneaky)
	}
}

func TestUtf8ChunkingContract(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-u")

	marker := "中文测试-µ-é"
	// awk BEGIN block lives entirely inside single quotes (literal to the
	// minimal shell) and prints 300 multibyte repetitions in one go.
	awkProg := "BEGIN{for(i=0;i<300;i++)printf \"" + marker + "\"}"
	resp := rpcCall(t, c, 1, "execute", map[string]any{
		"command": "awk '" + awkProg + "'",
		"rev":     "rev-u",
	})
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatal(resp)
	}
	jid, _ := res["job_id"].(string)
	if jid == "" {
		t.Fatalf("execute must return job_id: %v", res)
	}
	w := rpcCall(t, c, 2, "job_wait", map[string]any{"job_id": jid, "timeout_ms": 5000})
	if st, _ := w["result"].(map[string]any)["state"].(string); st != "done" {
		t.Fatalf("job_wait = %v", w["result"])
	}
	out := rpcCall(t, c, 3, "job_output", map[string]any{"job_id": jid, "stream": "all", "offset": 0, "limit": 500})
	lines := out["result"].(map[string]any)["lines"].([]any)
	var sb strings.Builder
	for _, l := range lines {
		sb.WriteString(l.(string))
	}
	// 300 repetitions of a multibyte marker streamed through 4096-byte
	// pipe chunks must survive intact (B8 regression).
	want := strings.Repeat(marker, 300)
	if sb.String() != want {
		t.Fatalf("output corrupted by chunk boundary: got %d bytes want %d", sb.Len(), len(want))
	}
}

func TestDeadGitEndpointsRemoved(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, ep := range []string{"/api/v1/git/sync", "/api/v1/git/push"} {
		resp, err := srv.Client().Post(srv.URL+ep, "application/json", strings.NewReader(`{"org":"o","repo":"r","branch":"main"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatalf("POST %s = %d, want 404 (endpoint removed)", ep, resp.StatusCode)
		}
	}
}

func TestJobOutputNoReserveOfLastLine(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-nores")

	resp := rpcCall(t, c, 1, "execute", map[string]any{"command": "sleep 0.5 && printf 'l1\\nl2\\n'", "rev": "rev-nores"})
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatal(resp)
	}
	jobID, _ := res["job_id"].(string)
	waitForJobCompleted(t, c)

	// Drain from the top, then poll past the end: must return ZERO
	// lines, not the final line again (streaming duplicate bug).
	drained := rpcCall(t, c, 2, "job_output", map[string]any{
		"job_id": jobID, "stream": "all", "offset": 0, "limit": 500,
	})
	d, ok := drained["result"].(map[string]any)
	if !ok {
		t.Fatal(drained)
	}
	total := int(d["total_lines"].(float64))
	if total != 2 {
		t.Fatalf("total_lines = %d, want 2", total)
	}
	for _, off := range []int{total, total + 1, total + 5} {
		poll := rpcCall(t, c, 3, "job_output", map[string]any{
			"job_id": jobID, "stream": "all", "offset": off, "limit": 200,
		})
		p, ok := poll["result"].(map[string]any)
		if !ok {
			t.Fatal(poll)
		}
		lines := p["lines"].([]any)
		if len(lines) != 0 {
			t.Fatalf("poll offset=%d re-served %d lines (want 0): %v", off, len(lines), lines)
		}
	}
}
