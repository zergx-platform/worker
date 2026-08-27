package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"forgejo.develop.10.199.64.20.nip.io/zergx/go-shared/jsonwrite"
)

// logger replaces the previous hand-rolled slogJSON (which discarded the
// level); standard slog keeps JSON output with real severity levels.
var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func buildHandler(state *State) http.Handler {
	store := state.store
	jobs := state.jobs
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		jsonwrite.JSON(w, http.StatusOK, map[string]any{"ok": true, "healthy": true})
	})
	mux.HandleFunc("/api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			jsonwrite.JSON(w, http.StatusOK, map[string]any{"ok": true, "jobs": jobsResponse(store)})
		case http.MethodDelete:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			jid, _ := body["job_id"].(string)
			jsonwrite.JSON(w, http.StatusOK, map[string]any{"ok": jobs.KillJob(jid)})
		default:
			jsonwrite.JSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method not allowed"})
		}
	})
	mux.HandleFunc("/api/v1/sync/check", func(w http.ResponseWriter, r *http.Request) {
		handleSyncCheck(w, r, state)
	})
	mux.HandleFunc("/api/v1/sync/files", func(w http.ResponseWriter, r *http.Request) {
		handleSyncFiles(w, r, state)
	})
	mux.HandleFunc("/api/v1/file", func(w http.ResponseWriter, r *http.Request) {
		handleFile(w, r, state.workdir)
	})

	ws := newWSHub(state)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws":
			ws.serve(w, r)
			return
		case "/ws/job":
			ws.serveJobStream(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func main() {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		logger.Error("mkdir failed", "err", err)
	}
	if err := os.Chdir(workdir); err != nil {
		logger.Error("chdir failed", "err", err)
	}

	store := NewStore()
	jobs := NewManager(store, workdir)
	state := &State{store: store, jobs: jobs, workdir: workdir, syncedRev: ""}

	srv := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: buildHandler(state),
	}
	logger.Info("zergx-worker listening", "port", port)
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server error", "err", err)
	}
}

func jobsResponse(store *Store) []map[string]any {
	out := make([]map[string]any, 0)
	for _, j := range store.ListJobs() {
		out = append(out, map[string]any{
			"id": j.ID, "command": j.Command, "state": j.State,
			"exit_code": j.ExitCode, "started_at": j.StartedAt,
			"finished_at": j.FinishedAt,
		})
	}
	return out
}

type State struct {
	store     *Store
	jobs      *Manager
	workdir   string
	syncedRev string
}

func handleSyncCheck(w http.ResponseWriter, r *http.Request, st *State) {
	root := st.workdir
	rev := r.URL.Query().Get("rev")
	var manifest []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
		jsonwrite.JSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	manifestSet := map[string]bool{}
	for _, e := range manifest {
		manifestSet[e.Path] = true
	}
	missing := []string{}
	for _, e := range manifest {
		fi, err := os.Stat(filepath.Join(root, e.Path))
		if err != nil || fi.Size() != e.Size {
			missing = append(missing, e.Path)
		}
	}
	extra := []string{}
	for _, rel := range listWorkspaceFiles(root) {
		if manifestSet[rel] {
			continue
		}
		extra = append(extra, rel)
		_ = os.Remove(filepath.Join(root, rel))
	}
	if rev != "" {
		st.syncedRev = rev
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]any{"ok": true, "missing": missing, "extra": extra})
}

func handleSyncFiles(w http.ResponseWriter, r *http.Request, st *State) {
	root := st.workdir
	rev := r.URL.Query().Get("rev")
	n, err := extractTarball(r.Body, root)
	if err != nil {
		// Failed syncs are real errors: 5xx + {ok:false,error}, matching the
		// gateway-wide status-code convention (memory/ops/jj-server).
		jsonwrite.JSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if rev != "" {
		st.syncedRev = rev
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]any{"ok": true, "files": n})
}

func handleFile(w http.ResponseWriter, r *http.Request, root string) {
	path := r.URL.Query().Get("path")
	full := filepath.Join(root, path)
	rel, err := filepath.Rel(root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		http.NotFound(w, r)
		return
	}
	if _, err := os.Stat(full); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("content-type", "application/gzip")
	_ = createTarball(w, root, []string{path})
}
