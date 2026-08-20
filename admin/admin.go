// Package admin exposes a tiny HTTP control surface so you can modify jobs at
// runtime and watch leadership. Changes are written to the shared store; the
// reconciler on every instance picks them up, so a single write converges the
// cluster.
package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/foreveryouyou/agcron/elector"
	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/logx"
	"github.com/foreveryouyou/agcron/scheduler"
)

// API exposes a tiny control surface so you can modify jobs at runtime and
// watch leadership. Changes are written to the shared store; the reconciler
// on every instance picks them up, so a single write converges the cluster.
type API struct {
	store   jobstore.Store
	instID  string
	elector *elector.RedisElector
	sched   *scheduler.Scheduler
	log     logx.Logger
	prefix  string          // URL prefix of the admin UI/API, e.g. "/agcron"
	funcs   func() []string // returns registered func names; optional, nil means "unknown"
}

// jobView enriches a JobDef with its most recent execution result.
type jobView struct {
	jobstore.JobDef
	LastExecution *jobstore.ExecutionRecord `json:"last_execution,omitempty"`
}

// withExec augments a JobDef view with its last execution record (if any).
func (a *API) withExec(ctx context.Context, d jobstore.JobDef) jobView {
	v := jobView{JobDef: d}
	if rec, ok, err := a.store.LastExecution(ctx, d.ID); err == nil && ok {
		v.LastExecution = &rec
	}
	return v
}

// New constructs the admin API with the default "/agcron" URL prefix. log is
// optional; a nil value uses the logx.Default logger.
func New(store jobstore.Store, instID string, e *elector.RedisElector, sched *scheduler.Scheduler, log logx.Logger) *API {
	return NewWithPrefix(store, instID, e, sched, "", log)
}

// NewWithPrefix is like New but mounts the admin UI/API under the given URL
// prefix, e.g. "/agcron" serves the UI at /agcron and the API at
// /agcron/api/*. An empty prefix uses the default "/agcron"; "/" mounts the
// admin at the server root (the pre-prefix behaviour).
func NewWithPrefix(store jobstore.Store, instID string, e *elector.RedisElector, sched *scheduler.Scheduler, prefix string, log logx.Logger) *API {
	return &API{
		store:   store,
		instID:  instID,
		elector: e,
		sched:   sched,
		log:     logx.With(log),
		prefix:  normalizePrefix(prefix),
	}
}

// normalizePrefix ensures the prefix has a leading "/" and no trailing "/".
// "" falls back to "/agcron"; "/" means "no prefix" (mount at root).
func normalizePrefix(p string) string {
	if p == "" {
		return "/agcron"
	}
	if p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimSuffix(p, "/")
}

// Prefix returns the normalized URL prefix the admin UI/API is mounted under,
// e.g. "/agcron" ("" when mounted at the root).
func (a *API) Prefix() string { return a.prefix }

// SetFuncNames wires up the callback that returns the registered func names,
// so GET /api/funcs can offer them to the UI when creating func-type jobs.
func (a *API) SetFuncNames(fn func() []string) { a.funcs = fn }

func (a *API) Mux() http.Handler {
	p := a.prefix
	mux := http.NewServeMux()
	mux.HandleFunc(p+"/api/status", a.status)
	mux.HandleFunc(p+"/api/echo", a.echo)
	mux.HandleFunc(p+"/api/funcs", a.funcsHandler)
	mux.HandleFunc(p+"/api/jobs", a.jobs)
	mux.HandleFunc(p+"/api/jobs/", a.jobByID)
	// task management UI: index.html is rendered via html/template with the API
	// prefix injected, so the page's /api/* requests always resolve correctly
	// regardless of the mount path.
	ui := a.ui()
	mux.Handle(p+"/", ui)
	if p != "" {
		// 直接渲染 UI 首页,而不是 301 重定向到 p+"/"。
		// 部分 Web 框架(如 GoFrame)在路由匹配前会剥离 URL.Path 末尾的 "/",
		// 使 "/agcron/" 被还原成 "/agcron" 后再次命中 301,造成无限重定向
		// (ERR_TOO_MANY_REDIRECTS)。模板渲染不依赖请求路径,直接复用 ui 即可。
		mux.HandleFunc(p, ui.ServeHTTP)
	}
	return mux
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	defs, _ := a.store.List(r.Context())
	jobs := make(map[string]jobView, len(defs))
	for id, d := range defs {
		jobs[id] = a.withExec(r.Context(), d)
	}
	writeJSON(w, 200, map[string]any{
		"instance_id": a.instID,
		"is_leader":   a.elector.IsLeaderNow(),
		"jobs":        jobs,
	})
}

// echo is a self-contained HTTP target the http-type demo job calls.
func (a *API) echo(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	a.log.Infof("[echo %s] %s %s body=%s", a.instID, r.Method, r.URL.Path, string(b))
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// funcsHandler returns the names of all registered Go functions, sorted, so the
// UI can render a dropdown when creating/editing func-type jobs.
func (a *API) funcsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	names := []string{}
	if a.funcs != nil {
		names = a.funcs()
	}
	writeJSON(w, 200, map[string]any{"funcs": names})
}

func (a *API) jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		defs, err := a.store.List(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		jobs := make([]jobView, 0, len(defs))
		for _, d := range defs {
			jobs = append(jobs, a.withExec(r.Context(), d))
		}
		writeJSON(w, 200, jobs)
	case http.MethodPost:
		var d jobstore.JobDef
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if d.ID == "" {
			writeJSON(w, 400, map[string]string{"error": "id required"})
			return
		}
		if err := scheduler.ValidateSchedule(d.Schedule, d.WithSeconds); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if _, ok, err := a.store.Get(r.Context(), d.ID); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		} else if ok {
			writeJSON(w, 409, map[string]string{"error": "job id already exists: " + d.ID})
			return
		}
		if err := a.store.Put(r.Context(), d); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 201, d)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (a *API) jobByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, a.prefix+"/api/jobs/")
	jobID := strings.SplitN(id, "/", 2)[0]
	if jobID == "" {
		w.WriteHeader(404)
		return
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/pause"):
		a.setEnabled(w, r, jobID, false)
	case strings.HasSuffix(r.URL.Path, "/resume"):
		a.setEnabled(w, r, jobID, true)
	case strings.HasSuffix(r.URL.Path, "/run"):
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !a.sched.RunNow(r.Context(), jobID) {
			writeJSON(w, 404, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, 200, map[string]string{"ok": "true", "job_id": jobID, "triggered_on": a.instID})
	case r.Method == http.MethodPut:
		// update an existing job: 409-free, but the job must already exist
		var d jobstore.JobDef
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if d.ID != jobID {
			writeJSON(w, 400, map[string]string{"error": "id mismatch: path id and body id differ"})
			return
		}
		if err := scheduler.ValidateSchedule(d.Schedule, d.WithSeconds); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if _, ok, err := a.store.Get(r.Context(), jobID); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		} else if !ok {
			writeJSON(w, 404, map[string]string{"error": "job not found"})
			return
		}
		if err := a.store.Put(r.Context(), d); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, d)
	case r.Method == http.MethodDelete:
		if err := a.store.Delete(r.Context(), jobID); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{"deleted": jobID})
	default:
		d, ok, err := a.store.Get(r.Context(), jobID)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		if !ok {
			w.WriteHeader(404)
			return
		}
		writeJSON(w, 200, a.withExec(r.Context(), d))
	}
}

func (a *API) setEnabled(w http.ResponseWriter, r *http.Request, id string, enabled bool) {
	d, ok, err := a.store.Get(r.Context(), id)
	if err != nil || !ok {
		w.WriteHeader(404)
		return
	}
	d.Enabled = enabled
	if err := a.store.Put(r.Context(), d); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, d)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
