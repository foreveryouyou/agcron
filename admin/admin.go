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
	"github.com/foreveryouyou/agcron/scheduler"
	"github.com/foreveryouyou/agcron/logx"
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

// New constructs the admin API. log is optional; a nil value uses the
// logx.Default logger.
func New(store jobstore.Store, instID string, e *elector.RedisElector, sched *scheduler.Scheduler, log logx.Logger) *API {
	return &API{store: store, instID: instID, elector: e, sched: sched, log: logx.With(log)}
}

func (a *API) Mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", a.status)
	mux.HandleFunc("/echo", a.echo)
	mux.HandleFunc("/jobs", a.jobs)
	mux.HandleFunc("/jobs/", a.jobByID)
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
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
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
