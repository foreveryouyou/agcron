// Package executor runs scheduled jobs. A job is either a Go function
// (registered via a FuncRegistry) or an outbound HTTP request.
package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/logx"
)

// JobFunc is a registered Go-function task.
type JobFunc func(ctx context.Context, j jobstore.JobDef) error

// FuncRegistry maps a func name (stored on the job) to its implementation.
type FuncRegistry map[string]JobFunc

type Executor struct {
	store  jobstore.Store
	funcs  FuncRegistry
	instID string
	client *http.Client
	log    logx.Logger
}

// New constructs an Executor bound to a job store and a set of registered funcs.
// log is optional; a nil value uses the logx.Default logger.
func New(store jobstore.Store, instID string, funcs FuncRegistry, log logx.Logger) *Executor {
	return &Executor{
		store:  store,
		funcs:  funcs,
		instID: instID,
		client: &http.Client{Timeout: 10 * time.Second},
		log:    logx.With(log),
	}
}

// RegisterFunc registers (or replaces) a Go function task by name. It is safe
// to call before Start; jobs referencing this name are picked up by the
// reconciler on the next pass.
func (e *Executor) RegisterFunc(name string, fn JobFunc) {
	e.funcs[name] = fn
}

// Run is the task body bound to every job. It is invoked by gocron only on the
// leader instance. It re-reads the definition from the shared store so that
// enable/disable takes effect immediately across the cluster.
func (e *Executor) Run(ctx context.Context, jobID string) {
	d, ok, err := e.store.Get(ctx, jobID)
	if err != nil {
		e.log.Errorf("[exec %s] get %s error: %v", e.instID, jobID, err)
		return
	}
	if !ok {
		e.log.Warnf("[exec %s] job %s no longer exists", e.instID, jobID)
		return
	}
	if !d.Enabled {
		e.log.Infof("[exec %s] job %q disabled, skip", e.instID, d.Name)
		return
	}

	e.log.Infof("[exec %s] >>> running job %q (type=%s)", e.instID, d.Name, d.Type)
	switch d.Type {
	case jobstore.JobTypeFunc:
		fn, found := e.funcs[d.Func]
		if !found {
			e.log.Errorf("[exec %s] func %q not registered", e.instID, d.Func)
			return
		}
		if err := fn(ctx, d); err != nil {
			e.log.Errorf("[exec %s] func %q error: %v", e.instID, d.Func, err)
		}
	case jobstore.JobTypeHTTP:
		e.doHTTP(ctx, d)
	default:
		e.log.Errorf("[exec %s] unknown job type %q", e.instID, d.Type)
	}
}

func (e *Executor) doHTTP(ctx context.Context, d jobstore.JobDef) {
	method := d.HTTP.Method
	if method == "" {
		method = http.MethodPost
	}
	var body io.Reader
	if d.HTTP.Body != "" {
		body = bytes.NewBufferString(d.HTTP.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.HTTP.URL, body)
	if err != nil {
		e.log.Errorf("[exec %s] http new request error: %v", e.instID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		e.log.Errorf("[exec %s] http do error: %v", e.instID, err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	e.log.Infof("[exec %s] http %s -> %d %s", e.instID, d.HTTP.URL, resp.StatusCode, truncate(string(b)))
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
