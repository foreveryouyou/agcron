// Package executor runs scheduled jobs. A job is either a Go function
// (registered via a FuncRegistry) or an outbound HTTP request.
package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/logx"
)

// JobFunc is a registered Go-function task. The string result is recorded in
// the execution record (ExecutionRecord.Result) and surfaced via the admin
// API; the error marks the run as failed.
type JobFunc func(ctx context.Context, j jobstore.JobDef) (res string, err error)

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
// enable/disable takes effect immediately across the cluster. The outcome is
// recorded via Store.OnExecuted for later inspection through the admin API.
func (e *Executor) Run(ctx context.Context, jobID string) {
	started := time.Now()
	rec := jobstore.ExecutionRecord{
		JobID:     jobID,
		Instance:  e.instID,
		StartedAt: started,
	}

	d, ok, err := e.store.Get(ctx, jobID)
	if err != nil {
		e.log.Errorf("[exec %s] get %s error: %v", e.instID, jobID, err)
		e.finish(rec, d, false, err, 0, "")
		return
	}
	if !ok {
		e.log.Warnf("[exec %s] job %s no longer exists", e.instID, jobID)
		e.finish(rec, jobstore.JobDef{ID: jobID}, false, fmt.Errorf("job no longer exists"), 0, "")
		return
	}
	rec.JobName = d.Name
	if !d.Enabled {
		e.log.Infof("[exec %s] job %q disabled, skip", e.instID, d.Name)
		e.finish(rec, d, false, fmt.Errorf("job disabled"), 0, "")
		return
	}

	e.log.Infof("[exec %s] >>> running job %q (type=%s)", e.instID, d.Name, d.Type)
	switch d.Type {
	case jobstore.JobTypeFunc:
		fn, found := e.funcs[d.Func]
		if !found {
			e.log.Errorf("[exec %s] func %q not registered", e.instID, d.Func)
			e.finish(rec, d, false, fmt.Errorf("func %q not registered", d.Func), 0, "")
			return
		}
		res, err := fn(ctx, d)
		if err != nil {
			e.log.Errorf("[exec %s] func %q error: %v", e.instID, d.Func, err)
			e.finish(rec, d, false, err, 0, res)
			return
		}
		e.finish(rec, d, true, nil, 0, res)
	case jobstore.JobTypeHTTP:
		status, body, err := e.doHTTP(ctx, d)
		if err != nil {
			e.finish(rec, d, false, err, status, body)
			return
		}
		e.finish(rec, d, true, nil, status, body)
	default:
		e.log.Errorf("[exec %s] unknown job type %q", e.instID, d.Type)
		e.finish(rec, d, false, fmt.Errorf("unknown job type %q", d.Type), 0, "")
	}
}

// finish records the outcome of a run and persists it via Store.OnExecuted.
// Errors here are logged but never fatal to the scheduler.
func (e *Executor) finish(rec jobstore.ExecutionRecord, d jobstore.JobDef, success bool, runErr error, httpStatus int, result string) {
	if rec.JobName == "" {
		rec.JobName = d.Name
	}
	rec.FinishedAt = time.Now()
	rec.Success = success
	rec.HTTPStatus = httpStatus
	rec.Result = truncate(result)
	if runErr != nil {
		rec.Error = runErr.Error()
	}
	if err := e.store.OnExecuted(context.Background(), rec); err != nil {
		e.log.Errorf("[exec %s] record execution of %s error: %v", e.instID, rec.JobID, err)
	}
}

func (e *Executor) doHTTP(ctx context.Context, d jobstore.JobDef) (status int, body string, err error) {
	method := d.HTTP.Method
	if method == "" {
		method = http.MethodPost
	}
	var r io.Reader
	if d.HTTP.Body != "" {
		r = bytes.NewBufferString(d.HTTP.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.HTTP.URL, r)
	if err != nil {
		e.log.Errorf("[exec %s] http new request error: %v", e.instID, err)
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range d.HTTP.Headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.log.Errorf("[exec %s] http do error: %v", e.instID, err)
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	body = string(b)
	e.log.Infof("[exec %s] http %s -> %d %s", e.instID, d.HTTP.URL, resp.StatusCode, truncate(body))
	return resp.StatusCode, body, nil
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
