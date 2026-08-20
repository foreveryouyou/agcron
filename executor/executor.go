// Package executor runs scheduled jobs. A job is either a Go function
// (registered via a FuncRegistry), an outbound HTTP request, or a shell
// command executed on the leader instance.
package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
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
	mu     sync.RWMutex // guards funcs against concurrent registration
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
// to call at any time, including at runtime: the registry is guarded by a
// RWMutex, and jobs referencing this name are picked up by the reconciler on
// the next pass.
func (e *Executor) RegisterFunc(name string, fn JobFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.funcs[name] = fn
}

// FuncNames returns the names of all registered funcs, sorted alphabetically.
// The admin UI uses it to offer a dropdown when creating/editing func-type
// jobs. It is safe to call concurrently with RegisterFunc.
func (e *Executor) FuncNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.funcs))
	for n := range e.funcs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Run is the task body bound to every job. It is invoked by gocron only on the
// leader instance. It re-reads the definition from the shared store so that
// enable/disable takes effect immediately across the cluster. The outcome is
// recorded via Store.OnExecuted for later inspection through the admin API.
// Disabled (paused) jobs are skipped without producing an execution record.
func (e *Executor) Run(ctx context.Context, jobID string) {
	e.run(ctx, jobID, false)
}

// ForceRun runs a job immediately regardless of its Enabled flag. It is used
// by the admin API for manual "run now" triggers so that a paused job can
// still be executed on demand.
func (e *Executor) ForceRun(ctx context.Context, jobID string) {
	e.run(ctx, jobID, true)
}

func (e *Executor) run(ctx context.Context, jobID string, force bool) {
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
	if !force && !d.Enabled {
		e.log.Infof("[exec %s] job %q disabled, skip", e.instID, d.Name)
		return // no execution record: a paused job must not surface as an error
	}

	e.log.Infof("[exec %s] >>> running job %q (type=%s)", e.instID, d.Name, d.Type)
	switch d.Type {
	case jobstore.JobTypeFunc:
		e.mu.RLock()
		fn, found := e.funcs[d.Func]
		e.mu.RUnlock() // release before running user code, so fn never runs under the lock
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
	case jobstore.JobTypeShell:
		res, err := e.doShell(ctx, d)
		if err != nil {
			e.finish(rec, d, false, err, 0, res)
			return
		}
		e.finish(rec, d, true, nil, 0, res)
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

// doShell runs a shell job's command via the system shell (/bin/sh -c) on the
// leader instance, so pipelines, redirection and environment expansion behave
// like an interactive shell. Command output (stdout+stderr, combined) becomes
// the execution record's result; a non-zero exit code or timeout marks the run
// as failed.
func (e *Executor) doShell(ctx context.Context, d jobstore.JobDef) (result string, err error) {
	cfg := d.Shell
	if cfg.Command == "" {
		return "", fmt.Errorf("shell command is empty")
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if cfg.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
		defer cancel()
	}
	cmd := exec.CommandContext(runCtx, "/bin/sh", "-c", cfg.Command)
	if cfg.WorkingDir != "" {
		cmd.Dir = cfg.WorkingDir
	}
	if len(cfg.Env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range cfg.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		e.log.Errorf("[exec %s] shell job %q error: %v", e.instID, d.Name, err)
		return strings.TrimSpace(buf.String()), err
	}
	out := strings.TrimSpace(buf.String())
	e.log.Infof("[exec %s] shell job %q done: %s", e.instID, d.Name, truncate(out))
	return out, nil
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
