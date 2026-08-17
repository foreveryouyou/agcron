// Package agcron is a distributed, Redis-backed cron library. It wraps gocron
// with a Redis leader election and a shared job store, so a cluster of
// processes runs each scheduled job exactly once — on the leader instance —
// while followers stay warm and take over if the leader dies.
//
// Minimal usage:
//
//	eng, err := agcron.New(ctx, agcron.Config{
//		RedisAddr:  "localhost:6379",
//		InstanceID: "node-1",
//		Funcs:      agcron.FuncRegistry{"sayHello": sayHello},
//		Seed:       []jobstore.JobDef{ /* ... */ },
//	})
//	if err != nil { log.Fatal(err) }
//	eng.Start()
//	defer eng.Stop()
package agcron

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/foreveryouyou/agcron/admin"
	"github.com/foreveryouyou/agcron/elector"
	"github.com/foreveryouyou/agcron/executor"
	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/logx"
	"github.com/foreveryouyou/agcron/scheduler"
)

// Config configures an Engine.
type Config struct {
	RedisAddr  string                // Redis address, e.g. "localhost:6379"
	RedisPass  string                // Redis password (may be empty)
	InstanceID string                // unique per process; defaults to hostname
	ElectorKey string                // Redis key used for leader election
	ElectorTTL time.Duration         // leader lock TTL; <=0 means 10s
	Reconcile  time.Duration         // reconciler poll interval; <=0 means 5s
	Store      jobstore.Store        // job store; defaults to a Redis-backed store
	Funcs      executor.FuncRegistry // Go-function jobs keyed by name
	Seed       []jobstore.JobDef     // seeded only when the store is empty
	AdminAddr  string                // if non-empty, serves the admin HTTP API here
	Logger     logx.Logger           // optional custom logger; nil uses logx.Default
}

// Engine ties together the store, executor, elector and scheduler.
type Engine struct {
	cfg     Config
	rdb     *redis.Client
	store   jobstore.Store
	exec    *executor.Executor
	elector *elector.RedisElector
	sched   *scheduler.Scheduler
	admin   *admin.API
	log     logx.Logger
}

// New constructs and wires up an Engine. It pings Redis and, if Seed is
// provided, populates the store when empty.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.InstanceID == "" {
		cfg.InstanceID, _ = os.Hostname()
	}
	if cfg.ElectorKey == "" {
		cfg.ElectorKey = "cron:leader"
	}
	lg := logx.With(cfg.Logger)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPass})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	store := cfg.Store
	if store == nil {
		store = jobstore.New(rdb) // default: Redis-backed store
	}
	exec := executor.New(store, cfg.InstanceID, cfg.Funcs, lg)
	e := elector.New(rdb, cfg.ElectorKey, cfg.InstanceID, cfg.ElectorTTL, lg)
	sched, err := scheduler.New(store, exec, e, cfg.Reconcile, lg)
	if err != nil {
		return nil, fmt.Errorf("create scheduler failed: %w", err)
	}
	api := admin.New(store, cfg.InstanceID, e, sched, lg)

	eng := &Engine{
		cfg:     cfg,
		rdb:     rdb,
		store:   store,
		exec:    exec,
		elector: e,
		sched:   sched,
		admin:   api,
		log:     lg,
	}

	if len(cfg.Seed) > 0 {
		if err := jobstore.SeedIfEmpty(ctx, store, cfg.Seed); err != nil {
			return nil, fmt.Errorf("seed jobs failed: %w", err)
		}
	}
	return eng, nil
}

// Start begins scheduling and, if configured, serves the admin API.
func (e *Engine) Start() {
	e.sched.Start()
	if e.cfg.AdminAddr != "" {
		go func() {
			e.log.Infof("[%s] admin on %s", e.cfg.InstanceID, e.cfg.AdminAddr)
			if err := http.ListenAndServe(e.cfg.AdminAddr, e.admin.Mux()); err != nil {
				e.log.Errorf("admin http error: %v", err)
			}
		}()
	}
}

// Stop shuts the scheduler down, releases the leadership lock and closes Redis.
func (e *Engine) Stop() {
	e.sched.Stop()
	e.elector.Close()
	_ = e.rdb.Close()
}

// Store returns the underlying job store for direct read/write access.
func (e *Engine) Store() jobstore.Store { return e.store }

// Mux returns the admin HTTP handler so you can mount it on your own server.
func (e *Engine) Mux() http.Handler { return e.admin.Mux() }

// RegisterFunc registers (or replaces) a Go-function job by name.
func (e *Engine) RegisterFunc(name string, fn executor.JobFunc) {
	e.exec.RegisterFunc(name, fn)
}

// IsLeader reports whether this instance currently holds leadership.
func (e *Engine) IsLeader() bool { return e.elector.IsLeaderNow() }
