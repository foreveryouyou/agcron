// Package gcron is a distributed, Redis-backed cron library. It wraps gocron
// with a Redis leader election and a shared job store, so a cluster of
// processes runs each scheduled job exactly once — on the leader instance —
// while followers stay warm and take over if the leader dies.
//
// Minimal usage:
//
//	eng, err := gcron.New(ctx, gcron.Config{
//		RedisAddr:  "localhost:6379",
//		InstanceID: "node-1",
//		Funcs:      gcron.FuncRegistry{"sayHello": sayHello},
//		Seed:       []jobstore.JobDef{ /* ... */ },
//	})
//	if err != nil { log.Fatal(err) }
//	eng.Start()
//	defer eng.Stop()
package gcron

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/foreveryouyou/gcron/admin"
	"github.com/foreveryouyou/gcron/elector"
	"github.com/foreveryouyou/gcron/executor"
	"github.com/foreveryouyou/gcron/jobstore"
	"github.com/foreveryouyou/gcron/scheduler"
)

// Config configures an Engine.
type Config struct {
	RedisAddr  string              // Redis address, e.g. "localhost:6379"
	RedisPass  string              // Redis password (may be empty)
	InstanceID string              // unique per process; defaults to hostname
	ElectorKey string              // Redis key used for leader election
	ElectorTTL time.Duration       // leader lock TTL; <=0 means 10s
	Reconcile  time.Duration       // reconciler poll interval; <=0 means 5s
	Funcs      executor.FuncRegistry // Go-function jobs keyed by name
	Seed       []jobstore.JobDef   // seeded only when the store is empty
	AdminAddr  string              // if non-empty, serves the admin HTTP API here
}

// Engine ties together the store, executor, elector and scheduler.
type Engine struct {
	cfg     Config
	rdb     *redis.Client
	store   *jobstore.Store
	exec    *executor.Executor
	elector *elector.RedisElector
	sched   *scheduler.Scheduler
	admin   *admin.API
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPass})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	store := jobstore.New(rdb)
	exec := executor.New(store, cfg.InstanceID, cfg.Funcs)
	e := elector.New(rdb, cfg.ElectorKey, cfg.InstanceID, cfg.ElectorTTL)
	sched, err := scheduler.New(store, exec, e, cfg.Reconcile)
	if err != nil {
		return nil, fmt.Errorf("create scheduler failed: %w", err)
	}
	api := admin.New(store, cfg.InstanceID, e, sched)

	eng := &Engine{
		cfg:     cfg,
		rdb:     rdb,
		store:   store,
		exec:    exec,
		elector: e,
		sched:   sched,
		admin:   api,
	}

	if len(cfg.Seed) > 0 {
		if err := store.SeedIfEmpty(ctx, cfg.Seed); err != nil {
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
			log.Printf("[%s] admin on %s", e.cfg.InstanceID, e.cfg.AdminAddr)
			if err := http.ListenAndServe(e.cfg.AdminAddr, e.admin.Mux()); err != nil {
				log.Printf("admin http error: %v", err)
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
func (e *Engine) Store() *jobstore.Store { return e.store }

// Mux returns the admin HTTP handler so you can mount it on your own server.
func (e *Engine) Mux() http.Handler { return e.admin.Mux() }

// RegisterFunc registers (or replaces) a Go-function job by name.
func (e *Engine) RegisterFunc(name string, fn executor.JobFunc) {
	e.exec.RegisterFunc(name, fn)
}

// IsLeader reports whether this instance currently holds leadership.
func (e *Engine) IsLeader() bool { return e.elector.IsLeaderNow() }
